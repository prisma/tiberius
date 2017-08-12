//! low level transport that deals with reading bytes from an underlying Io
//! handling data split accross packets, etc.
use std::collections::VecDeque;
use std::fmt;
use std::io::{self, Write};
use std::mem;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;
use std::str;
use tokio_io::{AsyncRead, AsyncWrite};
use bytes::{BufMut, Bytes, BytesMut};
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use futures::{Async, Sink, StartSend, Poll};
use protocol::{self, PacketHeader, PacketStatus};
use tokens::{TdsResponseToken, Tokens, TokenColMetaData, TokenEnvChange};
use types::ColumnData;
use {FromUint, TdsError};

pub trait Io: AsyncRead + AsyncWrite {}
impl<I: AsyncRead + AsyncWrite> Io for I {}

#[cfg(feature = "tls")]
pub mod tls {
    extern crate native_tls;
    extern crate tokio_tls;

    use std::cmp;
    use std::io::{self, Read, Write};
    use std::ops::{Deref, DerefMut};
    use std::mem;
    use futures::Poll;
    use tokio_io::{AsyncRead, AsyncWrite};
    use protocol::{self, PacketHeader, PacketType, PacketStatus};
    use transport::Io;
    pub use self::native_tls::TlsConnector;
    pub use self::tokio_tls::{TlsConnectorExt, ConnectAsync, TlsStream};
    use TdsError;

    impl From<native_tls::Error> for TdsError {
        fn from(e: native_tls::Error) -> TdsError {
            let err =  format!("{:?}", e);
            TdsError::Protocol(err.into())
        }
    }

    /// A variant of `Option` that automatically unwraps
    enum DerefOption<T> {
        None,
        Some(T)
    }

    impl<T> DerefOption<T> {
        fn take_unwrap(&mut self) -> T {
            match mem::replace(self, DerefOption::None) {
                DerefOption::None => panic!("DerefOption: take null"),
                DerefOption::Some(x) => x,
            }
        }
    }

    impl<T> Deref for DerefOption<T> {
        type Target = T;
        fn deref(&self) -> &T {
            match *self {
                DerefOption::None => panic!("DerefOption: Null"),
                DerefOption::Some(ref x) => x,
            }
        }
    }

    impl<T> DerefMut for DerefOption<T> {
        fn deref_mut(&mut self) -> &mut T {
            match *self {
                DerefOption::None => panic!("DerefOption: Null"),
                DerefOption::Some(ref mut x) => x,
            }
        }
    }

    /// wraps written/read data into PRELOGIN packets
    pub struct TlsTdsWrapper<S: Io> {
        stream: DerefOption<S>,
        /// whether to wrap written/read data into prelogin packets (required for the handshake)
        pub wrap: bool,
        wr: Vec<u8>,
        rd: Vec<u8>,
        bytes_left: usize,
    }


    impl<S: Io> TlsTdsWrapper<S> {
        pub fn new(s: S) -> TlsTdsWrapper<S> {
            TlsTdsWrapper {
                stream: DerefOption::Some(s),
                wrap: true,
                wr: vec![],
                rd: Vec::with_capacity(protocol::HEADER_BYTES),
                bytes_left: 0,
            }
        }

        pub fn take_stream(&mut self) -> S {
            self.stream.take_unwrap()
        }
    }

    impl<S: Io> Write for TlsTdsWrapper<S> {
        #[inline]
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            if !self.wrap {
                self.stream.write(buf)
            } else {
                self.wr.extend_from_slice(buf);
                Ok(buf.len())
            }
        }

        #[inline]
        fn flush(&mut self) -> io::Result<()> {
            if self.wrap {
                if self.wr.is_empty() {
                    return Ok(())
                }
                let header = PacketHeader {
                    ty: PacketType::PreLogin,
                    status: PacketStatus::EndOfMessage,
                    ..PacketHeader::new(self.wr.len() + protocol::HEADER_BYTES, 0)
                };
                let mut header_bytes = [0u8; protocol::HEADER_BYTES];
                try!(header.serialize(&mut header_bytes));
                try!(self.stream.write_all(&header_bytes));
                try!(self.stream.write_all(&self.wr));
                self.wr.truncate(0);
            }
            self.stream.flush()
        }
    }

    impl<S: Io> Read for TlsTdsWrapper<S> {
        #[inline]
        fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
            if !self.wrap {
                return self.stream.read(buf)
            }

            // read a new packet header, when required
            if self.bytes_left == 0 {
                try!(self.flush());
                let mut header_bytes = [0u8; protocol::HEADER_BYTES];
                let end_pos = header_bytes.len() - self.rd.len();
                let amount = try!(self.stream.read(&mut header_bytes[..end_pos]));

                self.rd.extend_from_slice(&header_bytes[..amount]);
                if self.rd.len() == protocol::HEADER_BYTES {
                    let header = try!(PacketHeader::unserialize(&self.rd).map_err(|_|
                        io::Error::new(io::ErrorKind::InvalidInput, "malformed packet header")));
                    self.bytes_left = header.length as usize - protocol::HEADER_BYTES;
                    self.rd.truncate(0);
                }
            }

            // read as much data as required
            if self.bytes_left > 0 {
                let end_pos = cmp::min(self.bytes_left, buf.len());
                let amount = try!(self.stream.read(&mut buf[..end_pos]));
                if amount == 0 {
                    return Ok(0);
                }
                self.bytes_left -= amount;
                return Ok(amount);
            }

            Ok(0)
        }
    }

    impl<S: Io> AsyncWrite for TlsTdsWrapper<S> {
        fn shutdown(&mut self) -> Poll<(), io::Error> {
            self.stream.shutdown()
        }
    }

    impl<S: Io> AsyncRead for TlsTdsWrapper<S> {}

    /// A potentially SSL capable stream that wraps any underlying IO
    pub enum TransportStream<S: Io> {
        None,
        TLS(TlsStream<TlsTdsWrapper<S>>),
        Raw(S),
    }

    impl<S: Io> Write for TransportStream<S> {
        #[inline]
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            match *self {
                TransportStream::None => unreachable!(),
                TransportStream::Raw(ref mut raw) => raw.write(buf),
                TransportStream::TLS(ref mut tls) => tls.write(buf),
            }
        }

        #[inline]
        fn flush(&mut self) -> io::Result<()> {
            match *self {
                TransportStream::None => unreachable!(),
                TransportStream::Raw(ref mut raw) => raw.flush(),
                TransportStream::TLS(ref mut tls) => tls.flush(),
            }
        }
    }

    impl<S: Io> Read for TransportStream<S> {
        #[inline]
        fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
            match *self {
                TransportStream::None => unreachable!(),
                TransportStream::Raw(ref mut raw) => raw.read(buf),
                TransportStream::TLS(ref mut tls) => tls.read(buf),
            }
        }
    }

    impl<S: Io> AsyncWrite for TransportStream<S> {
        fn shutdown(&mut self) -> Poll<(), io::Error> {
            match *self {
                TransportStream::None => unreachable!(),
                TransportStream::Raw(ref mut raw) => raw.shutdown(),
                TransportStream::TLS(ref mut tls) => tls.shutdown(),
            }
        }
    }

    impl<S: Io> AsyncRead for TransportStream<S> {}

    // #WARNING: If no hostname is provided, certificate validation is DISABLED
    pub fn connect_async<I, F>(stream: I, host: Option<&str>, callback: F) -> ConnectAsync<I> 
        where I: Io, F: Fn(Vec<u8>) + Sync + Send + 'static
    {
        let disable_verification = host.is_none();
        let mut builder = TlsConnector::builder().unwrap();

        let mut panic = true;
        #[cfg(windows)]
        {
            panic = false;
            extern crate schannel;
            use transport::tls::native_tls::backend::schannel::TlsConnectorBuilderExt;
            if disable_verification {
                builder.verify_callback(|_| Ok(()));
            }
        }
        #[cfg(not(any(target_os = "macos", target_os = "windows")))]
        {
            panic = false;
            extern crate openssl;
            use transport::tls::native_tls::backend::openssl::TlsConnectorBuilderExt;
            if disable_verification {
                builder.builder_mut().builder_mut().set_verify(openssl::ssl::SSL_VERIFY_NONE);
            }
        }

        if panic {
            panic!("disabling cert verification is not supported for this target");
        }

        let cx = builder.build().unwrap();
        match host {
            Some(host) => cx.connect_async(host, stream),
            None => cx.danger_connect_async_without_providing_domain_for_certificate_verification_and_server_name_indication(stream),
        }
    }
}

#[cfg(feature = "tls")]
pub use self::tls::*;

#[cfg(not(feature = "tls"))]
pub type TlsStream<S: Io> = S;

#[cfg(not(feature = "tls"))]
pub type TransportStream<S: Io> = S;

#[derive(Debug)]
pub struct NVarcharPLPTyState {
    pub bytes: Vec<u16>,
    pub chunk_left: Option<usize>,
    pub leftover: Option<u8>,
}

#[derive(Debug)]
pub enum ReadTyState {
    NVarcharPLP(NVarcharPLPTyState),
    NVarchar(Vec<u16>),
}

#[derive(Debug)]
pub enum ReadState {
    Generic(Tokens, Option<usize>),
    Row(Tokens, Vec<ColumnData<'static>>, Option<ReadTyState>),

    Type(ReadTyState),
}

pub struct TdsTransport<I: Io> {
    pub inner: TdsTransportInner<I>,
    pub read_state: Option<ReadState>,
    /// the last buffer which will be resetted to if not enough data is available
    /// needed for parsers where simply trying until enough data is available is not expensive enough
    /// that writing a stateful parser would be worth it
    pub last_state: Option<TdsBuf>,
    pub transaction: u64,
    reinject_token: Option<TdsResponseToken>,
}

#[derive(Copy, Clone, Debug)]
pub struct TdsPacketId(u8);

impl TdsPacketId {
    /// get the next unused packet id
    #[inline]
    pub fn next(&mut self) -> u8 {
        let id = self.0;
        self.0 = (id + 1) % 0xff;
        id
    }
}

pub struct TdsTransportInner<I: Io> {
    pub io: I,
    missing: usize,
    hrd: [u8; protocol::HEADER_BYTES],
    pub rd: TdsBuf,
    header: Option<PacketHeader>,
    packets_left: bool,

    wr: VecDeque<(usize, Vec<u8>)>,
    pub next_packet_id: TdsPacketId,
    pub packet_size: usize,
    pub last_meta: Option<Arc<TokenColMetaData>>,
    pub row_bitmap: Option<TdsBuf>,
}

impl<I: Io> Deref for TdsTransportInner<I> {
    type Target = TdsBuf;

    fn deref(&self) -> &Self::Target {
        &self.rd
    }
}

impl<I: Io> DerefMut for TdsTransportInner<I> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.rd
    }
}

pub trait ReadSize<R: io::Read> {
    fn read_size(&mut R) -> io::Result<usize>;
}

pub trait WriteSize<W: io::Write> {
    fn write_size(&mut W, size: usize) -> io::Result<()>;
}

/// B_VARCHAR
impl<R: io::Read> ReadSize<R> for u8 {
     fn read_size(reader: &mut R) -> io::Result<usize> {
         Ok(try!(reader.read_u8()) as usize)
     }
}

/// US_VARCHAR
impl<R: io::Read> ReadSize<R> for u16 {
    fn read_size(reader: &mut R) -> io::Result<usize> {
         Ok(try!(reader.read_u16::<LittleEndian>()) as usize)
     }
}

pub struct NoLength;
impl<W: io::Write> WriteSize<W> for NoLength {
    fn write_size(_: &mut W, _: usize) -> io::Result<()> {
        Ok(())
    }
}

impl<W: io::Write> WriteSize<W> for u8 {
    fn write_size(writer: &mut W, size: usize) -> io::Result<()> {
        Ok(try!(writer.write_u8(size as u8)))
    }
}

impl<W: io::Write> WriteSize<W> for u16 {
    fn write_size(writer: &mut W, size: usize) -> io::Result<()> {
        Ok(try!(writer.write_u16::<LittleEndian>(size as u16)))
    }
}

#[derive(Clone)]
pub struct TdsBuf(Bytes);

impl TdsBuf {
    pub fn empty() -> TdsBuf {
        TdsBuf(Bytes::new())
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn as_str(&self) -> &str {
        // validation should've already happened in `read_varchar`
        // maybe use `from_utf8_unchecked` ?
        str::from_utf8(self.as_ref()).unwrap()
    }

    /// attempts to read n bytes and returns them as a subslice-buffer
    pub fn read_bytes(&mut self, n: usize) -> Option<TdsBuf> {
        if self.len() >= n {
            return Some(TdsBuf(self.0.split_to(n)))
        }
        None
    }

    /// attempts to read the amount of bytes required to totally fill the given slice (n=target.len())
    pub fn read_bytes_to(&mut self, target: &mut [u8]) -> Poll<(), io::Error> {
        let n = target.len();
        if self.len() >= n {
            target.clone_from_slice(&self.as_ref()[..n]);
            self.0 = self.0.slice_from(n);
            return Ok(Async::Ready(()));
        }
        Ok(Async::NotReady)
    }

    /// read bytes with length prefix
    pub fn read_varbyte<S: ReadSize<Self>>(&mut self) -> Poll<TdsBuf, io::Error> {
        let len = try!(S::read_size(self));
        let ret = match self.read_bytes(len) {
            Some(bytes) => Async::Ready(bytes),
            None => Async::NotReady,
        };
        Ok(ret)
    }

    /// read bytes with an length prefix (which either is in bytes or in bytes/2 [u16 characters]) and interpret them as UCS-2 encoded string
    pub fn read_varchar<S: ReadSize<Self>>(&mut self, size_in_bytes: bool) -> Poll<TdsBuf, TdsError> {
        let mut len = try!(S::read_size(self));
        if size_in_bytes {
            assert_eq!(len % 2, 0);
            len /= 2;
        }
        // this is suboptimal but we need to copy them to be able to interpret these strings properly
        let data: Vec<u16> = try!(vec![0u16; len].into_iter().map(|_| self.read_u16::<LittleEndian>()).collect());
        let bytes = try!(String::from_utf16(&data[..])).into_bytes();;
        Ok(Async::Ready(TdsBuf(bytes.into())))
    }
}

impl io::Read for TdsBuf {
    /// this is basically an exact read (always returns the size of the input buffer)
    /// or an error that there aren't enough bytes
    fn read(&mut self, mut buf: &mut [u8]) -> io::Result<usize> {
        if buf.len() > self.len() {
            return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "not enough bytes"));
        }
        let len = buf.len();
        let written = try!(buf.write(&self.as_ref()[..len]));
        assert_eq!(written, len);
        self.0 = self.0.slice_from(written);
        Ok(written)
    }

    #[inline]
    #[allow(unused_io_amount)]
    fn read_exact(&mut self, buf: &mut [u8]) -> io::Result<()> {
        try!(self.read(buf));
        Ok(())
    }
}

impl AsRef<[u8]> for TdsBuf {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl fmt::Debug for TdsBuf {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        match str::from_utf8(self.as_ref()) {
            Ok(str_) => write!(f, "{:?}", str_),
            Err(_) => write!(f, "TdsBuf({:?})", self.as_ref()),
        }
    }
}

impl<I: Io> TdsTransport<I> {
    pub fn new(io: I) -> TdsTransport<I> {
        let packet_size = 4096;
        TdsTransport {
            inner: TdsTransportInner {
                io: io,
                missing: protocol::HEADER_BYTES,
                hrd: [0; protocol::HEADER_BYTES],
                rd: TdsBuf::empty(),
                header: None,
                packets_left: true,
                wr: VecDeque::new(),
                //
                next_packet_id: TdsPacketId(0),
                packet_size: packet_size,
                last_meta: None,
                row_bitmap: None,
            },
            read_state: None,
            last_state: None,
            transaction: 0,
            reinject_token: None,
        }
    }

    /// get the next unused packet id
    #[inline]
    pub fn next_id(&mut self) -> u8 {
        self.inner.next_id()
    }

    /// reinject a token, so it's returned again on the next call to read_token
    pub fn reinject(&mut self, tok: TdsResponseToken) {
        assert!(self.reinject_token.is_none());
        self.reinject_token = Some(tok);
    }

    #[inline]
    pub fn commit_read_state<S: Into<Option<ReadState>>>(&mut self, state: S) {
        self.last_state = Some(self.inner.rd.clone());
        self.read_state = state.into();
    }

    /// returns a parsed token
    fn read_token(&mut self) -> Poll<TdsResponseToken, TdsError> {
        let (token, size_hint) = {
            // read a token
            if self.read_state.is_none() {
                let raw_token = try!(self.inner.read_u8());
                let token = Tokens::from_u8(raw_token);

                self.commit_read_state(match token {
                    Some(token) => ReadState::Generic(token, None),
                    None => panic!("invalid token received 0x{:x}", raw_token),
                });
            }

            // read the associated length for a token, if available
            if let Some(ReadState::Generic(token, None)) = self.read_state {
                let new_state = match token {
                    Tokens::SSPI | Tokens::EnvChange | Tokens::Info | Tokens::Error | Tokens::LoginAck => {
                        ReadState::Generic(token, Some(try!(self.inner.read_u16::<LittleEndian>()) as usize))
                    },
                    Tokens::Row | Tokens::NbcRow => {
                        let len = self.inner.last_meta.as_ref().map(|lm| lm.columns.len()).unwrap_or(0);
                        ReadState::Row(token, Vec::with_capacity(len), None)
                    },
                    _ => {
                        ReadState::Generic(token, Some(0))
                    }
                };
                self.commit_read_state(new_state);
            }

            match self.read_state {
                Some(ReadState::Generic(token, Some(size_hint))) => (token, size_hint),
                Some(ReadState::Row(tok, _, _)) => (tok, 0),
                _ => unreachable!()
            }
        };
        let ret = self.parse_token(token, size_hint);
        if let Ok(Async::Ready(_)) = ret {
            self.commit_read_state(None);
        }
        ret
    }

    pub fn next_token(&mut self) -> Poll<Option<TdsResponseToken>, TdsError> {
        // return a reinjected token instantly
        if let Some(next_token) = self.reinject_token.take() {
            return Ok(Async::Ready(Some(next_token)));
        }

        loop {
            self.last_state = Some(self.inner.rd.clone());

            let ret = match self.read_token() {
                Err(TdsError::Io(ref err)) if err.kind() == ::std::io::ErrorKind::UnexpectedEof => {
                    Async::NotReady
                },
                x => try!(x)
            };

            match ret {
                Async::NotReady if !self.inner.packets_left && self.inner.len() == 0 => {
                    return Ok(Async::Ready(None))
                },
                Async::NotReady => {
                    if let Some(last_state) = self.last_state.take() {
                        self.inner.rd = last_state;
                    }
                },
                Async::Ready(ret) => {
                    // we only limit the current token to the current stream of packets
                    self.inner.packets_left = true;
                    // handle tokens which are only relevant for the connection (notifications)
                    match ret {
                        TdsResponseToken::EnvChange(env_change) => {
                            match env_change {
                                TokenEnvChange::PacketSize(new_size, _) => {
                                    self.inner.packet_size = new_size as usize;
                                },
                                TokenEnvChange::BeginTransaction(trans_id) => {
                                    self.transaction = trans_id;
                                },
                                TokenEnvChange::RollbackTransaction(old_trans_id) | TokenEnvChange::CommitTransaction(old_trans_id) => {
                                    assert_eq!(self.transaction, old_trans_id);
                                    self.transaction = 0;
                                },
                                _ => (),
                            }
                            continue;
                        },
                        TdsResponseToken::Info(_) | TdsResponseToken::Order(_) => continue,
                        TdsResponseToken::Error(err) => {
                            return Err(TdsError::Server(err));
                        },
                        _ => ()
                    }
                    self.last_state = None;
                    return Ok(Async::Ready(Some(ret)))
                },
            }
            // if we aren't done with the packets, load more
            if self.inner.packets_left {
                let header = try_ready!(self.inner.next_packet());
                // a token cannot span across multiple packets
                if header.status == PacketStatus::EndOfMessage {
                    self.inner.packets_left = false;
                }
            }
        }
    }
}

impl <I: Io> TdsTransportInner<I> {
    /// get the next unused packet id
    #[inline]
    pub fn next_id(&mut self) -> u8 {
        self.next_packet_id.next()
    }

    pub fn queue_vec(&mut self, buf: Vec<u8>) -> io::Result<()> {
        self.wr.push_back((0, buf));
        Ok(())
    }

    /// simply returns a chunk of data with the specified length, adjusting read and write positions
    pub fn get_packet(&mut self, full_length: usize) -> TdsBuf {
        let len = full_length-protocol::HEADER_BYTES;
        self.read_bytes(len).unwrap()
    }

    /// buffers another packet from the underlying IO (or continues the last I/O operation)
    pub fn next_packet(&mut self) -> Poll<PacketHeader, TdsError> {
        // read the header first
        if self.header.is_none() {
            let mut offset = protocol::HEADER_BYTES - self.missing;

            while self.missing > 0 {
                let amount = try_nb!(self.io.read(&mut self.hrd[offset..]));
                self.missing -= amount;
                offset += amount;
            }

            let header = try!(PacketHeader::unserialize(&self.hrd));
            self.missing = header.length as usize - protocol::HEADER_BYTES;
            self.header = Some(header);
        }

        // read the packet body
        if self.header.is_some() {
            // make sure the packet body fits into the buffer
            while self.missing > 0 {
                let buf = mem::replace(&mut self.rd.0, Bytes::new());
                let mut write_buf = match buf.try_mut() {
                    Ok(mut buf) => {
                        if buf.remaining_mut() < self.missing {
                            buf.reserve(self.missing);
                        }
                        buf
                    },
                    Err(old_buf) => {
                        let mut buf = BytesMut::with_capacity(old_buf.len() + self.missing);
                        buf.put_slice(old_buf.as_ref());
                        buf
                    }
                };
                unsafe {
                    let count_result = self.io.read(&mut write_buf.bytes_mut()[..self.missing]);
                    if let Ok(count) = count_result {
                        write_buf.advance_mut(count);
                        self.missing -= count;
                    }
                    self.rd.0 = write_buf.freeze();
                    try_nb!(count_result);
                }
            }

            // if we're done get ready to read the next packet and restore state
            self.missing = protocol::HEADER_BYTES;
            return Ok(Async::Ready(mem::replace(&mut self.header, None).unwrap()));
        }

        Ok(Async::NotReady)
    }
}

impl<I: Io> Sink for TdsTransportInner<I> {
    type SinkItem = ();
    type SinkError = io::Error;

    /// this is never used
    fn start_send(&mut self, _: Self::SinkItem) -> StartSend<Self::SinkItem, Self::SinkError> {
        unimplemented!()
    }

    fn poll_complete(&mut self) -> Poll<(), Self::SinkError> {
        while !self.wr.is_empty() {
            let mut front_consumed = false;
            if let Some(ref mut front) = self.wr.front_mut() {
                let bytes = try_nb!(self.io.write(&front.1[front.0..]));
                front.0 += bytes;
                if front.0 >= front.1.len() {
                    front_consumed = true;
                }
            }
            if front_consumed {
                self.wr.pop_front();
            }
        }

        try_nb!(self.io.flush());
        if !self.wr.is_empty() {
            return Ok(Async::NotReady);
        }
        Ok(Async::Ready(()))
    }
}

pub trait PrimitiveWrites: Write {
    fn write_varchar<S: WriteSize<Self>>(&mut self, str_: &str) -> io::Result<()> where Self: Sized;
}
impl<W: Write> PrimitiveWrites for W {
    fn write_varchar<S: WriteSize<Self>>(&mut self, str_: &str) -> io::Result<()> {
        try!(S::write_size(self, str_.len()));
        for chr in str_.encode_utf16() {
            try!(self.write_u16::<LittleEndian>(chr));
        }
        Ok(())
    }
}

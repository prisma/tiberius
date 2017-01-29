//! low level transport that deals with reading bytes from an underlying Io
//! handling data split accross packets, etc.
use std::collections::VecDeque;
use std::cell::RefCell;
use std::fmt;
use std::io::{self, Write};
use std::mem;
use std::ops::{Deref, DerefMut};
use std::rc::Rc;
use std::sync::Arc;
use std::str;
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use futures::{Async, Sink, StartSend, Poll};
use tokio_core::io::Io;
use protocol::{self, PacketHeader, PacketStatus};
use tokens::{TdsResponseToken, Tokens, TokenColMetaData};
use types::ColumnData;
use {FromUint, TdsError};

pub struct NVarcharPLPTyState {
    pub size: usize,
    pub bytes: Vec<u16>,
    pub chunk_left: Option<usize>,
    pub leftover: Option<u8>,
}

pub enum ReadTyState {
    None,
    NVarcharPLP(NVarcharPLPTyState),
    NVarchar(Vec<u16>),
}

pub enum ReadState {
    None,
    Generic(Tokens, Option<usize>),
    Row(Vec<ColumnData<'static>>, ReadTyState),

    Type(ReadTyState),
}

pub struct TdsTransport<I: Io> {
    io: I,
    header: Option<PacketHeader>,
    pub read_state: Rc<RefCell<ReadState>>,
    /// whether to reset the position, if parsing this token fails (see comment below)
    pub read_reset: bool,
    /// the last position which will be resetted to if not enough data is available
    /// needed for parsers where simply trying until enough data is available is not expensive enough
    /// that writing a stateful parser would be worth it
    pub last_pos: usize,
    packets_left: bool,
    missing: usize,
    hrd: [u8; protocol::HEADER_BYTES],
    pub rd: TdsBuf,
    wr: VecDeque<(usize, Vec<u8>)>,
    next_packet_id: u8,
    pub packet_size: usize,
    pub last_meta: Option<Arc<TokenColMetaData>>,
}

impl<I: Io> Deref for TdsTransport<I> {
    type Target = TdsBuf;

    fn deref(&self) -> &Self::Target {
        &self.rd
    }
}

impl<I: Io> DerefMut for TdsTransport<I> {
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

/// TdsBuf/TdsBufMut inspired by tokio's EasyBuf
pub struct TdsBuf {
    start: usize,
    end: usize,
    //TODO: Arc or Rc?
    buf: Arc<Vec<u8>>,
}

impl TdsBuf {
    pub fn with_capacity(cap: usize) -> TdsBuf {
        TdsBuf {
            start: 0,
            end: 0,
            buf: Arc::new(Vec::with_capacity(cap)),
        }
    }

    pub fn set_position(&mut self, pos: usize) {
        self.start = pos;
        assert!(self.end >= self.start);
    }

    pub fn position(&self) -> usize {
        self.start
    }

    pub fn len(&self) -> usize {
        self.end - self.start
    }

    pub fn as_str(&self) -> &str {
        // validation should've already happened in `read_varchar`
        // maybe use `from_utf8_unchecked` ?
        str::from_utf8(self.as_ref()).unwrap()
    }

    /// attempts to read n bytes and returns them as a subslice-buffer
    pub fn read_bytes(&mut self, n: usize) -> Option<TdsBuf> {
        if self.len() >= n {
            // determine whether it's the better option to copy or zero-copy
            // based on the buffer memory overhead
            let buf = if self.len() * 3/4 < n {
                // zero-copy
                TdsBuf {
                    start: self.start,
                    end: self.start + n,
                    buf: self.buf.clone(),
                }
            } else {
                // copy
                self.as_ref()[..n].to_owned().into()
            };
            self.start += n;
            return Some(buf)
        }
        None
    }

    /// attempts to read the amount of bytes required to totally fill the given slice (n=target.len())
    pub fn read_bytes_to(&mut self, target: &mut [u8]) -> Poll<(), io::Error> {
        let n = target.len();
        if self.len() >= n {
            target.clone_from_slice(&self.as_ref()[..n]);
            self.start += n;
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
        Ok(Async::Ready(bytes.into()))
    }

    /// get a mutable reference and
    // optionally ensure the underlying buffer has atleast a given length
    pub fn get_mut(&mut self, required_length: Option<usize>) -> &mut [u8] {
        // the underlying buffer is only used by us, we can get exclusive access
        if Arc::get_mut(&mut self.buf).is_some() {
            let buf = Arc::get_mut(&mut self.buf).unwrap();

            if self.start > 0 {
                buf.drain(..self.start);
                self.end -= self.start;
                self.start = 0;
            }

            if let Some(min_len) = required_length {
                if buf.len() < min_len + self.end {
                    buf.resize(min_len + self.end, 0);
                }
            }
            return &mut buf[self.end..]
        }

        // can't get access, need a new buffer
        let mut new_capacity = self.buf.capacity();
        let min_capacity = self.len() + required_length.unwrap_or(0);
        if min_capacity > new_capacity {
           new_capacity = min_capacity;
        }
        // allocate a new buffer with the required length
        let mut v = Vec::with_capacity(new_capacity);
        v.extend_from_slice(self.as_ref());
        self.end -= v.len();
        if let Some(min_len) = required_length {
            if v.len() < min_len + self.end {
                v.resize(min_len + self.end, 0);
            }
        }
        self.start = 0;
        self.buf = Arc::new(v);
        let new_buf = Arc::get_mut(&mut self.buf).unwrap();
        &mut new_buf[self.end..]
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
        self.start += written;
        Ok(written)
    }

    #[inline]
    #[allow(unused_io_amount)]
    fn read_exact(&mut self, mut buf: &mut [u8]) -> io::Result<()> {
        try!(self.read(buf));
        Ok(())
    }
}

impl AsRef<[u8]> for TdsBuf {
    fn as_ref(&self) -> &[u8] {
        &self.buf[self.start..self.end]
    }
}

impl From<Vec<u8>> for TdsBuf {
    fn from(v: Vec<u8>) -> TdsBuf {
        TdsBuf {
            start: 0,
            end: v.len(),
            buf: Arc::new(v),
        }
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
            io: io,
            header: None,
            read_reset: true,
            last_pos: 0,
            read_state: Rc::new(RefCell::new(ReadState::None)),
            packets_left: true,
            missing: protocol::HEADER_BYTES,
            hrd: [0; protocol::HEADER_BYTES],
            rd: TdsBuf::with_capacity(packet_size),
            wr: VecDeque::new(),
            //
            next_packet_id: 0,
            packet_size: packet_size,
            last_meta: None,
        }
    }

    /// get the next unused packet id
    #[inline]
    pub fn next_id(&mut self) -> u8 {
        let id = self.next_packet_id;
        self.next_packet_id = (id + 1) % 0xff;
        id
    }

    pub fn queue_vec(&mut self, buf: Vec<u8>) -> io::Result<()> {
        self.wr.push_back((0, buf));
        Ok(())
    }

    pub fn take_read_state(&mut self) -> ReadState {
        mem::replace(&mut *self.read_state.borrow_mut(), ReadState::None)
    }

    /// returns a parsed token
    fn read_token(&mut self) -> Poll<TdsResponseToken, TdsError> {
        let (token, size_hint) = {
            let read_state = self.read_state.clone();
            let mut read_state_mut = read_state.borrow_mut();

            // read a token
            if let ReadState::None = *read_state_mut {
                let raw_token = try!(self.read_u8());
                let token = Tokens::from_u8(raw_token);

                *read_state_mut = match token {
                    Some(token) => ReadState::Generic(token, None),
                    None => panic!("invalid token received 0x{:x}", raw_token),
                };
            }

            // read the associated length for a token, if available
            if let ReadState::Generic(token, None) = *read_state_mut {
                *read_state_mut = match token {
                    Tokens::SSPI | Tokens::EnvChange | Tokens::Info | Tokens::LoginAck => {
                        ReadState::Generic(token, Some(try!(self.read_u16::<LittleEndian>()) as usize))
                    },
                    Tokens::Row => {
                        let len = self.last_meta.as_ref().map(|lm| lm.columns.len()).unwrap_or(0);
                        ReadState::Row(Vec::with_capacity(len), ReadTyState::None)
                    },
                    _ => {
                        ReadState::Generic(token, Some(0))
                    }
                };
            }

            match *read_state_mut {
                ReadState::Generic(token, Some(size_hint)) => (token, size_hint),
                ReadState::Row(_, _) => (Tokens::Row, 0),
                _ => unreachable!()
            }
        };
        let ret = self.parse_token(token, size_hint);
        if let Ok(Async::Ready(_)) = ret {
            *self.read_state.borrow_mut() = ReadState::None;
        }
        ret
    }

    pub fn next_token(&mut self) -> Poll<Option<(usize, TdsResponseToken)>, TdsError> {
        loop {
            self.last_pos = self.position();

            let ret = match self.read_token() {
                Err(TdsError::Io(ref err)) if err.kind() == ::std::io::ErrorKind::UnexpectedEof => {
                    Async::NotReady
                },
                x => try!(x)
            };
            match ret {
                Async::NotReady if !self.packets_left && self.len() == 0 => {
                    return Ok(Async::Ready(None))
                },
                Async::NotReady => {
                    if self.read_reset {
                        self.rd.start = self.last_pos;
                    }
                    self.read_reset = true;
                },
                Async::Ready(ret) => {
                    // we only limit the current token to the current stream of packets
                    self.packets_left = true;
                    return Ok(Async::Ready(Some((self.last_pos, ret))))
                },
            }
            // if we aren't done with the packets, load more
            if self.packets_left {
                let header = try_ready!(self.next_packet());
                // a token cannot span across multiple packets
                if header.status == PacketStatus::EndOfMessage {
                    self.packets_left = false;
                }
            }
        }
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
            let offset = protocol::HEADER_BYTES - self.missing;

            while self.missing > 0 {
                self.missing -= try_nb!(self.io.read(&mut self.hrd[offset..]))
            }

            let header = try!(PacketHeader::unserialize(&self.hrd));
            self.missing = header.length as usize - protocol::HEADER_BYTES;
            self.header = Some(header);
        }

        // read the packet body
        if self.header.is_some() {
            // make sure the packet body fits into the buffer
            while self.missing > 0 {
                let count = {
                    let write_buf = self.rd.get_mut(Some(self.missing));
                    try_nb!(self.io.read(&mut write_buf[..self.missing]))
                };
                self.rd.end += count;
                self.missing -= count;
            }

            // if we're done get ready to read the next packet and restore state
            self.missing = protocol::HEADER_BYTES;
            return Ok(Async::Ready(mem::replace(&mut self.header, None).unwrap()));
        }

        Ok(Async::NotReady)
    }
}

impl<I: Io> Sink for TdsTransport<I> {
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

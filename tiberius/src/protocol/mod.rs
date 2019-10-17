use bitflags::bitflags;
use byteorder::{BigEndian, LittleEndian, ReadBytesExt, WriteBytesExt};
use std::borrow::Cow;
use std::convert::TryFrom;
use std::io::{self, Cursor, Write};
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::{Arc, Mutex};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tracing::{event, Level};

use crate::{Error, Result};

macro_rules! uint_enum {
    ($( #[$gattr:meta] )* pub enum $ty:ident { $( $( #[$attr:meta] )* $variant:ident = $val:expr,)* }) => {
        uint_enum!($( #[$gattr ])* (pub) enum $ty { $( $( #[$attr] )* $variant = $val, )* });
    };
    ($( #[$gattr:meta] )* enum $ty:ident { $( $( #[$attr:meta] )* $variant:ident = $val:expr,)* }) => {
        uint_enum!($( #[$gattr ])* () enum $ty { $( $( #[$attr] )* $variant = $val, )* });
    };

    ($( #[$gattr:meta] )* ( $($vis:tt)* ) enum $ty:ident { $( $( #[$attr:meta] )* $variant:ident = $val:expr,)* }) => {
        #[derive(Debug, Copy, Clone, PartialEq)]
        $( #[$gattr] )*
        $( $vis )* enum $ty {
            $( $( #[$attr ])* $variant = $val, )*
        }

        impl ::std::convert::TryFrom<u8> for $ty {
            type Error = ();
            fn try_from(n: u8) -> ::std::result::Result<$ty, ()> {
                match n {
                    $( x if x == $ty::$variant as u8 => Ok($ty::$variant), )*
                    _ => Err(()),
                }
            }
        }

        impl ::std::convert::TryFrom<u32> for $ty {
            type Error = ();
            fn try_from(n: u32) -> ::std::result::Result<$ty, ()> {
                match n {
                    $( x if x == $ty::$variant as u32 => Ok($ty::$variant), )*
                    _ => Err(()),
                }
            }
        }
    }
}

mod tokenstream;
pub use tokenstream::*;
mod login;
pub use login::{LoginMessage, PreloginMessage};
mod types;
pub use types::ColumnData;

uint_enum! {
    #[repr(u32)]
    pub enum FeatureLevel {
        SqlServerV7 = 0x70000000,
        SqlServer2000 = 0x71000000,
        SqlServer2000Sp1 = 0x71000001,
        SqlServer2005 = 0x72090002,
        SqlServer2008 = 0x730A0003,
        SqlServer2008R2 = 0x730B0003,
        /// 2012, 2014, 2016
        SqlServerN = 0x74000004,
    }
}

uint_enum! {
    /// The configured encryption level specifying if encryption is required
    #[repr(u8)]
    pub enum EncryptionLevel {
        /// Only use encryption for the login procedure
        Off = 0,
        /// Encrypt everything if possible
        On = 1,
        /// Do not encrypt anything
        NotSupported = 2,
        /// Encrypt everything and fail if not possible
        Required = 3,
    }
}

/// Context, that might be required to make sure we understand and are understood by the server
pub struct Context {
    pub version: FeatureLevel,
    pub packet_size: usize,
    pub packet_id: AtomicU8,
    pub last_meta: Mutex<Option<Arc<TokenColMetaData>>>,
}

impl Context {
    pub fn new() -> Context {
        Context {
            version: FeatureLevel::SqlServerN,
            packet_size: 4096,
            packet_id: AtomicU8::new(0),
            last_meta: Mutex::new(None),
        }
    }

    pub fn new_header(&self, length: usize) -> PacketHeader {
        PacketHeader::new(length, self.packet_id.fetch_add(1, Ordering::SeqCst))
    }
}

/// The amount of bytes a packet header consists of
pub const HEADER_BYTES: usize = 8;
pub const ALL_HEADERS_LEN_TX: usize = 22;

uint_enum! {
    /// the type of the packet [2.2.3.1.1]#[repr(u32)]
    #[repr(u8)]
    pub enum PacketType {
        SQLBatch = 1,
        /// unused
        PreTDSv7Login = 2,
        RPC = 3,
        TabularResult = 4,
        AttentionSignal = 6,
        BulkLoad = 7,
        /// Federated Authentication Token
        Fat = 8,
        TransactionManagerReq = 14,
        TDSv7Login = 16,
        SSPI = 17,
        PreLogin = 18,
    }
}

uint_enum! {
    /// the message state [2.2.3.1.2]
    #[repr(u8)]
    pub enum PacketStatus {
        NormalMessage = 0,
        EndOfMessage = 1,
        /// [client to server ONLY] (EndOfMessage also required)
        IgnoreEvent = 3,
        /// [client to server ONLY] [>= TDSv7.1]
        ResetConnection = 0x08,
        /// [client to server ONLY] [>= TDSv7.3]
        ResetConnectionSkipTran = 0x10,
    }
}

/// packet header consisting of 8 bytes [2.2.3.1]
#[derive(Debug)]
pub struct PacketHeader {
    pub ty: PacketType,
    pub status: PacketStatus,
    /// [BE] the length of the packet (including the 8 header bytes)
    /// must match the negotiated size sending from client to server [since TDSv7.3] after login
    /// (only if not EndOfMessage)
    pub length: u16,
    /// [BE] the process ID on the server, for debugging purposes only
    pub spid: u16,
    /// packet id
    pub id: u8,
    /// currently unused
    pub window: u8,
}

impl PacketHeader {
    pub fn new(length: usize, id: u8) -> PacketHeader {
        assert!(length <= u16::max_value() as usize);
        PacketHeader {
            ty: PacketType::TDSv7Login,
            status: PacketStatus::ResetConnection,
            length: length as u16,
            spid: 0,
            id: id,
            window: 0,
        }
    }

    pub fn serialize(&self, target: &mut [u8]) -> io::Result<()> {
        let mut writer = Cursor::new(target);
        writer.write_u8(self.ty as u8)?;
        writer.write_u8(self.status as u8)?;
        writer.write_u16::<BigEndian>(self.length)?;
        writer.write_u16::<BigEndian>(self.spid)?;
        writer.write_u8(self.id)?;
        writer.write_u8(self.window)
    }

    pub fn unserialize(buf: &[u8]) -> Result<PacketHeader> {
        let mut cursor = Cursor::new(buf);
        Ok(PacketHeader {
            ty: PacketType::try_from(cursor.read_u8()?)
                .map_err(|_| Error::Protocol("header: invalid packet type".into()))?,
            status: PacketStatus::try_from(cursor.read_u8()?)
                .map_err(|_| Error::Protocol("header: invalid packet status".into()))?,
            length: cursor.read_u16::<BigEndian>()?,
            spid: cursor.read_u16::<BigEndian>()?,
            id: cursor.read_u8()?,
            window: cursor.read_u8()?,
        })
    }
}

pub struct PacketReader<'a, C: AsyncRead> {
    conn: &'a mut C,
    /// packet contents (without headers)
    buf: Vec<u8>,
    pos: usize,
    done: bool,
}

impl<'a, C: AsyncRead + Unpin> PacketReader<'a, C> {
    pub fn new(conn: &'a mut C) -> Self {
        PacketReader {
            conn,
            buf: vec![],
            pos: 0,
            done: false,
        }
    }

    pub(crate) fn remaining_buf(&self) -> &[u8] {
        &self.buf[self.pos..]
    }

    pub async fn read_header(&mut self) -> Result<PacketHeader> {
        // tokens can only span across packets within the same stream (no EndOfMessage in between)
        // so we are done with all tokens that came before if this is the case
        if self.done {
            self.done = false;
            self.buf.clear();
            self.pos = 0;
        }
        let mut header_buf = vec![0u8; HEADER_BYTES];
        event!(
            Level::TRACE,
            read_bytes = self.conn.read_exact(&mut header_buf).await?
        );
        let header = PacketHeader::unserialize(&header_buf)?;
        Ok(header)
    }

    pub async fn read_packet(&mut self) -> Result<PacketHeader> {
        let header = self.read_header().await?;
        self.read_packet_with_header(&header).await?;
        Ok(header)
    }

    pub async fn read_packet_with_header(&mut self, header: &PacketHeader) -> Result<()> {
        let pos = self.buf.len();
        self.buf
            .resize(pos + header.length as usize - HEADER_BYTES, 0);
        event!(
            Level::TRACE,
            read_bytes = self.conn.read_exact(&mut self.buf[pos..]).await?
        );
        if header.status == PacketStatus::EndOfMessage {
            self.done = true;
        }
        Ok(())
    }

    pub async fn read_bytes(&mut self, n: usize) -> Result<&[u8]> {
        // TODO: optimize allocations?
        while self.buf[self.pos..].len() < n {
            self.read_packet().await?;
        }
        let ret = &self.buf[self.pos..self.pos + n];
        self.pos += n;
        Ok(ret)
    }
}

pub struct PacketWriter<'a, C: AsyncWrite> {
    conn: &'a mut C,
    header_template: PacketHeader,
    buf: Vec<u8>,
}

impl<'a, C: AsyncWrite + Unpin> PacketWriter<'a, C> {
    pub fn new(conn: &'a mut C, header_template: PacketHeader) -> Self {
        PacketWriter {
            conn,
            header_template,
            buf: vec![0u8; HEADER_BYTES],
        }
    }

    pub async fn write_bytes(&mut self, ctx: &Context, mut buf: &[u8]) -> Result<()> {
        while !buf.is_empty() {
            let free_buf_space = ctx.packet_size - self.buf.len();
            let writable = std::cmp::min(buf.len(), free_buf_space);
            self.buf.extend_from_slice(&buf[..writable]);
            buf = &buf[writable..];
            // If we overlap into a next packet, flush it out
            if !buf.is_empty() {
                self.flush_packet(ctx).await?;
            }
        }
        Ok(())
    }

    pub async fn flush_packet(&mut self, ctx: &Context) -> Result<()> {
        self.header_template.length = self.buf.len() as u16;
        self.header_template
            .serialize(&mut self.buf[..HEADER_BYTES])?;
        event!(Level::TRACE, write_bytes = self.buf.len());
        self.conn.write_all(&self.buf).await?;
        self.buf.truncate(HEADER_BYTES);
        Ok(())
    }

    pub async fn finish(mut self, ctx: &Context) -> Result<()> {
        self.header_template.status = PacketStatus::EndOfMessage;
        self.flush_packet(ctx).await?;
        event!(Level::TRACE, "flush");
        self.conn.flush().await?;
        Ok(())
    }
}

#[derive(Debug)]
#[repr(u16)]
enum AllHeaderTy {
    QueryDescriptor = 1,
    TransactionDescriptor = 2,
    TraceActivity = 3,
}

pub async fn write_trans_descriptor<C: AsyncWrite + Unpin>(
    w: &mut PacketWriter<'_, C>,
    ctx: &Context,
    id: u64,
) -> Result<()> {
    let mut buf = [0u8; 22];
    let mut cursor = Cursor::new(&mut buf[..]);
    cursor.write_u32::<LittleEndian>(ALL_HEADERS_LEN_TX as u32)?;
    cursor.write_u32::<LittleEndian>(ALL_HEADERS_LEN_TX as u32 - 4)?;
    cursor.write_u16::<LittleEndian>(AllHeaderTy::TransactionDescriptor as u16)?;
    // transaction descriptor
    cursor.write_u64::<LittleEndian>(id)?;
    // outstanding requests (TransactionDescrHeader)
    cursor.write_u32::<LittleEndian>(1)?;

    w.write_bytes(ctx, &buf).await?;
    Ok(())
}

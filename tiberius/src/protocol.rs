//! protocol abstraction, mostly type definitions (meta)
use std::borrow::Cow;
use std::cmp;
use std::io::{self, Cursor, Write};
use std::mem;
use futures::Sink;
use byteorder::{BigEndian, LittleEndian, ReadBytesExt, WriteBytesExt};
use transport::{Io, TdsTransport, TdsTransportInner};
use {TdsError, TdsResult, FromUint, DRIVER_VERSION};

/// The amount of bytes a packet header consists of
pub const HEADER_BYTES: usize = 8;
pub const ALL_HEADERS_LEN_TX: usize = 22;

/// serialize a simple message that only translates to ONE packet
pub trait SerializeMessage {
    fn serialize_message<I: Io>(&self, trans: &mut TdsTransport<I>) -> io::Result<Vec<u8>>;
}

pub trait UnserializeMessage<T> {
    fn unserialize_message<I: Io>(&self, trans: &mut TdsTransport<I>) -> TdsResult<T>;
}

/// the type of the packet [2.2.3.1.1]
#[repr(u8)]
#[derive(Copy, Clone, Debug, PartialEq)]
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
uint_to_enum!(PacketType, SQLBatch, PreTDSv7Login, RPC, TabularResult, AttentionSignal,
            BulkLoad, Fat, TransactionManagerReq, TDSv7Login, SSPI, PreLogin);

/// the message state [2.2.3.1.2]
#[repr(u8)]
#[derive(Copy, Clone, Debug, PartialEq)]
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
uint_to_enum!(PacketStatus, NormalMessage, EndOfMessage, IgnoreEvent, ResetConnection, ResetConnectionSkipTran);

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
}

impl PacketHeader {
    pub fn serialize(&self, target: &mut [u8]) -> io::Result<()> {
        let mut writer = Cursor::new(target);
        try!(writer.write_u8(self.ty as u8));
        try!(writer.write_u8(self.status as u8));
        try!(writer.write_u16::<BigEndian>(self.length));
        try!(writer.write_u16::<BigEndian>(self.spid));
        try!(writer.write_u8(self.id));
        writer.write_u8(self.window)
    }

    pub fn unserialize(buf: &[u8]) -> TdsResult<PacketHeader> {
        let mut cursor = Cursor::new(buf);
        Ok(PacketHeader {
            ty: try!(PacketType::from_u8(try!(cursor.read_u8())).ok_or(TdsError::Protocol("header: invalid packet type".into()))),
            status: try!(PacketStatus::from_u8(try!(cursor.read_u8())).ok_or(TdsError::Protocol("header: invalid packet status".into()))),
            length: try!(cursor.read_u16::<BigEndian>()),
            spid: try!(cursor.read_u16::<BigEndian>()),
            id: try!(cursor.read_u8()),
            window: try!(cursor.read_u8()),
         })
    }
}

#[repr(u32)]
#[derive(Debug, Copy, Clone)]
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
uint_to_enum!(FeatureLevel, SqlServerV7, SqlServer2000, SqlServer2000Sp1, SqlServer2005, SqlServer2008,
    SqlServer2008R2, SqlServerN);

/// The configured encryption level specifying if encryption is required
#[repr(u8)]
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
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
uint_to_enum!(EncryptionLevel, Off, On, NotSupported, Required);

/// The prelogin packet used to initialize a connection
#[derive(Debug)]
pub struct PreloginMessage {
    /// [BE] token=0x00
    /// Either the driver version or the version of the SQL server
    pub version: u32,
    pub sub_build: u16,
    /// token=0x01
    pub encryption: EncryptionLevel,
    /// [client] threadid for debugging purposes, token=0x03
    pub thread_id: u32,
    /// token=0x04
    pub mars: bool,
}

impl PreloginMessage {
    pub fn new() -> PreloginMessage {
        PreloginMessage {
            version: *DRIVER_VERSION as u32,
            sub_build: (*DRIVER_VERSION >> 32) as u16,
            encryption: EncryptionLevel::NotSupported,
            thread_id: 0,
            mars: false,
        }
    }
}

impl SerializeMessage for PreloginMessage {
    fn serialize_message<I: Io>(&self, ctx: &mut TdsTransport<I>) -> io::Result<Vec<u8>> {
        let mut cursor = Cursor::new(vec![0; HEADER_BYTES + 21]);
        cursor.set_position(8);

        // build the packet-body
        // offset = PL_OPTION_TOKEN + PL_OFFSET + PL_OPTION_LENGTH = 5 bytes + the terminator (0xFF)
        let mut data_offset = 4 * 5 + 1;

        // write the offsets
        {
            let mut write_option = |token: u8, length: u16| -> io::Result<()> {
                try!(cursor.write_u8(token));
                try!(cursor.write_u16::<BigEndian>(data_offset));
                try!(cursor.write_u16::<BigEndian>(length));
                data_offset += length;
                Ok(())
            };

            try!(write_option(0x00, 0x04 + 0x02)); // version + subbuild
            try!(write_option(0x01, 0x01)); // encryption
            try!(write_option(0x03, 0x04)); // threadid
            try!(write_option(0x04, 0x01)); // MARS
        }
        try!(cursor.write_u8(0xff));

        // write the data (body of the options)
        try!(cursor.write_u32::<BigEndian>(self.version as u32));
        try!(cursor.write_u16::<BigEndian>(self.sub_build as u16));
        try!(cursor.write_u8(self.encryption as u8));
        try!(cursor.write_u32::<BigEndian>(self.thread_id));
        try!(cursor.write_u8(self.mars as u8));

        // build the header
        let header = PacketHeader {
            ty: PacketType::PreLogin,
            status: PacketStatus::EndOfMessage,
            ..PacketHeader::new(cursor.get_ref().len(), ctx.next_id())
        };
        let mut vec = cursor.into_inner();
        try!(header.serialize(&mut vec));
        Ok(vec)
    }
}

impl<'a> UnserializeMessage<PreloginMessage> for &'a [u8] {
    fn unserialize_message<I: Io>(&self, _: &mut TdsTransport<I>) -> TdsResult<PreloginMessage> {
        let mut cursor = Cursor::new(self);
        let mut ret = PreloginMessage::new();

        // read all options
        loop {
            let token = try!(cursor.read_u8());
            // read until terminator
            if token == 0xff {
                break;
            }
            let offset = try!(cursor.read_u16::<BigEndian>());
            let length = try!(cursor.read_u16::<BigEndian>());
            let old_pos = cursor.position();
            cursor.set_position(offset as u64);
            // verify whether the server acts in accordance to what we requested
            // and if we can handle on what we seemingly agreed to
            // TODO: support parsing more
            match token {
                // version
                0 => {
                    ret.version = try!(cursor.read_u32::<BigEndian>());
                    ret.sub_build = try!(cursor.read_u16::<BigEndian>());
                },
                // encryption
                1 => {
                    let encrypt = try!(cursor.read_u8());
                    ret.encryption = try!(EncryptionLevel::from_u8(encrypt)
                        .ok_or(TdsError::Protocol(format!("invalid encryption value: {}", encrypt).into())));
                },
                3 => debug_assert_eq!(length, 0), // threadid
                4 => debug_assert_eq!(length, 1), // mars
                _ => panic!("unsupported prelogin token: {}", token)
            }
            cursor.set_position(old_pos);
        }
        Ok(ret)
    }
}


bitflags! {
    pub struct LoginOptionFlags1: u8 {
        const OF1_BIG_ENDIAN           = 0b00000001;
        /// Charset_EBDDIC, default/bit not set = Charset_ASCII
        const OF1_CHARSET_EBDDIC       = 0b00000010;
        /// default float is IEEE_754
        const OF1_FLOAT_VAX            = 0b00000100;
        const OF1_FLOAT_ND5000         = 0b00001000;
        const OF1_DUMPLOAD_ON          = 0b00010000;
        /// Set if the client requires warning messages on execution of the USE SQL
        /// statement. If this flag is NOT SET, the server MUST NOT inform the client when the database
        /// changes, and therefore the client will be unaware of any accompanying collation changes.
        const OF1_USE_DB_NOTIFY        = 0b00100000;
        /// Set if the change to initial database needs to succeed if the connection is to succeed. (false: warn)
        const OF1_INITIAL_DB_FATAL     = 0b01000000;
        /// Set if the client requires warning messages on execution of a language change statement.
        const OF1_LANG_CHANGE_WARN     = 0b10000000;
    }
}
bitflags! {
    pub struct LoginOptionFlags2: u8 {
        /// Set if the change to initial language needs to succeed if the connect is to succeed.
        const OF2_INIT_LANG_FATAL      = 0b00000001;
        /// Set if the client is the ODBC driver. This causes the server to set ANSI_DEFAULTS=ON,
        /// CURSOR_CLOSE_ON_COMMIT, IMPLICIT_TRANSACTIONS=OFF, TEXTSIZE=0x7FFFFFFF (2GB) (TDS 7.2 and earlier)
        /// TEXTSIZE to infinite (TDS 7.3), and ROWCOUNT to infinite
        /// (2.2.6.4)
        const OF2_ODBC_DRIVER          = 0b00000010;
        const OF2_TRANS_BOUNDARY       = 0b00000100;
        const OF2_CACHE_CONNECT        = 0b00001000;
        /// reserved
        const OF2_USER_TYPE_SERVER     = 0b00010000;
        /// Distributed Query login
        const OF2_USER_TYPE_REM_USER   = 0b00100000;
        /// Replication login
        const OF2_USER_TYPE_SQL_REPL   = 0b00110000;
        const OF2_INTEGRATED_SECURITY  = 0b10000000;
    }
}
bitflags! {
    pub struct LoginTypeFlags: u8 {
        /// use TSQL insteadof DFLT
        const LTF_SQL_TSQL             = 0b00000001;
        /// Set if the client is the OLEDB driver. This causes the server to set ANSI_DEFAULTS to ON ...
        const LTF_OLEDB_DRIVER         = 0b00010000;
        const LTF_READ_ONLY_INTENT     = 0b00100000;
    }
}
bitflags! {
    pub struct LoginOptionFlags3: u8 {
        const OF3_REQUEST_CHANGE_PWD   = 0b00000001;
        /// 1 if XML data type instances are returned as binary XML
        const OF3_SEND_YUKON_BINARY    = 0b00000010;
        /// 1 if client is requesting separate process to be spawned as user instance
        const OF3_SPAWN_USER_INSTANCE  = 0b00000100;
        /// 0 = The server MUST restrict the collations sent to a specific set of collations.
        /// 1 = The server MAY send any collation that fits in the storage space.
        const OF3_SUPPORT_UNKNOWN_COLL = 0b00001000;
        // TODO: fExtension?
    }
}

/// the login packet
pub struct LoginMessage<'a> {
    /// the highest TDS version the client supports
    pub tds_version: FeatureLevel,
    /// the requested packet size
    pub packet_size: u32,
    /// the version of the interface library
    pub client_prog_ver: u32,
    /// the process id of the client application
    pub client_pid: u32,
    /// the connection id of the primary server
    /// (used when connecting to an "Always UP" backup server)
    pub connection_id: u32,

    pub option_flags_1: LoginOptionFlags1,
    pub option_flags_2: LoginOptionFlags2,

    /// flag included in option_flags_2
    pub integrated_security: Option<Vec<u8>>,
    pub type_flags: LoginTypeFlags,
    pub option_flags_3: LoginOptionFlags3,

    pub client_timezone: i32,
    pub client_lcid: u32,

    pub hostname: Cow<'a, str>,
    pub username: Cow<'a, str>,
    pub password: Cow<'a, str>,
    pub app_name: Cow<'a, str>,
    pub server_name: Cow<'a, str>,
    /// the default database to connect to
    pub db_name: Cow<'a, str>,
}

impl<'a> LoginMessage<'a> {
    pub fn new() -> LoginMessage<'a> {
        LoginMessage {
            tds_version: FeatureLevel::SqlServerN,
            packet_size: 4096,
            client_prog_ver: 0,
            client_pid: 0,
            connection_id: 0,
            option_flags_1: OF1_USE_DB_NOTIFY | OF1_INITIAL_DB_FATAL,
            option_flags_2: OF2_INIT_LANG_FATAL | OF2_ODBC_DRIVER,
            integrated_security: None,
            type_flags: LoginTypeFlags::empty(),
            option_flags_3: OF3_SUPPORT_UNKNOWN_COLL,
            client_timezone: 0, //TODO
            client_lcid: 0, // TODO
            hostname: "".into(),
            username: "".into(),
            password: "".into(),
            app_name: "".into(),
            server_name: "".into(),
            db_name:  "".into(),
        }
    }
}

impl<'a> SerializeMessage for LoginMessage<'a> {
    fn serialize_message<I: Io>(&self, trans: &mut TdsTransport<I>) -> io::Result<Vec<u8>> {
        let mut cursor = Cursor::new(Vec::with_capacity(1<<9));

        cursor.set_position(HEADER_BYTES as u64 + 4);

        // ignore the specified value for integrated security since we determine that by the struct field
        let option_flags2 = if self.integrated_security.is_some() {
            self.option_flags_2 | OF2_INTEGRATED_SECURITY
        } else {
            self.option_flags_2 & !OF2_INTEGRATED_SECURITY
        };
        // write..
        for val in &[self.tds_version as u32, self.packet_size, self.client_prog_ver, self.client_pid, self.connection_id] {
            try!(cursor.write_u32::<LittleEndian>(*val));
        }
        for val in &[self.option_flags_1.bits(), option_flags2.bits(), self.type_flags.bits(), self.option_flags_3.bits()] {
            try!(cursor.write_u8(*val));
        }
        for val in &[self.client_timezone as u32, self.client_lcid] {
            try!(cursor.write_u32::<LittleEndian>(*val));
        }

        // variable length data (OffsetLength)
        let var_data = [
            &self.hostname, &self.username, &self.password, &self.app_name, &self.server_name,
            &"".into(), // 5. ibExtension
            &"".into(), // ibCltIntName
            &"".into(), // ibLanguage
            &self.db_name,
            &"".into(), // 9. ClientId (6 bytes); this is included in var_data so we don't lack the bytes of cbSspiLong (4=2*2) and can insert it at the correct position
            &"".into(), // 10. ibSSPI
            &"".into(), // ibAtchDBFile
            &"".into(), // ibChangePassword
        ];

        let mut data_offset = cursor.position() as usize + var_data.len() * 2*2 + 6;

        for (i, value) in var_data.into_iter().enumerate() {
            // write the client ID (created from the MAC address)
            if i == 9 {
                try!(cursor.write_u32::<LittleEndian>(0)); //TODO:
                try!(cursor.write_u16::<LittleEndian>(42)); //TODO: generate real client id
                continue;
            }
            try!(cursor.write_u16::<LittleEndian>((data_offset - HEADER_BYTES) as u16));
            if i == 10 {
                let length = if let Some(ref bytes) = self.integrated_security {
                    let bak = cursor.position();
                    cursor.set_position(data_offset as u64);
                    try!(cursor.write_all(bytes));
                    data_offset += bytes.len();
                    cursor.set_position(bak);
                    bytes.len()
                } else {
                    0
                };
                try!(cursor.write_u16::<LittleEndian>(length as u16));
                continue;
            }

            // jump into the data portion of the output
            let bak = cursor.position();
            cursor.set_position(data_offset as u64);
            for codepoint in value.encode_utf16() {
                try!(cursor.write_u16::<LittleEndian>(codepoint));
            }
            let new_position = cursor.position() as usize;
            // prepare the password in MS-fashion
            if i == 2 {
                let buffer = cursor.get_mut();
                for idx in data_offset..new_position {
                    let byte = buffer[idx];
                    buffer[idx] = ((byte << 4) & 0xf0 | (byte >> 4) & 0x0f) ^ 0xA5;
                }
            }
            let length = new_position - data_offset;
            cursor.set_position(bak);
            data_offset += length;

            // microsoft being really consistent here... using byte offsets with utf16-length's
            // sounds like premature optimization
            try!(cursor.write_u16::<LittleEndian>(length as u16 / 2));
        }
        // cbSSPILong
        try!(cursor.write_u32::<LittleEndian>(0));

        cursor.set_position(data_offset as u64);
        // FeatureExt: unsupported for now, simply write a terminator
        try!(cursor.write_u8(0xFF));

        // build the header
        let header = PacketHeader {
            ty: PacketType::TDSv7Login,
            status: PacketStatus::EndOfMessage,
            ..PacketHeader::new(cursor.get_ref().len(), trans.next_id())
        };
        cursor.set_position(HEADER_BYTES as u64);
        try!(cursor.write_u32::<LittleEndian>(header.length as u32 - HEADER_BYTES as u32));
        let mut buf = cursor.into_inner();
        try!(header.serialize(&mut buf));

        Ok(buf)
     }
}

pub struct SspiMessage(pub Vec<u8>);

impl SerializeMessage for SspiMessage {
    fn serialize_message<I: Io>(&self, ctx: &mut TdsTransport<I>) -> io::Result<Vec<u8>> {
        let len = HEADER_BYTES + self.0.len();
        let mut buf: Vec<u8> = vec![0; len];
        try!((&mut buf[HEADER_BYTES..]).write_all(&self.0));

        // build the header
        let header = PacketHeader {
            ty: PacketType::TDSv7Login,
            status: PacketStatus::EndOfMessage,
            ..PacketHeader::new(len, ctx.next_id())
        };
        try!(header.serialize(&mut buf));
        Ok(buf)
    }
}

#[allow(dead_code)]
#[derive(Debug)]
#[repr(u16)]
pub enum AllHeaderTy {
    QueryDescriptor = 1,
    TransactionDescriptor = 2,
    TraceActivity = 3,
}

pub fn write_trans_descriptor<W: Write>(mut wr: W, id: u64) -> io::Result<()> {
    try!(wr.write_u32::<LittleEndian>(ALL_HEADERS_LEN_TX as u32));
    try!(wr.write_u32::<LittleEndian>(ALL_HEADERS_LEN_TX as u32 - 4));
    try!(wr.write_u16::<LittleEndian>(AllHeaderTy::TransactionDescriptor as u16));
    // transaction descriptor
    try!(wr.write_u64::<LittleEndian>(id));
    // outstanding requests (TransactionDescrHeader)
    wr.write_u32::<LittleEndian>(1)
}

/// build an SQL batch packet
pub fn build_sql_batch<I: Io>(trans: &mut TdsTransport<I>, query: &str) -> io::Result<Vec<u8>> {
    let vec = vec![0u8; HEADER_BYTES + ALL_HEADERS_LEN_TX as usize];
    let mut cursor = Cursor::new(vec);
    cursor.set_position(8);

    try!(write_trans_descriptor(&mut cursor, trans.transaction));

    // the SQL query (after ALL_HEADERS)
    for byte in query.encode_utf16() {
        try!(cursor.write_u16::<LittleEndian>(byte));
    }
    // build the packet header
    let header = PacketHeader {
        ty: PacketType::SQLBatch,
        status: PacketStatus::EndOfMessage,
        ..PacketHeader::new(cursor.get_ref().len(), trans.next_id())
    };
    cursor.set_position(0);
    let mut buf = cursor.into_inner();
    try!(header.serialize(&mut buf));
    Ok(buf)
}

/// a writer that splits the written data across multiple packets
pub struct PacketWriter<'a, I: 'a + Io> {
    transport: &'a mut TdsTransportInner<I>,
    header: PacketHeader,
    buf: Vec<u8>,
}

#[inline]
fn new_packet_buf(capacity: usize) -> Vec<u8> {
    let mut buf = Vec::with_capacity(capacity);
    buf.resize(HEADER_BYTES, 0);
    buf
}

impl<'a, I: 'a + Io> PacketWriter<'a, I> {
    pub fn new(transport: &'a mut TdsTransportInner<I>, header: PacketHeader) -> Self {
        PacketWriter {
            header: header,
            buf: new_packet_buf(transport.packet_size),
            transport: transport,
        }
    }

    pub fn finalize(mut self) -> io::Result<()> {
        self.header.status = PacketStatus::EndOfMessage;
        self.flush()
    }
}

impl<'a, I: Io> Write for PacketWriter<'a, I> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        // fast path for small writes
        if self.buf.capacity() - self.buf.len() > buf.len() {
            self.buf.extend_from_slice(buf);
            return Ok(buf.len());
        }

        let mut pending = buf;
        while !pending.is_empty() {
            let max_bytes = self.buf.capacity() - self.buf.len();
            let (fitting, next) = pending.split_at(cmp::min(max_bytes, pending.len()));
            self.buf.extend_from_slice(fitting);
            if self.buf.capacity() <= self.buf.len() {
                try!(self.flush());
            }
            pending = next;
        }
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        let mut buf = mem::replace(&mut self.buf, new_packet_buf(self.transport.packet_size));
        if !buf.is_empty() {
            // update the packet header
            self.header.id = self.transport.next_id();
            self.header.length = buf.len() as u16;
            try!(self.header.serialize(&mut buf));
            try!(self.transport.queue_vec(buf));
        }
        let _ = try!(self.transport.poll_complete());
        Ok(())
    }
}

pub struct PLPChunkWriter<W: Write> {
    pub target: W,
    pub buf: Vec<u8>,
}

impl<W: Write> io::Write for PLPChunkWriter<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        // fast path for small writes
        if self.buf.capacity() - self.buf.len() > buf.len() {
            self.buf.extend_from_slice(buf);
            return Ok(buf.len());
        }

        let mut pending = buf;
        while !pending.is_empty() {
            let free_bytes = self.buf.capacity() - self.buf.len();
            let boundary = cmp::min(pending.len(), free_bytes);
            let (fitting, next) = pending.split_at(boundary);

            // we can produce a whole chunk => write to the underlying buf
            if fitting.len() == free_bytes {
                try!(self.target.write_u32::<LittleEndian>(self.buf.capacity() as u32));
                try!(self.target.write_all(&self.buf));
                try!(self.target.write_all(fitting));
                self.buf.truncate(0);
            } else {
                self.buf.extend_from_slice(fitting);
            }
            pending = next;
        }
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        if !self.buf.is_empty() {
            try!(self.target.write_u32::<LittleEndian>(self.buf.len() as u32));
            try!(self.target.write_all(&self.buf));
            self.buf.truncate(0);
        }
        Ok(())
    }
}

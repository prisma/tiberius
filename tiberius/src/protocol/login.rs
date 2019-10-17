use crate::{protocol, Error, Result};
use bitflags::bitflags;
use byteorder::{BigEndian, LittleEndian, ReadBytesExt, WriteBytesExt};
use std::borrow::Cow;
use std::convert::TryFrom;
use std::io::{self, Cursor, Write};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

/// The prelogin packet used to initialize a connection
#[derive(Debug)]
pub struct PreloginMessage {
    /// [BE] token=0x00
    /// Either the driver version or the version of the SQL server
    pub version: u32,
    pub sub_build: u16,
    /// token=0x01
    pub encryption: protocol::EncryptionLevel,
    /// [client] threadid for debugging purposes, token=0x03
    pub thread_id: u32,
    /// token=0x04
    pub mars: bool,
}

impl PreloginMessage {
    pub fn new() -> PreloginMessage {
        let driver_version = crate::get_driver_version();
        PreloginMessage {
            version: driver_version as u32,
            sub_build: (driver_version >> 32) as u16,
            encryption: protocol::EncryptionLevel::NotSupported,
            thread_id: 0,
            mars: false,
        }
    }

    pub async fn unserialize<'a, C: AsyncRead + Unpin>(
        r: &mut protocol::PacketReader<'a, C>,
    ) -> Result<PreloginMessage> {
        let header = r.read_packet().await?;
        if header.ty != protocol::PacketType::TabularResult {
            return Err(Error::Protocol(
                "expected tabular result in response to prelogin message".into(),
            ));
        }

        let mut cursor = Cursor::new(&r.buf);
        let mut ret = PreloginMessage::new();

        // read all options
        loop {
            let token = cursor.read_u8()?;
            // read until terminator
            if token == 0xff {
                break;
            }
            let offset = cursor.read_u16::<BigEndian>()?;
            let length = cursor.read_u16::<BigEndian>()?;
            let old_pos = cursor.position();
            cursor.set_position(offset as u64);
            // verify whether the server acts in accordance to what we requested
            // and if we can handle on what we seemingly agreed to
            // TODO: support parsing more
            match token {
                // version
                0 => {
                    ret.version = cursor.read_u32::<BigEndian>()?;
                    ret.sub_build = cursor.read_u16::<BigEndian>()?;
                }
                // encryption
                1 => {
                    let encrypt = cursor.read_u8()?;
                    ret.encryption =
                        protocol::EncryptionLevel::try_from(encrypt).map_err(|_| {
                            Error::Protocol(format!("invalid encryption value: {}", encrypt).into())
                        })?;
                }
                3 => debug_assert_eq!(length, 0), // threadid
                4 => debug_assert_eq!(length, 1), // mars
                _ => panic!("unsupported prelogin token: {}", token),
            }
            cursor.set_position(old_pos);
        }

        Ok(ret)
    }

    /// Serialize the prelogin message, leaving the header zeroed and for the caller to set
    pub fn serialize(&self) -> io::Result<Vec<u8>> {
        let mut cursor = Cursor::new(vec![0; protocol::HEADER_BYTES + 21]);
        cursor.set_position(protocol::HEADER_BYTES as u64);

        // build the packet-body
        // offset = PL_OPTION_TOKEN + PL_OFFSET + PL_OPTION_LENGTH = 5 bytes + the terminator (0xFF)
        let mut data_offset = 4 * 5 + 1;

        // write the offsets
        {
            let mut write_option = |token: u8, length: u16| -> io::Result<()> {
                cursor.write_u8(token)?;
                cursor.write_u16::<BigEndian>(data_offset)?;
                cursor.write_u16::<BigEndian>(length)?;
                data_offset += length;
                Ok(())
            };

            write_option(0x00, 0x04 + 0x02)?; // version + subbuild
            write_option(0x01, 0x01)?; // encryption
            write_option(0x03, 0x04)?; // threadid
            write_option(0x04, 0x01)?; // MARS
        }
        cursor.write_u8(0xff)?;

        // write the data (body of the options)
        cursor.write_u32::<BigEndian>(self.version as u32)?;
        cursor.write_u16::<BigEndian>(self.sub_build as u16)?;
        cursor.write_u8(self.encryption as u8)?;
        cursor.write_u32::<BigEndian>(self.thread_id)?;
        cursor.write_u8(self.mars as u8)?;
        Ok(cursor.into_inner())
    }
}

bitflags! {
    pub struct LoginOptionFlags1: u8 {
        const BIG_ENDIAN           = 0b00000001;
        /// Charset_EBDDIC, default/bit not set = Charset_ASCII
        const CHARSET_EBDDIC       = 0b00000010;
        /// default float is IEEE_754
        const FLOAT_VAX            = 0b00000100;
        const FLOAT_ND5000         = 0b00001000;
        const DUMPLOAD_ON          = 0b00010000;
        /// Set if the client requires warning messages on execution of the USE SQL
        /// statement. If this flag is NOT SET, the server MUST NOT inform the client when the database
        /// changes, and therefore the client will be unaware of any accompanying collation changes.
        const USE_DB_NOTIFY        = 0b00100000;
        /// Set if the change to initial database needs to succeed if the connection is to succeed. (false: warn)
        const INITIAL_DB_FATAL     = 0b01000000;
        /// Set if the client requires warning messages on execution of a language change statement.
        const LANG_CHANGE_WARN     = 0b10000000;
    }
}
bitflags! {
    pub struct LoginOptionFlags2: u8 {
        /// Set if the change to initial language needs to succeed if the connect is to succeed.
        const INIT_LANG_FATAL      = 0b00000001;
        /// Set if the client is the ODBC driver. This causes the server to set ANSI_DEFAULTS=ON,
        /// CURSOR_CLOSE_ON_COMMIT, IMPLICIT_TRANSACTIONS=OFF, TEXTSIZE=0x7FFFFFFF (2GB) (TDS 7.2 and earlier)
        /// TEXTSIZE to infinite (TDS 7.3), and ROWCOUNT to infinite
        /// (2.2.6.4)
        const ODBC_DRIVER          = 0b00000010;
        const TRANS_BOUNDARY       = 0b00000100;
        const CACHE_CONNECT        = 0b00001000;
        /// reserved
        const USER_TYPE_SERVER     = 0b00010000;
        /// Distributed Query login
        const USER_TYPE_REM_USER   = 0b00100000;
        /// Replication login
        const USER_TYPE_SQL_REPL   = 0b00110000;
        const INTEGRATED_SECURITY  = 0b10000000;
    }
}
bitflags! {
    pub struct LoginTypeFlags: u8 {
        /// use TSQL insteadof DFLT
        const SQL_TSQL             = 0b00000001;
        /// Set if the client is the OLEDB driver. This causes the server to set ANSI_DEFAULTS to ON ...
        const OLEDB_DRIVER         = 0b00010000;
        const READ_ONLY_INTENT     = 0b00100000;
    }
}
bitflags! {
    pub struct LoginOptionFlags3: u8 {
        const REQUEST_CHANGE_PWD   = 0b00000001;
        /// 1 if XML data type instances are returned as binary XML
        const SEND_YUKON_BINARY    = 0b00000010;
        /// 1 if client is requesting separate process to be spawned as user instance
        const SPAWN_USER_INSTANCE  = 0b00000100;
        /// 0 = The server MUST restrict the collations sent to a specific set of collations.
        /// 1 = The server MAY send any collation that fits in the storage space.
        const SUPPORT_UNKNOWN_COLL = 0b00001000;
        // TODO: fExtension?
    }
}

/// the login packet
pub struct LoginMessage<'a> {
    /// the highest TDS version the client supports
    pub tds_version: protocol::FeatureLevel,
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
            tds_version: protocol::FeatureLevel::SqlServerN,
            packet_size: 4096,
            client_prog_ver: 0,
            client_pid: 0,
            connection_id: 0,
            option_flags_1: LoginOptionFlags1::USE_DB_NOTIFY | LoginOptionFlags1::INITIAL_DB_FATAL,
            option_flags_2: LoginOptionFlags2::INIT_LANG_FATAL | LoginOptionFlags2::ODBC_DRIVER,
            integrated_security: None,
            type_flags: LoginTypeFlags::empty(),
            option_flags_3: LoginOptionFlags3::SUPPORT_UNKNOWN_COLL,
            client_timezone: 0, //TODO
            client_lcid: 0,     // TODO
            hostname: "".into(),
            username: "".into(),
            password: "".into(),
            app_name: "".into(),
            server_name: "".into(),
            db_name: "".into(),
        }
    }

    pub fn serialize(&self, ctx: &protocol::Context) -> io::Result<Vec<u8>> {
        let mut cursor = Cursor::new(Vec::with_capacity(1 << 9));

        cursor.set_position(protocol::HEADER_BYTES as u64 + 4);

        // ignore the specified value for integrated security since we determine that by the struct field
        let option_flags2 = if self.integrated_security.is_some() {
            self.option_flags_2 | LoginOptionFlags2::INTEGRATED_SECURITY
        } else {
            self.option_flags_2 & !LoginOptionFlags2::INTEGRATED_SECURITY
        };
        // write..
        for val in &[
            self.tds_version as u32,
            self.packet_size,
            self.client_prog_ver,
            self.client_pid,
            self.connection_id,
        ] {
            cursor.write_u32::<LittleEndian>(*val)?;
        }
        for val in &[
            self.option_flags_1.bits(),
            option_flags2.bits(),
            self.type_flags.bits(),
            self.option_flags_3.bits(),
        ] {
            cursor.write_u8(*val)?;
        }
        for val in &[self.client_timezone as u32, self.client_lcid] {
            cursor.write_u32::<LittleEndian>(*val)?;
        }

        // variable length data (OffsetLength)
        let var_data = [
            &self.hostname,
            &self.username,
            &self.password,
            &self.app_name,
            &self.server_name,
            &"".into(), // 5. ibExtension
            &"".into(), // ibCltIntName
            &"".into(), // ibLanguage
            &self.db_name,
            &"".into(), // 9. ClientId (6 bytes); this is included in var_data so we don't lack the bytes of cbSspiLong (4=2*2) and can insert it at the correct position
            &"".into(), // 10. ibSSPI
            &"".into(), // ibAtchDBFile
            &"".into(), // ibChangePassword
        ];

        let mut data_offset = cursor.position() as usize + var_data.len() * 2 * 2 + 6;

        for (i, value) in var_data.into_iter().enumerate() {
            // write the client ID (created from the MAC address)
            if i == 9 {
                cursor.write_u32::<LittleEndian>(0)?; //TODO:
                cursor.write_u16::<LittleEndian>(42)?; //TODO: generate real client id
                continue;
            }
            cursor.write_u16::<LittleEndian>((data_offset - protocol::HEADER_BYTES) as u16)?;
            if i == 10 {
                let length = if let Some(ref bytes) = self.integrated_security {
                    let bak = cursor.position();
                    cursor.set_position(data_offset as u64);
                    cursor.write_all(bytes)?;
                    data_offset += bytes.len();
                    cursor.set_position(bak);
                    bytes.len()
                } else {
                    0
                };
                cursor.write_u16::<LittleEndian>(length as u16)?;
                continue;
            }

            // jump into the data portion of the output
            let bak = cursor.position();
            cursor.set_position(data_offset as u64);
            for codepoint in value.encode_utf16() {
                cursor.write_u16::<LittleEndian>(codepoint)?;
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
            cursor.write_u16::<LittleEndian>(length as u16 / 2)?;
        }
        // cbSSPILong
        cursor.write_u32::<LittleEndian>(0)?;

        cursor.set_position(data_offset as u64);
        // FeatureExt: unsupported for now, simply write a terminator
        cursor.write_u8(0xFF)?;

        cursor.set_position(protocol::HEADER_BYTES as u64);
        cursor.write_u32::<LittleEndian>(
            cursor.get_ref().len() as u32 - protocol::HEADER_BYTES as u32,
        )?;
        Ok(cursor.into_inner())
    }
}

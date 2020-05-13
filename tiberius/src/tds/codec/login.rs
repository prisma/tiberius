use super::Encode;
use bitflags::bitflags;
use byteorder::{LittleEndian, WriteBytesExt};
use bytes::BytesMut;
use io::{Cursor, Write};
use std::{borrow::Cow, io};

uint_enum! {
    #[repr(u32)]
    #[derive(PartialOrd)]
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

impl FeatureLevel {
    pub fn done_row_count_bytes(self) -> u8 {
        if self as u8 >= FeatureLevel::SqlServer2005 as u8 {
            8
        } else {
            4
        }
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
}

impl<'a> Encode<BytesMut> for LoginMessage<'a> {
    fn encode(self, dst: &mut BytesMut) -> crate::Result<()> {
        let mut cursor = Cursor::new(Vec::with_capacity(512));

        // Space for the length
        cursor.write_u32::<LittleEndian>(0)?;

        // ignore the specified value for integrated security since we determine that by the struct field
        let option_flags2 = if self.integrated_security.is_some() {
            self.option_flags_2 | LoginOptionFlags2::INTEGRATED_SECURITY
        } else {
            self.option_flags_2 & !LoginOptionFlags2::INTEGRATED_SECURITY
        };

        cursor.write_u32::<LittleEndian>(self.tds_version as u32)?;
        cursor.write_u32::<LittleEndian>(self.packet_size)?;
        cursor.write_u32::<LittleEndian>(self.client_prog_ver)?;
        cursor.write_u32::<LittleEndian>(self.client_pid)?;
        cursor.write_u32::<LittleEndian>(self.connection_id)?;

        cursor.write_u8(self.option_flags_1.bits())?;
        cursor.write_u8(option_flags2.bits())?;
        cursor.write_u8(self.type_flags.bits())?;
        cursor.write_u8(self.option_flags_3.bits())?;

        cursor.write_u32::<LittleEndian>(self.client_timezone as u32)?;
        cursor.write_u32::<LittleEndian>(self.client_lcid)?;

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

        for (i, value) in var_data.iter().enumerate() {
            // write the client ID (created from the MAC address)
            if i == 9 {
                cursor.write_u32::<LittleEndian>(0)?; //TODO:
                cursor.write_u16::<LittleEndian>(42)?; //TODO: generate real client id
                continue;
            }

            cursor.write_u16::<LittleEndian>(data_offset as u16)?;

            // ibSSPI
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

        cursor.set_position(0);
        cursor.write_u32::<LittleEndian>(cursor.get_ref().len() as u32)?;

        dst.extend(cursor.into_inner());

        Ok(())
    }
}

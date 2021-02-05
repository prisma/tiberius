use super::Encode;
use byteorder::{LittleEndian, WriteBytesExt};
use bytes::BytesMut;
use enumflags2::BitFlags;
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

impl Default for FeatureLevel {
    fn default() -> Self {
        Self::SqlServerN
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

#[derive(Debug, Clone, Copy, BitFlags, PartialEq)]
#[repr(u8)]
pub enum OptionFlag1 {
    /// The byte order used by client for numeric and datetime data types.
    /// (default: little-endian)
    BigEndian = 0x01,
    /// The character set used on the client. (default: ASCII)
    CharsetEBDDIC = 0x02,
    /// Use VAX floating point representation. (default: IEEE 754)
    FloatVax = 0x04,
    /// Use ND5000 floating point representation. (default: IEEE 754)
    FloatND5000 = 0x08,
    /// Set is dump/load or BCP capabilities are needed by the client.
    /// (default: ON)
    BcpDumploadOff = 0x10,
    /// Set if the client requires warning messages on execution of the USE SQL
    /// statement. If this flag is not set, the server MUST NOT inform the
    /// client when the database changes, and therefore the client will be
    /// unaware of any accompanying collation changes. (default: ON)
    UseDbNotify = 0x20,
    /// Set if the change to initial database needs to succeed if the connection
    /// is to succeed. (default: OFF)
    InitDbFatal = 0x40,
    /// Set if the client requires warning messages on execution of a language
    /// change statement. (default: OFF)
    LangChangeWarn = 0x80,
}

#[derive(Debug, Clone, Copy, BitFlags, PartialEq)]
#[repr(u8)]
pub enum OptionFlag2 {
    /// Set if the change to initial language needs to succeed if the connect is
    /// to succeed.
    InitLangFatal = 0x01,
    /// Set if the client is the ODBC driver. This causes the server to set
    /// `ANSI_DEFAULTS=ON`, `CURSOR_CLOSE_ON_COMMIT`, `IMPLICIT_TRANSACTIONS=OFF`,
    /// `TEXTSIZE=0x7FFFFFFF` (2GB) (TDS 7.2 and earlier) `TEXTSIZE` to infinite
    /// (TDS 7.3), and `ROWCOUNT` to infinite.
    OdbcDriver = 0x02,
    /// (not documented)
    TransBoundary = 0x04,
    /// (not documented)
    CacheConnect = 0x08,
    /// Reserved (not really documented)
    UserTypeServer = 0x10,
    /// Distributed Query login
    UserTypeRemUser = 0x20,
    /// Replication login
    UserTypeSqlRepl = 0x40,
    /// Use integrated security in the client.
    IntegratedSecurity = 0x80,
}

#[derive(Debug, Clone, Copy, BitFlags, PartialEq)]
#[repr(u8)]
pub enum OptionFlag3 {
    /// Request to change login's password.
    RequestChangePassword = 0x01,
    /// XML data type instances are returned as binary XML.
    BinaryXML = 0x02,
    /// Client is requesting separate process to be spawned as user instance.
    SpawnUserInstance = 0x04,
    /// This bit is used by the server to determine if a client is able to
    /// properly handle collations introduced after TDS 7.2. TDS 7.2 and earlier
    /// clients are encouraged to use this loginpacket bit. Servers MUST ignore
    /// this bit when it is sent by TDS 7.3 or 7.4 clients.
    UnknownCollationHandling = 0x08,
    /// ibExtension/cbExtension fields are used.
    ExtensionUsed = 0x10,
}

#[derive(Debug, Clone, Copy, BitFlags, PartialEq)]
#[repr(u8)]
pub enum LoginTypeFlag {
    /// Use T-SQL syntax.
    UseTSQL = 0x01,
    /// Set if the client is the OLEDB driver. This causes the server to set
    /// ANSI_DEFAULTS to ON, CURSOR_CLOSE_ON_COMMIT and IMPLICIT_TRANSACTIONS to
    /// OFF, TEXTSIZE to 0x7FFFFFFF (2GB) (TDS 7.2 and earlier), TEXTSIZE to
    /// infinite (introduced in TDS 7.3), and ROWCOUNT to infinite.
    UseOLEDB = 0x10,
    /// This bit was introduced in TDS 7.4; however, TDS 7.1, 7.2, and 7.3
    /// clients can also use this bit in LOGIN7 to specify that the application
    /// intent of the connection is read-only. The server SHOULD ignore this bit
    /// if the highest TDS version supported by the server is lower than TDS 7.4.
    ReadOnlyIntent = 0x20,
}

/// the login packet
#[derive(Debug, Clone, Default)]
pub struct LoginMessage<'a> {
    /// the highest TDS version the client supports
    tds_version: FeatureLevel,
    /// the requested packet size
    packet_size: u32,
    /// the version of the interface library
    client_prog_ver: u32,
    /// the process id of the client application
    client_pid: u32,
    /// the connection id of the primary server
    /// (used when connecting to an "Always UP" backup server)
    connection_id: u32,
    option_flags_1: BitFlags<OptionFlag1>,
    option_flags_2: BitFlags<OptionFlag2>,
    /// flag included in option_flags_2
    integrated_security: Option<Vec<u8>>,
    type_flags: BitFlags<LoginTypeFlag>,
    option_flags_3: BitFlags<OptionFlag3>,
    client_timezone: i32,
    client_lcid: u32,
    hostname: Cow<'a, str>,
    username: Cow<'a, str>,
    password: Cow<'a, str>,
    app_name: Cow<'a, str>,
    server_name: Cow<'a, str>,
    /// the default database to connect to
    db_name: Cow<'a, str>,
}

impl<'a> LoginMessage<'a> {
    pub fn new() -> LoginMessage<'a> {
        Self {
            packet_size: 4096,
            option_flags_1: OptionFlag1::UseDbNotify | OptionFlag1::InitDbFatal,
            option_flags_2: OptionFlag2::InitLangFatal | OptionFlag2::OdbcDriver,
            option_flags_3: BitFlags::from_flag(OptionFlag3::UnknownCollationHandling),
            app_name: "tiberius".into(),
            ..Default::default()
        }
    }

    #[cfg(any(all(unix, feature = "integrated-auth-gssapi"), windows))]
    pub fn integrated_security(&mut self, bytes: Option<Vec<u8>>) {
        if bytes.is_some() {
            self.option_flags_2.insert(OptionFlag2::IntegratedSecurity);
        } else {
            self.option_flags_2.remove(OptionFlag2::IntegratedSecurity);
        }

        self.integrated_security = bytes;
    }

    pub fn db_name(&mut self, db_name: impl Into<Cow<'a, str>>) {
        self.db_name = db_name.into();
    }

    pub fn server_name(&mut self, server_name: impl Into<Cow<'a, str>>) {
        self.server_name = server_name.into();
    }

    pub fn user_name(&mut self, user_name: impl Into<Cow<'a, str>>) {
        self.username = user_name.into();
    }

    pub fn password(&mut self, password: impl Into<Cow<'a, str>>) {
        self.password = password.into();
    }
}

impl<'a> Encode<BytesMut> for LoginMessage<'a> {
    fn encode(self, dst: &mut BytesMut) -> crate::Result<()> {
        let mut cursor = Cursor::new(Vec::with_capacity(512));

        // Space for the length
        cursor.write_u32::<LittleEndian>(0)?;

        cursor.write_u32::<LittleEndian>(self.tds_version as u32)?;
        cursor.write_u32::<LittleEndian>(self.packet_size)?;
        cursor.write_u32::<LittleEndian>(self.client_prog_ver)?;
        cursor.write_u32::<LittleEndian>(self.client_pid)?;
        cursor.write_u32::<LittleEndian>(self.connection_id)?;

        cursor.write_u8(self.option_flags_1.bits())?;
        cursor.write_u8(self.option_flags_2.bits())?;
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

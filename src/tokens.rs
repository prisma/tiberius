///! token stream definitions
use std::sync::Arc;
use byteorder::{ReadBytesExt, LittleEndian, BigEndian};
use futures::{Async, Poll};
use tokio_core::io::Io;
use transport::{TdsBuf, TdsTransport};
use types::{TypeInfo, ColumnData};
use {TdsError, FromUint};

/// read a token from an underlying transport
pub trait ParseToken<I: Io> {
    fn parse_token(&mut TdsTransport<I>) -> Poll<TdsResponseToken, TdsError>;
}

#[derive(Copy, Clone, Debug)]
pub enum Tokens {
    Info = 0xAB,
    LoginAck = 0xAD,
    Row = 0xD1,
    SSPI = 0xED,
    EnvChange = 0xE3,
    Done = 0xFD,
    ColMetaData = 0x81,
}
uint_to_enum!(Tokens, Info, LoginAck, Row, SSPI, EnvChange, Done, ColMetaData);

#[derive(Debug)]
pub enum TdsResponseToken {
    SSPI(TdsBuf),
    EnvChange(TokenEnvChange),
    Info(TokenInfo),
    LoginAck(TokenLoginAck),
    Done(TokenDone),
    ColMetaData(Arc<TokenColMetaData>),
    Row(TokenRow),
}

impl<I: Io> TdsTransport<I> {
    pub fn parse_token(&mut self, token: Tokens, min_len: usize) -> Poll<TdsResponseToken, TdsError> {
        match token {
            Tokens::SSPI => {
                if let Some(bytes) = self.read_bytes(min_len) {
                    return Ok(Async::Ready(TdsResponseToken::SSPI(bytes)))
                }
                Ok(Async::NotReady)
            },
            Tokens::EnvChange => TokenEnvChange::parse_token(self),
            Tokens::Info => TokenInfo::parse_token(self),
            Tokens::LoginAck => TokenLoginAck::parse_token(self),
            Tokens::Done => TokenDone::parse_token(self),
            Tokens::ColMetaData => TokenColMetaData::parse_token(self),
            Tokens::Row => TokenRow::parse_token(self),
        }
    }
}

#[derive(Debug)]
pub enum TokenEnvChange {
    Database(TdsBuf, TdsBuf),
    PacketSize(u32, u32),
    SqlCollation(TdsBuf, TdsBuf),
}

#[repr(u8)]
enum EnvChangeTy {
    Database = 1,
    Language = 2,
    CharacterSet = 3,
    PacketSize = 4,
    UnicodeDataSortingLID = 5,
    UnicodeDataSortingCFL = 6,
    SqlCollation = 7,
    /// below here: >= TDSv7.2
    BeginTransaction = 8,
    CommitTransaction = 9,
    RollbackTransaction = 10,
    EnlistDTCTransaction = 11,
    DefectTransaction = 12,
    RTLS = 13,
    PromoteTransaction = 15,
    TransactionManagerAddress = 16,
    TransactionEnded = 17,
    ResetConnection = 18,
    UserName = 19,
    /// below here: TDS v7.4
    Routing = 20,
}
uint_to_enum!(EnvChangeTy, Database, Language, CharacterSet, PacketSize, UnicodeDataSortingLID, UnicodeDataSortingCFL, SqlCollation,
    BeginTransaction, CommitTransaction, RollbackTransaction, EnlistDTCTransaction, DefectTransaction, RTLS,
    PromoteTransaction, TransactionManagerAddress, TransactionEnded, ResetConnection, UserName, Routing);

impl<I: Io> ParseToken<I> for TokenEnvChange {
    fn parse_token(trans: &mut TdsTransport<I>) -> Poll<TdsResponseToken, TdsError> {
        let ty = try!(trans.read_u8());
        let token = match EnvChangeTy::from_u8(ty) {
            Some(EnvChangeTy::Database) => {
                let new_value = try_ready!(trans.read_varchar::<u8>());
                let old_value = try_ready!(trans.read_varchar::<u8>());
                TokenEnvChange::Database(new_value, old_value)
            },
            Some(EnvChangeTy::PacketSize) => {
                let new_value = try_ready!(trans.read_varchar::<u8>());
                let old_value = try_ready!(trans.read_varchar::<u8>());
                let new_size = try!(new_value.as_str().parse::<u32>());
                let old_size = try!(old_value.as_str().parse::<u32>());
                trans.packet_size = new_size as usize;
                TokenEnvChange::PacketSize(new_size, old_size)
            },
            Some(EnvChangeTy::SqlCollation) => {
                let new_value = try_ready!(trans.read_varbyte::<u8>());
                let old_value = try_ready!(trans.read_varbyte::<u8>());
                TokenEnvChange::SqlCollation(new_value, old_value)
            },
            _ => panic!("unimplemented env change ty: {:x}", ty),
        };
        Ok(Async::Ready(TdsResponseToken::EnvChange(token)))
    }
}

#[derive(Debug)]
pub struct TokenInfo {
    /// info number
    number: u32,
    /// error state
    state: u8,
    /// severity (<10: Info)
    class: u8,
    message: TdsBuf,
    server: TdsBuf,
    procedure: TdsBuf,
    line: u32,
}

impl<I: Io> ParseToken<I> for TokenInfo {
    fn parse_token(trans: &mut TdsTransport<I>) -> Poll<TdsResponseToken, TdsError> {
        let token = TokenInfo {
            number: try!(trans.read_u32::<LittleEndian>()),
            state: try!(trans.read_u8()),
            class: try!(trans.read_u8()),
            message: try_ready!(trans.read_varchar::<u16>()),
            server: try_ready!(trans.read_varchar::<u8>()),
            procedure: try_ready!(trans.read_varchar::<u8>()),
            line: try!(trans.read_u32::<LittleEndian>()),
        };
        Ok(Async::Ready(TdsResponseToken::Info(token)))
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

#[derive(Debug)]
pub struct TokenLoginAck {
    /// The type of interface with which the server will accept client requests
    /// 0: SQL_DFLT (server confirms that whatever is sent by the client is acceptable. If the client
    ///    requested SQL_DFLT, SQL_TSQL will be used)
    /// 1: SQL_TSQL (TSQL is accepted)
    interface: u8,
    tds_version: FeatureLevel,
    prog_name: TdsBuf,
    /// major.minor.buildhigh.buildlow
    version: u32,
}

impl<I: Io> ParseToken<I> for TokenLoginAck {
    fn parse_token(trans: &mut TdsTransport<I>) -> Poll<TdsResponseToken, TdsError> {
        let token = TokenLoginAck {
            interface: try!(trans.read_u8()),
            tds_version: try!(FeatureLevel::from_u32(try!(trans.read_u32::<BigEndian>())).ok_or(TdsError::Protocol("loginack: invalid tds version".into()))),
            prog_name: try_ready!(trans.read_varchar::<u8>()),
            version: try!(trans.read_u32::<LittleEndian>()),
        };
        Ok(Async::Ready(TdsResponseToken::LoginAck(token)))
    }
}

#[derive(Debug)]
pub struct TokenDone {
    pub status: u16,
    pub current_cmd: u16,
    pub done_rows: u64,
}

impl<I: Io> ParseToken<I> for TokenDone {
    fn parse_token(trans: &mut TdsTransport<I>) -> Poll<TdsResponseToken, TdsError> {
        let token = TokenDone {
            status: try!(trans.read_u16::<LittleEndian>()),
            current_cmd: try!(trans.read_u16::<LittleEndian>()),
            done_rows: try!(trans.read_u64::<LittleEndian>()),
        };
        Ok(Async::Ready(TdsResponseToken::Done(token)))
    }
}

bitflags! {
    pub flags ColmetaDataFlags: u16 {
        const CDF_NULLABLE            = 1<<0,
        const CDF_CASE_SENSITIVE      = 1<<1,
        const CDF_UPDATEABLE          = 1<<3,
        const CDF_UPDATEABLE_UNKNOWN  = 1<<4,
        const CDF_IDENTITY            = 1<<5,
        const CDF_COMPUTED            = 1<<7,
        // 2 bits reserved for ODBC gateway
        const CDF_FIXED_LEN_CLR_TYPE  = 1<<10,
        const CDF_SPARSE_COLUMN_SET   = 1<<11,
        const CDF_ENCRYPTED           = 1<<12,
        const CDF_HIDDEN              = 1<<13,
        const CDF_KEY                 = 1<<14,
        const CDF_NULLABLE_UNKNOWN    = 1<<15,
    }
}

#[derive(Debug)]
pub struct TokenColMetaData {
    pub columns: Vec<MetaDataColumn>,
}

#[derive(Debug)]
pub struct MetaDataColumn {
    pub flags: ColmetaDataFlags,
    pub ty: TypeInfo,
    pub name: TdsBuf,
}

impl<I: Io> ParseToken<I> for TokenColMetaData {
    fn parse_token(trans: &mut TdsTransport<I>) -> Poll<TdsResponseToken, TdsError> {
        let column_count = try!(trans.read_u16::<LittleEndian>());
        trace!("COLMETADATA columns {}", column_count);

        /*// CekTable (Column Encryption Keys)
        let cek_count = try!(self.read_u16::<LittleEndian>());
        // TODO: Cek/encryption stuff not implemented yet
        assert_eq!(cek_count, 0);*/

        let mut columns = vec![];
        // read all metadata for each column
        for _ in 0..column_count {
            let user_ty = try!(trans.read_u32::<LittleEndian>());
            let flags = ColmetaDataFlags::from_bits(try!(trans.read_u16::<LittleEndian>())).unwrap();

            let raw_ty = try!(trans.read_u8());
            let ty: TypeInfo = match TypeInfo::from_u8(raw_ty) {
                Some(x) => x,
                None => return Err(TdsError::Protocol(format!("invalid or unsupported column type: {:?}", raw_ty).into())),
            };
            // TODO: for type={text, ntext, and image} TABLENAME

            /*// CryptoMetaData
            let cmd_ordinal = try!(self.read_u16::<LittleEndian>());
            let cmd_user_ty = try!(self.read_u32::<LittleEndian>());
            let cmd_ty_info: TypeInfo = try!(self.unserialize(ctx));
            let cmd_encryption_algo = try!(self.read_u8());
            // TODO:
            assert_eq!(cmd_encryption_algo, 0);
            let cmd_algo_name = try!(self.read_varchar::<u8>());
            let cmd_algo_type = try!(self.read_u8());
            let cmd_norm_version = try!(self.read_u8());*/

            let col_name = try_ready!(trans.read_varchar::<u8>());

            columns.push(MetaDataColumn {
                flags: flags,
                ty: ty,
                name: col_name,
            });
        }

        let token = Arc::new(TokenColMetaData {
            columns: columns,
        });
        trace!("COLMETADATA: {:?}", token);
        trans.last_meta = Some(token.clone());
        Ok(Async::Ready(TdsResponseToken::ColMetaData(token)))
    }
}

#[derive(Debug)]
pub struct TokenRow {
    pub meta: Arc<TokenColMetaData>,
    pub columns: Vec<ColumnData>,
}

impl<I: Io> ParseToken<I> for TokenRow {
    fn parse_token(trans: &mut TdsTransport<I>) -> Poll<TdsResponseToken, TdsError> {
        let col_meta = if let Some(ref col_meta) = trans.last_meta {
            col_meta.clone()
        } else {
            return Err(TdsError::Protocol("missing colmeta data".into()));
        };

        let mut columns = vec![];
        for column in &col_meta.columns {
            columns.push(try!(ColumnData::parse(trans, column)));
        }
        let token = TokenRow { meta: col_meta, columns: columns };
        Ok(Async::Ready(TdsResponseToken::Row(token)))
    }
}

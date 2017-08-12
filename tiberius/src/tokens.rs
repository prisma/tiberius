///! token stream definitions
use std::borrow::Cow;
use std::sync::Arc;
use byteorder::{ReadBytesExt, WriteBytesExt, LittleEndian, BigEndian};
use futures::{Async, Poll};
use transport::{Io, ReadState, TdsBuf, TdsTransport, PrimitiveWrites};
use types::{TypeInfo, ColumnData};
use protocol::{self, PacketStatus, PacketType, PacketHeader, PacketWriter};
use {TdsError, TdsResult, FromUint};

/// read a token from an underlying transport
pub trait ParseToken<I: Io> {
    fn parse_token(&mut TdsTransport<I>) -> Poll<TdsResponseToken, TdsError>;
}

pub trait WriteToken<I: Io> {
    fn write_token(&self, &mut TdsTransport<I>) -> TdsResult<()>;
}

#[derive(Copy, Clone, Debug)]
pub enum Tokens {
    ReturnStatus = 0x79,
    ColMetaData = 0x81,
    Error = 0xAA,
    Info = 0xAB,
    Order = 0xA9,
    ReturnValue = 0xAC,
    LoginAck = 0xAD,
    Row = 0xD1,
    NbcRow = 0xD2,
    SSPI = 0xED,
    EnvChange = 0xE3,
    Done = 0xFD,
    DoneProc = 0xFE,
    DoneInProc = 0xFF,
}
uint_to_enum!(Tokens, ReturnStatus, ColMetaData, Error, Info, Order, ReturnValue, LoginAck, Row,
    NbcRow, SSPI, EnvChange, Done, DoneProc, DoneInProc);

#[derive(Debug)]
pub enum TdsResponseToken {
    SSPI(TdsBuf),
    EnvChange(TokenEnvChange),
    Error(TokenError),
    Info(TokenInfo),
    Order(TokenOrder),
    LoginAck(TokenLoginAck),
    Done(TokenDone),
    ColMetaData(Arc<TokenColMetaData>),
    Row(TokenRow),
    DoneProc(TokenDone),
    DoneInProc(TokenDone),
    ReturnStatus(u32),
    ReturnValue(TokenReturnValue),
}

impl<I: Io> TdsTransport<I> {
    #[inline]
    pub fn parse_token(&mut self, token: Tokens, min_len: usize) -> Poll<TdsResponseToken, TdsError> {
        match token {
            Tokens::SSPI => {
                if let Some(bytes) = self.inner.read_bytes(min_len) {
                    return Ok(Async::Ready(TdsResponseToken::SSPI(bytes)))
                }
                Ok(Async::NotReady)
            },
            Tokens::ColMetaData => TokenColMetaData::parse_token(self),
            Tokens::EnvChange => TokenEnvChange::parse_token(self),
            Tokens::Info => TokenInfo::parse_token(self),
            Tokens::Order => TokenOrder::parse_token(self),
            Tokens::LoginAck => TokenLoginAck::parse_token(self),
            Tokens::Done => TokenDone::parse_token(self),
            Tokens::DoneProc => TokenDoneProc::parse_token(self),
            Tokens::DoneInProc => TokenDoneInProc::parse_token(self),
            Tokens::Row => TokenRow::parse_token(self),
            Tokens::NbcRow => TokenNbcRow::parse_token(self),
            Tokens::ReturnStatus => {
                let val = try!(self.inner.read_u32::<LittleEndian>());
                Ok(Async::Ready(TdsResponseToken::ReturnStatus(val)))
            },
            Tokens::ReturnValue => TokenReturnValue::parse_token(self),
            Tokens::Error => TokenError::parse_token(self),
        }
    }
}

#[derive(Debug)]
pub enum TokenEnvChange {
    Database(TdsBuf, TdsBuf),
    PacketSize(u32, u32),
    SqlCollation(TdsBuf, TdsBuf),
    BeginTransaction(u64),
    RollbackTransaction(u64),
    CommitTransaction(u64),
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
        let ty = try!(trans.inner.read_u8());
        let token = match EnvChangeTy::from_u8(ty) {
            Some(EnvChangeTy::Database) => {
                let new_value = try_ready!(trans.inner.read_varchar::<u8>(false));
                let old_value = try_ready!(trans.inner.read_varchar::<u8>(false));
                TokenEnvChange::Database(new_value, old_value)
            },
            Some(EnvChangeTy::PacketSize) => {
                let new_value = try_ready!(trans.inner.read_varchar::<u8>(false));
                let old_value = try_ready!(trans.inner.read_varchar::<u8>(false));
                let new_size = try!(new_value.as_str().parse::<u32>());
                let old_size = try!(old_value.as_str().parse::<u32>());
                TokenEnvChange::PacketSize(new_size, old_size)
            },
            Some(EnvChangeTy::SqlCollation) => {
                let new_value = try_ready!(trans.inner.read_varbyte::<u8>());
                let old_value = try_ready!(trans.inner.read_varbyte::<u8>());
                TokenEnvChange::SqlCollation(new_value, old_value)
            },
            Some(EnvChangeTy::BeginTransaction) => {
                assert_eq!(try!(trans.inner.read_u8()), 8);
                let new_value = try!(trans.inner.read_u64::<LittleEndian>());
                let old_value = try!(trans.inner.read_u8());
                assert_eq!(old_value, 0);
                TokenEnvChange::BeginTransaction(new_value)
            },
            Some(ty @ EnvChangeTy::RollbackTransaction) | Some(ty @ EnvChangeTy::CommitTransaction) => {
                let new_value = try!(trans.inner.read_u8());
                assert_eq!(new_value, 0);
                assert_eq!(try!(trans.inner.read_u8()), 8);
                let old_value = try!(trans.inner.read_u64::<LittleEndian>());
                if let EnvChangeTy::RollbackTransaction = ty {
                    TokenEnvChange::RollbackTransaction(old_value)
                } else {
                    TokenEnvChange::CommitTransaction(old_value)
                }
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
            number: try!(trans.inner.read_u32::<LittleEndian>()),
            state: try!(trans.inner.read_u8()),
            class: try!(trans.inner.read_u8()),
            message: try_ready!(trans.inner.read_varchar::<u16>(false)),
            server: try_ready!(trans.inner.read_varchar::<u8>(false)),
            procedure: try_ready!(trans.inner.read_varchar::<u8>(false)),
            line: try!(trans.inner.read_u32::<LittleEndian>()),
        };
        Ok(Async::Ready(TdsResponseToken::Info(token)))
    }
}

/// Contains the indexes of the columns by which the data is sorted
#[derive(Debug)]
pub struct TokenOrder(Vec<u16>);

impl<I: Io> ParseToken<I> for TokenOrder {
    fn parse_token(trans: &mut TdsTransport<I>) -> Poll<TdsResponseToken, TdsError> {
        let len = try!(trans.inner.read_u16::<LittleEndian>())/2;
        let mut cols = Vec::with_capacity(len as usize);
        for _ in 0..len {
            cols.push(try!(trans.inner.read_u16::<LittleEndian>()));
        }
        Ok(Async::Ready(TdsResponseToken::Order(TokenOrder(cols))))
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
            interface: try!(trans.inner.read_u8()),
            tds_version: try!(FeatureLevel::from_u32(try!(trans.inner.read_u32::<BigEndian>())).ok_or(TdsError::Protocol("loginack: invalid tds version".into()))),
            prog_name: try_ready!(trans.inner.read_varchar::<u8>(false)),
            version: try!(trans.inner.read_u32::<LittleEndian>()),
        };
        Ok(Async::Ready(TdsResponseToken::LoginAck(token)))
    }
}

bitflags! {
    pub struct DoneStatus: u16 {
        const DONE_MORE = 0x1;
        const DONE_ERROR = 0x2;
        const DONE_INEXACT = 0x4;
        const DONE_COUNT = 0x10;
        const DONE_ATTENTION = 0x20;
        const DONE_RPC_IN_BATCH  = 0x80;
        const DONE_SRVERROR = 0x100;
    }
}

#[derive(Debug)]
pub struct TokenDone {
    pub status: DoneStatus,
    pub cur_cmd: u16,
    pub done_rows: u64,
}

impl<I: Io> ParseToken<I> for TokenDone {
    fn parse_token(trans: &mut TdsTransport<I>) -> Poll<TdsResponseToken, TdsError> {
        let token = TokenDone {
            status: try!(DoneStatus::from_bits(try!(trans.inner.read_u16::<LittleEndian>())).ok_or(TdsError::Protocol("done(variant): invalid status".into()))),
            cur_cmd: try!(trans.inner.read_u16::<LittleEndian>()),
            done_rows: try!(trans.inner.read_u64::<LittleEndian>()),
        };
        Ok(Async::Ready(TdsResponseToken::Done(token)))
    }
}

bitflags! {
    pub struct ColmetaDataFlags: u16 {
        const CDF_NULLABLE            = 1<<0;
        const CDF_CASE_SENSITIVE      = 1<<1;
        const CDF_UPDATEABLE          = 1<<3;
        const CDF_UPDATEABLE_UNKNOWN  = 1<<4;
        const CDF_IDENTITY            = 1<<5;
        const CDF_COMPUTED            = 1<<7;
        // 2 bits reserved for ODBC gateway
        const CDF_FIXED_LEN_CLR_TYPE  = 1<<10;
        const CDF_SPARSE_COLUMN_SET   = 1<<11;
        const CDF_ENCRYPTED           = 1<<12;
        const CDF_HIDDEN              = 1<<13;
        const CDF_KEY                 = 1<<14;
        const CDF_NULLABLE_UNKNOWN    = 1<<15;
    }
}

#[derive(Debug)]
pub struct TokenColMetaData {
    pub columns: Vec<MetaDataColumn>,
}

#[derive(Debug)]
pub struct BaseMetaDataColumn {
    pub flags: ColmetaDataFlags,
    pub ty: TypeInfo,
}

#[derive(Debug)]
pub struct MetaDataColumn {
    pub base: BaseMetaDataColumn,
    pub col_name: TdsBuf,
}

impl BaseMetaDataColumn {
    fn parse<I: Io>(trans: &mut TdsTransport<I>) -> Poll<BaseMetaDataColumn, TdsError> {
        let user_ty = try!(trans.inner.read_u32::<LittleEndian>());

        let raw_flags = try!(trans.inner.read_u16::<LittleEndian>());
        let flags = ColmetaDataFlags::from_bits(raw_flags).unwrap();

        let ty = try_ready!(TypeInfo::parse(trans));
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

        let meta = BaseMetaDataColumn {
            flags: flags,
            ty: ty,
        };
        Ok(Async::Ready(meta))
    }
}

impl MetaDataColumn {
    fn parse<I: Io>(trans: &mut TdsTransport<I>) -> Poll<MetaDataColumn, TdsError> {
        let meta = MetaDataColumn {
            base: try_ready!(BaseMetaDataColumn::parse(trans)),
            col_name: try_ready!(trans.inner.read_varchar::<u8>(false)),
        };
        Ok(Async::Ready(meta))
    }
}

impl<I: Io> ParseToken<I> for TokenColMetaData {
    fn parse_token(trans: &mut TdsTransport<I>) -> Poll<TdsResponseToken, TdsError> {
        let column_count = try!(trans.inner.read_u16::<LittleEndian>());

        let mut columns = vec![];
        if column_count > 0 && column_count < 0xffff {
            /*// CekTable (Column Encryption Keys)
            let cek_count = try!(self.read_u16::<LittleEndian>());
            // TODO: Cek/encryption stuff not implemented yet
            assert_eq!(cek_count, 0);*/

            // read all metadata for each column
            for _ in 0..column_count {
                columns.push(try_ready!(MetaDataColumn::parse(trans)));
            }
        }

        let token = Arc::new(TokenColMetaData {
            columns: columns,
        });
        if !token.columns.is_empty() {
            trans.inner.last_meta = Some(token.clone());
        }
        Ok(Async::Ready(TdsResponseToken::ColMetaData(token)))
    }
}

#[derive(Debug)]
pub struct TokenRow {
    pub meta: Arc<TokenColMetaData>,
    pub columns: Vec<ColumnData<'static>>,
}

impl<I: Io> ParseToken<I> for TokenRow {
    #[inline]
    fn parse_token(trans: &mut TdsTransport<I>) -> Poll<TdsResponseToken, TdsError> {
        let col_meta = if let Some(ref col_meta) = trans.inner.last_meta {
            col_meta.clone()
        } else {
            return Err(TdsError::Protocol("missing colmeta data".into()));
        };

        // extract the state for the first Type/ColumnData parse call
        let (row_tok, mut column_data) = match trans.read_state.take() {
            Some(ReadState::Row(token, column_data, state)) => {
                if let Some(state) = state.map(ReadState::Type) {
                    trans.read_state = Some(state);
                }
                (token, column_data)
            },
            _ => unreachable!()
        };

        for (i, column) in col_meta.columns.iter().enumerate().skip(column_data.len()) {
            let index = i / 8;
            let bit = i % 8;

            // only needed for NBCRow, If the column is null (specified by the bitmap), there's nothing to read
            if trans.inner.row_bitmap.as_ref().map(|bm| bm.as_ref()[index] & (1 << bit)).unwrap_or(0) > 0 {
                column_data.push(ColumnData::None);
                trans.commit_read_state(None);
                continue;
            }

            match ColumnData::parse(trans, &column.base) {
                Ok(Async::Ready(coldata)) => {
                    column_data.push(coldata);
                    // we don't have a state for the next column yet
                    trans.commit_read_state(None);
                },
                ret => {
                    // reset the read state back to the last state value
                    let col_state = match trans.read_state.take() {
                        Some(ReadState::Type(tystate)) => Some(tystate),
                        _ => None,
                    };
                    trans.read_state = Some(ReadState::Row(row_tok, column_data, col_state));
                    // this closure cannot be executed here since Async::Ready is handled above, only used for type conversion
                    return ret.map(|async| async.map(|_| ((None as Option<TdsResponseToken>).unwrap())));
                }
            }
        }
        let token = TokenRow { meta: col_meta, columns: column_data };
        Ok(Async::Ready(TdsResponseToken::Row(token)))
    }
}

#[derive(Debug)]
pub struct TokenNbcRow;

impl<I: Io> ParseToken<I> for TokenNbcRow {
    fn parse_token(trans: &mut TdsTransport<I>) -> Poll<TdsResponseToken, TdsError> {
        if trans.inner.row_bitmap.is_none() {
            let col_meta = if let Some(ref col_meta) = trans.inner.last_meta {
                col_meta.clone()
            } else {
                return Err(TdsError::Protocol("missing colmeta data".into()));
            };

            // calculate size of the null-bitmap and read it
            let bitmap_size = (col_meta.columns.len() + 8 - 1) / 8;
            println!("{} columns => {} bits", col_meta.columns.len(), bitmap_size);
            trans.inner.row_bitmap = trans.inner.read_bytes(bitmap_size);
        }

        if trans.inner.row_bitmap.is_some() {
            let ret = try_ready!(TokenRow::parse_token(trans));
            trans.inner.row_bitmap = None;
            return Ok(Async::Ready(ret));
        }
        Ok(Async::NotReady)
    }
}

#[derive(Debug)]
pub struct TokenDoneInProc;

impl<I: Io> ParseToken<I> for TokenDoneInProc {
    fn parse_token(trans: &mut TdsTransport<I>) -> Poll<TdsResponseToken, TdsError> {
        let token = match try_ready!(TokenDone::parse_token(trans)) {
            TdsResponseToken::Done(x) => x,
            _ => unreachable!()
        };
        Ok(Async::Ready(TdsResponseToken::DoneInProc(token)))
    }
}

#[derive(Debug)]
pub struct TokenDoneProc;

impl<I: Io> ParseToken<I> for TokenDoneProc {
    fn parse_token(trans: &mut TdsTransport<I>) -> Poll<TdsResponseToken, TdsError> {
        let token = match try_ready!(TokenDone::parse_token(trans)) {
            TdsResponseToken::Done(x) => x,
            _ => unreachable!()
        };
        Ok(Async::Ready(TdsResponseToken::DoneProc(token)))
    }
}

#[derive(Debug)]
pub struct TokenError {
    /// ErrorCode
    code: u32,
    /// ErrorState (describing code)
    state: u8,
    /// The class (severity) of the error
    class: u8,
    /// The error message
    message: TdsBuf,
    server: TdsBuf,
    procedure: TdsBuf,
    line: u32,
}

impl<I: Io> ParseToken<I> for TokenError {
    fn parse_token(trans: &mut TdsTransport<I>) -> Poll<TdsResponseToken, TdsError> {
        let token = TokenError {
            code: try!(trans.inner.read_u32::<LittleEndian>()),
            state: try!(trans.inner.read_u8()),
            class: try!(trans.inner.read_u8()),
            message: try_ready!(trans.inner.read_varchar::<u16>(false)),
            server: try_ready!(trans.inner.read_varchar::<u8>(false)),
            procedure: try_ready!(trans.inner.read_varchar::<u8>(false)),
            line: try!(trans.inner.read_u32::<LittleEndian>()),
        };
        Ok(Async::Ready(TdsResponseToken::Error(token)))
    }
}

#[derive(Debug)]
pub struct TokenReturnValue {
    pub param_ordinal: u16,
    pub param_name: TdsBuf,
    /// return value of user defined function
    pub udf: bool,
    pub meta: BaseMetaDataColumn,
    pub value: ColumnData<'static>,
}

impl<I: Io> ParseToken<I> for TokenReturnValue {
    fn parse_token(trans: &mut TdsTransport<I>) -> Poll<TdsResponseToken, TdsError> {
        let param_ordinal = try!(trans.inner.read_u16::<LittleEndian>());
        let param_name = try_ready!(trans.inner.read_varchar::<u8>(false));
        let udf = match try!(trans.inner.read_u8()) {
            0x01 => false,
            0x02 => true,
            _ => return Err(TdsError::Protocol("ReturnValue: invalid status".into())),
        };
        let meta = try_ready!(BaseMetaDataColumn::parse(trans));
        let token = TokenReturnValue {
            param_ordinal: param_ordinal,
            param_name: param_name,
            udf: udf,
            value: try_ready!(ColumnData::parse(trans, &meta)),
            meta: meta,
        };
        Ok(Async::Ready(TdsResponseToken::ReturnValue(token)))
    }
}

/// 2.2.6.6 RPC Request
#[allow(dead_code)]
#[repr(u8)]
#[derive(Clone, Copy)]
pub enum RpcProcId {
    SpPrepare = 11,
    SpExecute = 12,
    SpPrepExec = 13,
    SpUnprepare = 15,
}

pub enum RpcProcIdValue<'a> {
    Name(Cow<'a, str>),
    Id(RpcProcId)
}

bitflags! {
    pub struct RpcStatusFlags: u8 {
        const RPC_PARAM_BY_REF_VALUE    = 0x01;
        const RPC_PARAM_DEFAULT_VALUE   = 0x02;
        // <- reserved
        const RPC_PARAM_ENCRYPTED       = 0x08;
        // <- 4 bits reserved
    }
}

bitflags! {
    pub struct RpcOptionFlags: u16 {
        const RPC_WITH_RECOMP   = 0x01;
        const RPC_NO_META       = 0x02;
        const RPC_REUSE_META    = 0x04;
        // <- 13 reserved bits
    }
}

pub struct RpcParam<'a> {
    pub name: Cow<'a, str>,
    pub flags: RpcStatusFlags,
    pub value: ColumnData<'a>,
}

pub struct TokenRpcRequest<'a> {
     pub proc_id: RpcProcIdValue<'a>,
     pub flags: RpcOptionFlags,
     pub params: Vec<RpcParam<'a>>,
}

impl<'a, I: Io> WriteToken<I> for TokenRpcRequest<'a> {
    fn write_token(&self, trans: &mut TdsTransport<I>) -> TdsResult<()> {
        // build the general header for the packet
        let header = PacketHeader {
            ty: PacketType::RPC,
            status: PacketStatus::NormalMessage,
            ..PacketHeader::new(0, 0)
        };
        let mut writer = PacketWriter::new(&mut trans.inner, header);

          try!(protocol::write_trans_descriptor(&mut writer, trans.transaction));
        match self.proc_id {
            RpcProcIdValue::Id(ref id) => {
                try!(writer.write_u16::<LittleEndian>(0xffff));
                try!(writer.write_u16::<LittleEndian>(*id as u16));
            },
            RpcProcIdValue::Name(ref name) => {
                //let (left_bytes, _) = try!(write_varchar::<u16>(&mut cursor, name, 0));
                //assert_eq!(left_bytes, 0);
                unimplemented!()
            }
        }
        try!(writer.write_u16::<LittleEndian>(self.flags.bits()));

        for (_, param) in self.params.iter().enumerate() {
            // name
            try!(writer.write_varchar::<u8>(&param.name));

            // status flag
            try!(writer.write_u8(param.flags.bits));
            // recalculate the position for the value (offset)
            try!(param.value.serialize(&mut writer));
        }

        // we're officially done with this token stream, flush a last time
        try!(writer.finalize());

        Ok(())
    }
}

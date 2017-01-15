///! token stream definitions
use std::borrow::Cow;
use std::io::Cursor;
use std::sync::Arc;
use byteorder::{ReadBytesExt, WriteBytesExt, LittleEndian, BigEndian};
use futures::{Async, Poll, Sink};
use tokio_core::io::Io;
use transport::{self, TdsBuf, TdsTransport, TokenWriteState, write_varchar};
use types::{TypeInfo, ColumnData};
use protocol::{self, AllHeaderTy, PacketStatus, PacketType, PacketHeader};
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
    Info = 0xAB,
    ReturnValue = 0xAC,
    LoginAck = 0xAD,
    Row = 0xD1,
    SSPI = 0xED,
    EnvChange = 0xE3,
    Done = 0xFD,
    DoneProc = 0xFE,
    DoneInProc = 0xFF,
}
uint_to_enum!(Tokens, ReturnStatus, ColMetaData, Info, ReturnValue, LoginAck, Row, SSPI, EnvChange, Done, DoneProc, DoneInProc);

#[derive(Debug)]
pub enum TdsResponseToken {
    SSPI(TdsBuf),
    EnvChange(TokenEnvChange),
    Info(TokenInfo),
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
    pub fn parse_token(&mut self, token: Tokens, min_len: usize) -> Poll<TdsResponseToken, TdsError> {
        match token {
            Tokens::SSPI => {
                if let Some(bytes) = self.read_bytes(min_len) {
                    return Ok(Async::Ready(TdsResponseToken::SSPI(bytes)))
                }
                Ok(Async::NotReady)
            },
            Tokens::ColMetaData => TokenColMetaData::parse_token(self),
            Tokens::EnvChange => TokenEnvChange::parse_token(self),
            Tokens::Info => TokenInfo::parse_token(self),
            Tokens::LoginAck => TokenLoginAck::parse_token(self),
            Tokens::Done => TokenDone::parse_token(self),
            Tokens::DoneProc => TokenDoneProc::parse_token(self),
            Tokens::DoneInProc => TokenDoneInProc::parse_token(self),
            Tokens::Row => TokenRow::parse_token(self),
            Tokens::ReturnStatus => {
                let val = try!(self.read_u32::<LittleEndian>());
                Ok(Async::Ready(TdsResponseToken::ReturnStatus(val)))
            },
            Tokens::ReturnValue => TokenReturnValue::parse_token(self),
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
                let new_value = try_ready!(trans.read_varchar::<u8>(false));
                let old_value = try_ready!(trans.read_varchar::<u8>(false));
                TokenEnvChange::Database(new_value, old_value)
            },
            Some(EnvChangeTy::PacketSize) => {
                let new_value = try_ready!(trans.read_varchar::<u8>(false));
                let old_value = try_ready!(trans.read_varchar::<u8>(false));
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
            message: try_ready!(trans.read_varchar::<u16>(false)),
            server: try_ready!(trans.read_varchar::<u8>(false)),
            procedure: try_ready!(trans.read_varchar::<u8>(false)),
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
            prog_name: try_ready!(trans.read_varchar::<u8>(false)),
            version: try!(trans.read_u32::<LittleEndian>()),
        };
        Ok(Async::Ready(TdsResponseToken::LoginAck(token)))
    }
}

bitflags! {
    pub flags DoneStatus: u16 {
        const DONE_MORE = 0x1,
        const DONE_ERROR = 0x2,
        const DONE_INEXACT = 0x4,
        const DONE_COUNT = 0x10,
        const DONE_ATTENTION = 0x20,
        const DONE_RPC_IN_BATCH  = 0x80,
        const DONE_SRVERROR = 0x100,
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
            status: try!(DoneStatus::from_bits(try!(trans.read_u16::<LittleEndian>())).ok_or(TdsError::Protocol("done(variant): invalid status".into()))),
            cur_cmd: try!(trans.read_u16::<LittleEndian>()),
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
        let user_ty = try!(trans.read_u32::<LittleEndian>());

        let raw_flags = try!(trans.read_u16::<LittleEndian>());
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
            col_name: try_ready!(trans.read_varchar::<u8>(false)),
        };
        Ok(Async::Ready(meta))
    }
}

impl<I: Io> ParseToken<I> for TokenColMetaData {
    fn parse_token(trans: &mut TdsTransport<I>) -> Poll<TdsResponseToken, TdsError> {
        let column_count = try!(trans.read_u16::<LittleEndian>());

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
            trans.last_meta = Some(token.clone());
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
    fn parse_token(trans: &mut TdsTransport<I>) -> Poll<TdsResponseToken, TdsError> {
        let col_meta = if let Some(ref col_meta) = trans.last_meta {
            col_meta.clone()
        } else {
            return Err(TdsError::Protocol("missing colmeta data".into()));
        };

        let mut columns = vec![];
        for column in &col_meta.columns {
            columns.push(try_ready!(ColumnData::parse(trans, &column.base)));
        }
        let token = TokenRow { meta: col_meta, columns: columns };
        Ok(Async::Ready(TdsResponseToken::Row(token)))
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
        let param_ordinal = try!(trans.read_u16::<LittleEndian>());
        let param_name = try_ready!(trans.read_varchar::<u8>(false));
        let udf = match try!(trans.read_u8()) {
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
    pub flags RpcStatusFlags: u8 {
        const RPC_PARAM_BY_REF_VALUE    = 0x01,
        const RPC_PARAM_DEFAULT_VALUE   = 0x02,
        // <- reserved
        const RPC_PARAM_ENCRYPTED       = 0x08,
        // <- 4 bits reserved
    }
}

bitflags! {
    pub flags RpcOptionFlags: u16 {
        const RPC_WITH_RECOMP   = 0x01,
        const RPC_NO_META       = 0x02,
        const RPC_REUSE_META    = 0x04,
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
        fn allocate_cursor(capacity: usize) -> Cursor<Vec<u8>> {
            let mut cursor = Cursor::new({
                let mut vec = Vec::with_capacity(capacity);
                vec.resize(protocol::HEADER_BYTES, 0);
                vec
            });
            cursor.set_position(protocol::HEADER_BYTES as u64);
            cursor
        };

        // allocate initial cursor
        let mut cursor = allocate_cursor(trans.packet_size);

        // write the start of the token stream, only once
        if trans.write_state.is_none() {
            // TODO: move this out (ALL_HEADERS)
            {
                try!(cursor.write_u32::<LittleEndian>(protocol::ALL_HEADERS_LEN_TX as u32));
                try!(cursor.write_u32::<LittleEndian>(protocol::ALL_HEADERS_LEN_TX as u32 - 4));
                try!(cursor.write_u16::<LittleEndian>(AllHeaderTy::TransactionDescriptor as u16));
                // transaction descriptor
                try!(cursor.write_u64::<LittleEndian>(0));
                // outstanding requests (TransactionDescrHeader)
                try!(cursor.write_u32::<LittleEndian>(1));
            }
            match self.proc_id {
                RpcProcIdValue::Id(ref id) => {
                    try!(cursor.write_u16::<LittleEndian>(0xffff));
                    try!(cursor.write_u16::<LittleEndian>(*id as u16));
                },
                RpcProcIdValue::Name(ref name) => {
                    let (left_bytes, _) = try!(write_varchar::<u16>(&mut cursor, name, 0));
                    assert_eq!(left_bytes, 0);
                }
            }
            try!(cursor.write_u16::<LittleEndian>(self.flags.bits()));
        }

        loop {
            // send if possible, but do not let it delay us, make sure to buffer
            let _ = try!(trans.poll_complete());

            // check if we already attempted to complete the current write request
            let (param_idx, last_pos) = match trans.write_state {
                Some(TokenWriteState::RpcRequest { param_idx, last_pos }) => (param_idx, last_pos),
                None => (0, 0),
                _ => unreachable!()
            };

            let mut missing = self.params.len() - param_idx;

            // this should be triggered on flushing after we're done (see "finally FLUSHING" below)
            if missing == 0 {
                break;
            }

            for (i, param) in self.params.iter().enumerate().skip(param_idx) {
                let mut last_pos = if param_idx == i { last_pos } else { 0 };
                let remaining = trans.packet_size - cursor.get_ref().len();
                // the current packet is exhausted, queue it (NormalMessage) and start another one
                if remaining == 0 {
                    break;
                }

                // name
                if last_pos == 0 {
                    try!(cursor.write_u8(param.name.len() as u8));
                    last_pos = 1;
                }
                if last_pos < 1 + 2*param.name.len() {
                    let (left_bytes, written_bytes) = try!(transport::write_varchar_fragment(&mut cursor, &param.name, last_pos - 1));
                    if left_bytes > 0 {
                        trans.write_state = Some(TokenWriteState::RpcRequest { param_idx: i, last_pos: last_pos + written_bytes });
                        break;
                    }
                    last_pos += written_bytes;
                }

                // status flag
                if last_pos == 1 + 2*param.name.len() {
                    if trans.packet_size - cursor.get_ref().len() < 1 {
                        trans.write_state = Some(TokenWriteState::RpcRequest { param_idx: i, last_pos: last_pos });
                        break;
                    }
                    try!(cursor.write_u8(param.flags.bits));
                    last_pos += 1;
                }
                // recalculate the position for the value (offset)
                let fixed_state = last_pos - 2*1 - 2*param.name.len();
                let ret = try!(param.value.serialize(&mut cursor, fixed_state));
                if let Some(state) = ret {
                    // cache the new state, packet is exhausted
                    assert_eq!(trans.packet_size - cursor.get_ref().len(), 0);
                    let fixed_state = (state - fixed_state) + last_pos;
                    trans.write_state = Some(TokenWriteState::RpcRequest { param_idx: i, last_pos: fixed_state });
                    break;
                }
                missing -= 1;
            }

            // we've filled a packet, queue it
            let mut buf = cursor.into_inner();
            let status = if missing == 0 {
                PacketStatus::EndOfMessage
            } else {
                PacketStatus::NormalMessage
            };
            // build the header for the packet
            let header = PacketHeader {
                ty: PacketType::RPC,
                status: status,
                ..PacketHeader::new(buf.len(), trans.next_id())
            };
            try!(header.serialize(&mut buf));

            try!(trans.queue_vec(buf));

            // we've sent the final packet, no reason to continue
            if missing == 0 {
                break;
            }

            cursor = allocate_cursor(trans.packet_size);
        }

        // we're officially done with this token stream, flush a last time
        trans.write_state = None;
        let _ = try!(trans.poll_complete());

        Ok(())
    }
}

use std::convert::TryFrom;
use std::io::Cursor;
use std::sync::Arc;

use bitflags::bitflags;
use byteorder::{BigEndian, LittleEndian, ReadBytesExt};
use tokio::io::{AsyncRead, AsyncReadExt};

use crate::protocol;
use crate::protocol::types::ColumnData;
use crate::{Error, Result};

fn read_varchar(b: &[u8]) -> Result<String> {
    let mut buf = vec![0u16; b.len() / 2];
    for i in 0..buf.len() {
        buf[i] = (&b[2 * i..]).read_u16::<LittleEndian>()?;
    }

    let varch = String::from_utf16(&buf[..])?;
    Ok(varch)
}

trait ReadSlice {
    fn read_slice(&mut self, n: usize) -> &[u8];
}

impl<T: AsRef<[u8]>> ReadSlice for Cursor<T> {
    fn read_slice(&mut self, length: usize) -> &[u8] {
        let pos = self.position() as usize;
        self.set_position((pos + length) as u64);
        &self.get_ref().as_ref()[pos..pos + length]
    }
}

uint_enum! {
    pub enum TokenType {
        ReturnStatus = 0x79,
        ColMetaData = 0x81,
        Error = 0xAA,
        Info = 0xAB,
        Order = 0xA9,
        ColInfo = 0xA5,
        ReturnValue = 0xAC,
        LoginAck = 0xAD,
        Row = 0xD1,
        NbcRow = 0xD2,
        SSPI = 0xED,
        EnvChange = 0xE3,
        Done = 0xFD,
        /// stored procedure completed
        DoneProc = 0xFE,
        /// sql within stored procedure completed
        DoneInProc = 0xFF,
    }
}

#[derive(Debug)]
pub enum TokenEnvChange {
    Database(String, String),
    PacketSize(u32, u32),
    SqlCollation(Vec<u8>, Vec<u8>),
    BeginTransaction(u64),
    RollbackTransaction(u64),
    CommitTransaction(u64),
}

uint_enum! {
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
}

#[derive(Debug)]
pub struct TokenInfo {
    /// info number
    number: u32,
    /// error state
    state: u8,
    /// severity (<10: Info)
    class: u8,
    message: String,
    server: String,
    procedure: String,
    line: u32,
}

#[derive(Debug)]
pub struct TokenLoginAck {
    /// The type of interface with which the server will accept client requests
    /// 0: SQL_DFLT (server confirms that whatever is sent by the client is acceptable. If the client
    ///    requested SQL_DFLT, SQL_TSQL will be used)
    /// 1: SQL_TSQL (TSQL is accepted)
    interface: u8,
    tds_version: protocol::FeatureLevel,
    prog_name: String,
    /// major.minor.buildhigh.buildlow
    version: u32,
}

bitflags! {
    pub struct DoneStatus: u16 {
        const MORE = 0x1;
        const ERROR = 0x2;
        const INEXACT = 0x4;
        const COUNT = 0x10;
        const ATTENTION = 0x20;
        const RPC_IN_BATCH  = 0x80;
        const SRVERROR = 0x100;
    }
}

#[derive(Debug)]
pub struct TokenDone {
    pub status: DoneStatus,
    pub cur_cmd: u16,
    pub done_rows: u64,
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
    pub ty: protocol::types::TypeInfo,
}

#[derive(Debug)]
pub struct MetaDataColumn {
    pub base: BaseMetaDataColumn,
    pub col_name: String,
}

#[derive(Clone, Debug)]
pub struct TokenError {
    /// ErrorCode
    pub code: u32,
    /// ErrorState (describing code)
    state: u8,
    /// The class (severity) of the error
    class: u8,
    /// The error message
    message: String,
    server: String,
    procedure: String,
    line: u32,
}

#[derive(Debug)]
pub struct TokenRow {
    // pub meta: Arc<TokenColMetaData>,
    pub columns: Vec<ColumnData>,
}

#[derive(Debug)]
pub struct TokenReturnValue {
    pub param_ordinal: u16,
    pub param_name: String,
    /// return value of user defined function
    pub udf: bool,
    pub meta: BaseMetaDataColumn,
    pub value: ColumnData,
}

#[derive(Debug)]
pub struct TokenOrder {
    column_indexes: Vec<u16>,
}

pub struct TokenStreamReader<'a, C: AsyncRead> {
    pub reader: super::PacketReader<'a, C>,
}

impl<'a, C: AsyncRead + Unpin> TokenStreamReader<'a, C> {
    pub fn new(reader: super::PacketReader<'a, C>) -> Self {
        TokenStreamReader { reader }
    }

    pub async fn read_token(&mut self) -> Result<TokenType> {
        let ty_byte = self.reader.read_bytes(1).await?[0];
        let ty = TokenType::try_from(ty_byte)
            .map_err(|_| Error::Protocol(format!("invalid token type {:x}", ty_byte).into()))?;
        Ok(ty)
    }

    pub async fn read_sspi_token(&mut self) -> Result<&[u8]> {
        let len = self
            .reader
            .read_bytes(2)
            .await?
            .read_u16::<LittleEndian>()? as usize;
        let bytes = self.reader.read_bytes(len).await?;
        Ok(bytes)
    }

    pub async fn read_env_change_token(&mut self) -> Result<TokenEnvChange> {
        let stream_len = self
            .reader
            .read_bytes(2)
            .await?
            .read_u16::<LittleEndian>()? as usize;

        let ty_byte = self.reader.read_bytes(1).await?[0];
        let ty = EnvChangeTy::try_from(ty_byte)
            .map_err(|_| Error::Protocol(format!("invalid envchange type {:x}", ty_byte).into()))?;
        let mut env_value_data = Cursor::new(self.reader.read_bytes(stream_len - 1).await?);

        let token = match ty {
            EnvChangeTy::Database => {
                let new_value_len = env_value_data.read_u8()? as usize;
                let new_value = read_varchar(env_value_data.read_slice(2 * new_value_len))?;
                let old_value_len = env_value_data.read_u8()? as usize;
                let old_value = read_varchar(env_value_data.read_slice(2 * old_value_len))?;
                TokenEnvChange::Database(new_value, old_value)
            }
            EnvChangeTy::PacketSize => {
                let new_value_len = env_value_data.read_u8()? as usize;
                let new_value = read_varchar(env_value_data.read_slice(2 * new_value_len))?;
                let old_value_len = env_value_data.read_u8()? as usize;
                let old_value = read_varchar(env_value_data.read_slice(2 * old_value_len))?;
                let new_size = new_value.as_str().parse::<u32>()?;
                let old_size = old_value.as_str().parse::<u32>()?;
                TokenEnvChange::PacketSize(new_size, old_size)
            }
            EnvChangeTy::SqlCollation => {
                let new_value_len = env_value_data.read_u8()? as usize;
                let new_value = env_value_data.read_slice(new_value_len).to_vec();
                let old_value_len = env_value_data.read_u8()? as usize;
                let old_value = env_value_data.read_slice(old_value_len).to_vec();
                TokenEnvChange::SqlCollation(new_value, old_value)
            }
            EnvChangeTy::BeginTransaction => {
                /*assert_eq!(trans.inner.read_u8()?, 8);
                let new_value = trans.inner.read_u64::<LittleEndian>()?;
                let old_value = trans.inner.read_u8()?;
                assert_eq!(old_value, 0);
                TokenEnvChange::BeginTransaction(new_value)*/
                unimplemented!()
            }
            ty @ EnvChangeTy::RollbackTransaction | ty @ EnvChangeTy::CommitTransaction => {
                /*let new_value = trans.inner.read_u8()?;
                assert_eq!(new_value, 0);
                assert_eq!(trans.inner.read_u8()?, 8);
                let old_value = trans.inner.read_u64::<LittleEndian>()?;
                if let EnvChangeTy::RollbackTransaction = ty {
                    TokenEnvChange::RollbackTransaction(old_value)
                } else {
                    TokenEnvChange::CommitTransaction(old_value)
                }*/
                unimplemented!()
            }
            ty => panic!("skipping env change type {:?}", ty),
        };
        if env_value_data.position() as usize != env_value_data.get_ref().len() {
            return Err(Error::Protocol("incomplete envchange token read".into()));
        }
        Ok(token)
    }

    pub async fn read_info_token(&mut self) -> Result<TokenInfo> {
        let length = self
            .reader
            .read_bytes(2)
            .await?
            .read_u16::<LittleEndian>()? as usize;
        let mut content = Cursor::new(self.reader.read_bytes(length).await?);

        let token = TokenInfo {
            number: content.read_u32::<LittleEndian>()?,
            state: content.read_u8()?,
            class: content.read_u8()?,
            message: {
                let len = content.read_u16::<LittleEndian>()? as usize;
                read_varchar(content.read_slice(2 * len))?
            },
            server: {
                let len = content.read_u8()? as usize;
                read_varchar(content.read_slice(2 * len))?
            },
            procedure: {
                let len = content.read_u8()? as usize;
                read_varchar(content.read_slice(2 * len))?
            },
            line: content.read_u32::<LittleEndian>()?,
        };
        if content.position() as usize != content.get_ref().len() {
            return Err(Error::Protocol("incomplete info token read".into()));
        }
        Ok(token)
    }

    pub async fn read_login_ack_token(&mut self) -> Result<TokenLoginAck> {
        let length = self
            .reader
            .read_bytes(2)
            .await?
            .read_u16::<LittleEndian>()? as usize;
        let mut content = Cursor::new(self.reader.read_bytes(length).await?);

        let token = TokenLoginAck {
            interface: content.read_u8()?,
            tds_version: protocol::FeatureLevel::try_from(content.read_u32::<BigEndian>()?)
                .map_err(|_| Error::Protocol("loginack: invalid tds version".into()))?,
            prog_name: {
                let len = content.read_u8()? as usize;
                read_varchar(content.read_slice(2 * len))?
            },
            version: content.read_u32::<LittleEndian>()?,
        };
        if content.position() as usize != content.get_ref().len() {
            return Err(Error::Protocol("incomplete login ack read".into()));
        }
        Ok(token)
    }

    pub async fn read_done_token(&mut self, ctx: &protocol::Context) -> Result<TokenDone> {
        let done_row_count_bytes =
            if ctx.version as u8 >= protocol::FeatureLevel::SqlServer2005 as u8 {
                8
            } else {
                4
            };

        let mut content = Cursor::new(self.reader.read_bytes(4 + done_row_count_bytes).await?);

        let token = TokenDone {
            status: DoneStatus::from_bits(content.read_u16::<LittleEndian>()?)
                .ok_or(Error::Protocol("done(variant): invalid status".into()))?,
            cur_cmd: content.read_u16::<LittleEndian>()?,
            done_rows: match done_row_count_bytes {
                8 => content.read_u64::<LittleEndian>()?,
                4 => content.read_u32::<LittleEndian>()? as u64,
                _ => unreachable!(),
            },
        };
        Ok(token)
    }

    pub async fn read_basemetadata_column(
        &mut self,
        ctx: &protocol::Context,
    ) -> Result<BaseMetaDataColumn> {
        let _user_ty = self
            .reader
            .read_bytes(4)
            .await?
            .read_u32::<LittleEndian>()?;

        let raw_flags = self
            .reader
            .read_bytes(2)
            .await?
            .read_u16::<LittleEndian>()?;
        let flags = ColmetaDataFlags::from_bits(raw_flags).unwrap();

        let ty = self.reader.read_type_info(ctx).await?;
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
        Ok(meta)
    }

    pub async fn read_colmetadata_token(
        &mut self,
        ctx: &protocol::Context,
    ) -> Result<TokenColMetaData> {
        let column_count = self
            .reader
            .read_bytes(2)
            .await?
            .read_u16::<LittleEndian>()?;

        let mut columns = vec![];
        if column_count > 0 && column_count < 0xffff {
            /*// CekTable (Column Encryption Keys)
            let cek_count = try!(self.read_u16::<LittleEndian>());
            // TODO: Cek/encryption stuff not implemented yet
            assert_eq!(cek_count, 0);*/

            // read all metadata for each column
            for _ in 0..column_count {
                let meta = MetaDataColumn {
                    base: self.read_basemetadata_column(ctx).await?,
                    col_name: {
                        let len = self.reader.read_bytes(1).await?[0] as usize;
                        read_varchar(self.reader.read_bytes(2 * len).await?)?
                    },
                };
                columns.push(meta);
            }
        }

        let meta = TokenColMetaData { columns };
        Ok(meta)
    }

    pub async fn read_colinfo_token(&mut self, ctx: &protocol::Context) -> Result<()> {
        let mut byte_length = self
            .reader
            .read_bytes(2)
            .await?
            .read_u16::<LittleEndian>()?;
        /*let mut col_names = vec![];
        while byte_length > 0 {
            let col_num = self.reader.read_bytes(1).await?[0];
            let table_num = self.reader.read_bytes(1).await?[0];
            let status = self.reader.read_bytes(1).await?[0];
        }*/
        self.reader.read_bytes(byte_length as usize).await?;
        Ok(())
    }

    pub async fn read_row_token(&mut self, ctx: &protocol::Context) -> Result<TokenRow> {
        let col_meta = ctx
            .last_meta
            .lock()
            .unwrap()
            .clone()
            .ok_or(Error::Protocol("missing colmeta data".into()))?;

        let mut row = TokenRow {
            columns: Vec::with_capacity(col_meta.columns.len()),
        };
        for (i, column) in col_meta.columns.iter().enumerate() {
            row.columns
                .push(self.reader.read_column_data(ctx, &column.base).await?);
        }
        Ok(row)
    }

    pub async fn read_return_status_token(&mut self, ctx: &protocol::Context) -> Result<u32> {
        let status = self
            .reader
            .read_bytes(4)
            .await?
            .read_u32::<LittleEndian>()?;
        Ok(status)
    }

    pub async fn read_return_value_token(
        &mut self,
        ctx: &protocol::Context,
    ) -> Result<TokenReturnValue> {
        let param_ordinal = self
            .reader
            .read_bytes(2)
            .await?
            .read_u16::<LittleEndian>()?;
        let param_name_len = self.reader.read_bytes(1).await?[0] as usize;
        let param_name = read_varchar(self.reader.read_bytes(2 * param_name_len).await?)?;
        let udf = match self.reader.read_bytes(1).await?[0] {
            0x01 => false,
            0x02 => true,
            _ => return Err(Error::Protocol("ReturnValue: invalid status".into())),
        };
        let meta = self.read_basemetadata_column(&ctx).await?;
        let token = TokenReturnValue {
            param_ordinal,
            param_name,
            udf,
            value: self.reader.read_column_data(&ctx, &meta).await?,
            meta,
        };
        Ok(token)
    }

    pub async fn read_error_token(&mut self, ctx: &protocol::Context) -> Result<TokenError> {
        let length = self
            .reader
            .read_bytes(2)
            .await?
            .read_u16::<LittleEndian>()? as usize;
        let mut content = Cursor::new(self.reader.read_bytes(length).await?);

        let code = content.read_u32::<LittleEndian>()?;
        let state = content.read_u8()?;
        let class = content.read_u8()?;
        let message_len = content.read_u16::<LittleEndian>()? as usize;
        let message = read_varchar(content.read_slice(2 * message_len))?;
        let server_len = content.read_u8()? as usize;
        let server = read_varchar(content.read_slice(2 * server_len))?;
        let procedure_len = content.read_u8()? as usize;
        let procedure = read_varchar(content.read_slice(2 * procedure_len))?;
        let line = if ctx.version > protocol::FeatureLevel::SqlServer2005 {
            content.read_u32::<LittleEndian>()?
        } else {
            content.read_u16::<LittleEndian>()? as u32
        };

        let token = TokenError {
            code,
            state,
            class,
            message,
            server,
            procedure,
            line,
        };
        if content.position() as usize != content.get_ref().len() {
            debug_assert_eq!(content.position() as usize, content.get_ref().len());
            return Err(Error::Protocol("incomplete error token read".into()));
        }
        Ok(token)
    }

    pub async fn read_order_token(&mut self, ctx: &protocol::Context) -> Result<TokenOrder> {
        let len = self
            .reader
            .read_bytes(2)
            .await?
            .read_u16::<LittleEndian>()?
            / 2;

        let mut column_indexes = Vec::with_capacity(len as usize);
        for _ in 0..len {
            column_indexes.push(
                self.reader
                    .read_bytes(2)
                    .await?
                    .read_u16::<LittleEndian>()?,
            );
        }
        Ok(TokenOrder { column_indexes })
    }
}

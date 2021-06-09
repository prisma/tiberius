use super::{Encode, FixedLenType, TypeInfo, VarLenType};
use crate::tds::{Collation, DateTime, SmallDateTime};
#[cfg(feature = "tds73")]
use crate::tds::{Date, DateTime2, DateTimeOffset, Time};
use crate::{
    tds::{
        codec::guid,
        xml::{XmlData, XmlSchema},
        Numeric,
    },
    Error, SqlReadBytes,
};
use byteorder::{ByteOrder, LittleEndian};
use bytes::{BufMut, BytesMut};
use encoding::DecoderTrap;
use futures::io::AsyncReadExt;
use std::borrow::BorrowMut;
use std::{borrow::Cow, sync::Arc};
use uuid::Uuid;

const MAX_NVARCHAR_SIZE: usize = 1 << 30;

#[derive(Clone, Debug)]
/// A container of a value that can be represented as a TDS value.
pub enum ColumnData<'a> {
    /// 8-bit integer, unsigned.
    U8(Option<u8>),
    /// 16-bit integer, signed.
    I16(Option<i16>),
    /// 32-bit integer, signed.
    I32(Option<i32>),
    /// 64-bit integer, signed.
    I64(Option<i64>),
    /// 32-bit floating point number.
    F32(Option<f32>),
    /// 64-bit floating point number.
    F64(Option<f64>),
    /// Boolean.
    Bit(Option<bool>),
    /// A string value.
    String(Option<Cow<'a, str>>),
    /// A Guid (UUID) value.
    Guid(Option<Uuid>),
    /// Binary data.
    Binary(Option<Cow<'a, [u8]>>),
    /// Numeric value (a decimal).
    Numeric(Option<Numeric>),
    /// XML data.
    Xml(Option<Cow<'a, XmlData>>),
    /// DateTime value.
    DateTime(Option<DateTime>),
    /// A small DateTime value.
    SmallDateTime(Option<SmallDateTime>),
    #[cfg(feature = "tds73")]
    #[cfg_attr(feature = "docs", doc(cfg(feature = "tds73")))]
    /// Time value.
    Time(Option<Time>),
    #[cfg(feature = "tds73")]
    #[cfg_attr(feature = "docs", doc(cfg(feature = "tds73")))]
    /// Date value.
    Date(Option<Date>),
    #[cfg(feature = "tds73")]
    #[cfg_attr(feature = "docs", doc(cfg(feature = "tds73")))]
    /// DateTime2 value.
    DateTime2(Option<DateTime2>),
    #[cfg(feature = "tds73")]
    #[cfg_attr(feature = "docs", doc(cfg(feature = "tds73")))]
    /// DateTime2 value with an offset.
    DateTimeOffset(Option<DateTimeOffset>),
}

impl<'a> ColumnData<'a> {
    pub(crate) fn type_name(&self) -> Cow<'static, str> {
        match self {
            ColumnData::U8(_) => "tinyint".into(),
            ColumnData::I16(_) => "smallint".into(),
            ColumnData::I32(_) => "int".into(),
            ColumnData::I64(_) => "bigint".into(),
            ColumnData::F32(_) => "float(24)".into(),
            ColumnData::F64(_) => "float(53)".into(),
            ColumnData::Bit(_) => "bit".into(),
            ColumnData::String(None) => "nvarchar(4000)".into(),
            ColumnData::String(Some(ref s)) if s.len() <= 4000 => "nvarchar(4000)".into(),
            ColumnData::String(Some(ref s)) if s.len() <= MAX_NVARCHAR_SIZE => {
                "nvarchar(max)".into()
            }
            ColumnData::String(_) => "ntext(max)".into(),
            ColumnData::Guid(_) => "uniqueidentifier".into(),
            ColumnData::Binary(Some(ref b)) if b.len() <= 8000 => "varbinary(8000)".into(),
            ColumnData::Binary(_) => "varbinary(max)".into(),
            ColumnData::Numeric(Some(ref n)) => {
                format!("numeric({},{})", n.precision(), n.scale()).into()
            }
            ColumnData::Numeric(None) => "numeric".into(),
            ColumnData::Xml(_) => "xml".into(),
            ColumnData::DateTime(_) => "datetime".into(),
            ColumnData::SmallDateTime(_) => "smalldatetime".into(),
            #[cfg(feature = "tds73")]
            ColumnData::Time(_) => "time".into(),
            #[cfg(feature = "tds73")]
            ColumnData::Date(_) => "date".into(),
            #[cfg(feature = "tds73")]
            ColumnData::DateTime2(_) => "datetime2".into(),
            #[cfg(feature = "tds73")]
            ColumnData::DateTimeOffset(_) => "datetimeoffset".into(),
        }
    }
}

/// Mode for type reader.
#[derive(Debug, Clone, Copy)]
pub enum ReadTyMode {
    /// Fixed-size type with given size
    FixedSize(usize),
    /// Partially length-prefixed type
    Plp,
}

impl ReadTyMode {
    /// Determine the mode automatically from size
    pub fn auto(size: usize) -> Self {
        if size < 0xffff {
            ReadTyMode::FixedSize(size)
        } else {
            ReadTyMode::Plp
        }
    }
}

/// A partially read type
#[derive(Debug)]
pub struct ReadTyState {
    pub(crate) mode: ReadTyMode,
    pub(crate) data: Option<Vec<u8>>,
    pub(crate) chunk_data_left: usize,
}

impl ReadTyState {
    /// Initialize a type reader
    pub fn new(mode: ReadTyMode) -> Self {
        ReadTyState {
            mode,
            data: None,
            chunk_data_left: 0,
        }
    }
}

#[derive(Clone, Debug, Copy)]
pub struct VariableLengthContext {
    ty: VarLenType,
    len: usize,
    collation: Option<Collation>,
}

#[derive(Clone, Debug, Copy)]
pub struct VariableLengthPrecisionContext {
    pub scale: u8,
}

impl VariableLengthContext {
    pub fn new(ty: VarLenType, len: usize, collation: Option<Collation>) -> Self {
        Self { ty, len, collation }
    }
}

impl<'a> ColumnData<'a> {
    pub(crate) async fn decode<R>(src: &mut R, ctx: &TypeInfo) -> crate::Result<ColumnData<'a>>
    where
        R: SqlReadBytes + Unpin,
    {
        let res = match ctx {
            TypeInfo::FixedLen(fixed_ty) => Self::decode_fixed_len(src, &fixed_ty).await?,
            TypeInfo::VarLenSized(ty, max_len, collation) => {
                let context = VariableLengthContext::new(*ty, *max_len, *collation);
                Self::decode_var_len(src, &context).await?
            }
            TypeInfo::VarLenSizedPrecision { ty, scale, .. } => match ty {
                VarLenType::Decimaln | VarLenType::Numericn => {
                    ColumnData::Numeric(Numeric::decode(src, *scale).await?)
                }
                _ => todo!(),
            },
            TypeInfo::Xml { schema, size } => Self::decode_xml(src, *size, schema.clone()).await?,
        };

        Ok(res)
    }

    async fn decode_fixed_len<R>(src: &mut R, ty: &FixedLenType) -> crate::Result<ColumnData<'a>>
    where
        R: SqlReadBytes + Unpin,
    {
        let ret = match ty {
            FixedLenType::Null => ColumnData::Bit(None),
            FixedLenType::Bit => ColumnData::Bit(Some(src.read_u8().await? != 0)),
            FixedLenType::Int1 => ColumnData::U8(Some(src.read_u8().await?)),
            FixedLenType::Int2 => ColumnData::I16(Some(src.read_i16_le().await?)),
            FixedLenType::Int4 => ColumnData::I32(Some(src.read_i32_le().await?)),
            FixedLenType::Int8 => ColumnData::I64(Some(src.read_i64_le().await?)),
            FixedLenType::Float4 => ColumnData::F32(Some(src.read_f32_le().await?)),
            FixedLenType::Float8 => ColumnData::F64(Some(src.read_f64_le().await?)),
            FixedLenType::Datetime => Self::decode_datetimen(src, 8).await?,
            FixedLenType::Datetime4 => Self::decode_datetimen(src, 4).await?,
            FixedLenType::Money4 => Self::decode_money(src, 4).await?,
            FixedLenType::Money => Self::decode_money(src, 8).await?,
        };

        Ok(ret)
    }

    async fn decode_var_len<R>(
        src: &mut R,
        ctx: &VariableLengthContext,
    ) -> crate::Result<ColumnData<'a>>
    where
        R: SqlReadBytes + Unpin,
    {
        let ty = ctx.ty;
        let len = ctx.len;
        let collation = ctx.collation;

        let res = match ty {
            VarLenType::Bitn => Self::decode_bit(src).await?,
            VarLenType::Intn => Self::decode_int(src).await?,
            // 2.2.5.5.1.5 IEEE754
            VarLenType::Floatn => Self::decode_float(src).await?,
            VarLenType::Guid => Self::decode_guid(src).await?,
            VarLenType::BigChar | VarLenType::NChar | VarLenType::NVarchar => {
                let decoded = Self::decode_variable_string(src, ty, len, collation)
                    .await?
                    .map(Cow::from);

                ColumnData::String(decoded)
            }
            VarLenType::BigVarChar => Self::decode_big_varchar(src, len, collation).await?,

            VarLenType::Money => {
                let len = src.read_u8().await?;
                Self::decode_money(src, len).await?
            }

            VarLenType::Datetimen => {
                let len = src.read_u8().await?;
                Self::decode_datetimen(src, len).await?
            }

            #[cfg(feature = "tds73")]
            VarLenType::Daten => Self::decode_date(src).await?,

            #[cfg(feature = "tds73")]
            VarLenType::Timen => {
                let rlen = src.read_u8().await?;

                match rlen {
                    0 => ColumnData::Time(None),
                    _ => {
                        let time = Time::decode(src, len as usize, rlen as usize).await?;
                        ColumnData::Time(Some(time))
                    }
                }
            }

            #[cfg(feature = "tds73")]
            VarLenType::Datetime2 => {
                let rlen = src.read_u8().await?;
                match rlen {
                    0 => ColumnData::DateTime2(None),
                    rlen => {
                        let dt = DateTime2::decode(src, len as usize, rlen as usize - 3).await?;
                        ColumnData::DateTime2(Some(dt))
                    }
                }
            }

            #[cfg(feature = "tds73")]
            VarLenType::DatetimeOffsetn => {
                let rlen = src.read_u8().await?;

                match rlen {
                    0 => ColumnData::DateTimeOffset(None),
                    _ => {
                        let dto = DateTimeOffset::decode(src, len, rlen - 5).await?;
                        ColumnData::DateTimeOffset(Some(dto))
                    }
                }
            }

            VarLenType::BigBinary | VarLenType::BigVarBin => Self::decode_binary(src, len).await?,
            VarLenType::Text => Self::decode_text(src, collation).await?,
            VarLenType::NText => Self::decode_ntext(src).await?,
            VarLenType::Image => Self::decode_image(src).await?,
            t => unimplemented!("{:?}", t),
        };

        Ok(res)
    }

    async fn decode_xml<R>(
        src: &mut R,
        len: usize,
        schema: Option<Arc<XmlSchema>>,
    ) -> crate::Result<ColumnData<'a>>
    where
        R: SqlReadBytes + Unpin,
    {
        let xml = Self::decode_variable_string(src, VarLenType::Xml, len, None)
            .await?
            .map(|data| {
                let mut data = XmlData::new(data);

                if let Some(schema) = schema {
                    data.set_schema(schema);
                }

                Cow::Owned(data)
            });

        Ok(ColumnData::Xml(xml))
    }

    #[cfg(feature = "tds73")]
    async fn decode_date<R>(src: &mut R) -> crate::Result<ColumnData<'static>>
    where
        R: SqlReadBytes + Unpin,
    {
        let len = src.read_u8().await?;

        let res = match len {
            0 => ColumnData::Date(None),
            3 => ColumnData::Date(Some(Date::decode(src).await?)),
            _ => {
                return Err(Error::Protocol(
                    format!("daten: length of {} is invalid", len).into(),
                ))
            }
        };

        Ok(res)
    }

    async fn decode_datetimen<R>(src: &mut R, len: u8) -> crate::Result<ColumnData<'static>>
    where
        R: SqlReadBytes + Unpin,
    {
        let datetime = match len {
            0 => ColumnData::SmallDateTime(None),
            4 => ColumnData::SmallDateTime(Some(SmallDateTime::decode(src).await?)),
            8 => ColumnData::DateTime(Some(DateTime::decode(src).await?)),
            _ => {
                return Err(Error::Protocol(
                    format!("datetimen: length of {} is invalid", len).into(),
                ))
            }
        };

        Ok(datetime)
    }

    async fn decode_bit<R>(src: &mut R) -> crate::Result<ColumnData<'static>>
    where
        R: SqlReadBytes + Unpin,
    {
        let recv_len = src.read_u8().await? as usize;

        let res = match recv_len {
            0 => ColumnData::Bit(None),
            1 => ColumnData::Bit(Some(src.read_u8().await? > 0)),
            v => {
                return Err(Error::Protocol(
                    format!("bitn: length of {} is invalid", v).into(),
                ))
            }
        };

        Ok(res)
    }

    async fn decode_int<R>(src: &mut R) -> crate::Result<ColumnData<'static>>
    where
        R: SqlReadBytes + Unpin,
    {
        let recv_len = src.read_u8().await? as usize;

        let res = match recv_len {
            0 => ColumnData::U8(None),
            1 => ColumnData::U8(Some(src.read_u8().await?)),
            2 => ColumnData::I16(Some(src.read_i16_le().await?)),
            4 => ColumnData::I32(Some(src.read_i32_le().await?)),
            8 => ColumnData::I64(Some(src.read_i64_le().await?)),
            _ => unimplemented!(),
        };

        Ok(res)
    }

    async fn decode_float<R>(src: &mut R) -> crate::Result<ColumnData<'static>>
    where
        R: SqlReadBytes + Unpin,
    {
        let len = src.read_u8().await? as usize;

        let res = match len {
            0 => ColumnData::F32(None),
            4 => ColumnData::F32(Some(src.read_f32_le().await?)),
            8 => ColumnData::F64(Some(src.read_f64_le().await?)),
            _ => {
                return Err(Error::Protocol(
                    format!("floatn: length of {} is invalid", len).into(),
                ))
            }
        };

        Ok(res)
    }

    async fn decode_guid<R>(src: &mut R) -> crate::Result<ColumnData<'static>>
    where
        R: SqlReadBytes + Unpin,
    {
        let len = src.read_u8().await? as usize;

        let res = match len {
            0 => ColumnData::Guid(None),
            16 => {
                let mut data = [0u8; 16];

                for item in &mut data {
                    *item = src.read_u8().await?;
                }

                guid::reorder_bytes(&mut data);
                ColumnData::Guid(Some(Uuid::from_bytes(data)))
            }
            _ => {
                return Err(Error::Protocol(
                    format!("guid: length of {} is invalid", len).into(),
                ))
            }
        };

        Ok(res)
    }

    async fn decode_text<R>(
        src: &mut R,
        collation: Option<Collation>,
    ) -> crate::Result<ColumnData<'static>>
    where
        R: SqlReadBytes + Unpin,
    {
        let ptr_len = src.read_u8().await? as usize;

        if ptr_len == 0 {
            Ok(ColumnData::String(None))
        } else {
            let _ = src.read_exact(&mut vec![0; ptr_len][0..ptr_len]).await?; // text ptr

            src.read_i32_le().await?; // days
            src.read_u32_le().await?; // second fractions

            let text_len = src.read_u32_le().await? as usize;
            let mut buf = vec![0; text_len];

            src.read_exact(&mut buf[0..text_len]).await?;

            let collation = collation.as_ref().unwrap();
            let encoder = collation.encoding()?;

            let text: String = encoder
                .decode(buf.as_ref(), DecoderTrap::Strict)
                .map_err(Error::Encoding)?;

            Ok(ColumnData::String(Some(text.into())))
        }
    }

    async fn decode_ntext<R>(src: &mut R) -> crate::Result<ColumnData<'static>>
    where
        R: SqlReadBytes + Unpin,
    {
        let ptr_len = src.read_u8().await? as usize;

        if ptr_len == 0 {
            Ok(ColumnData::String(None))
        } else {
            let _ = src.read_exact(&mut vec![0; ptr_len][0..ptr_len]).await?; // text ptr

            src.read_i32_le().await?; // days
            src.read_u32_le().await?; // second fractions

            let len = src.read_u32_le().await? as usize / 2;
            let mut buf = vec![0u16; len];

            for item in buf.iter_mut().take(len) {
                *item = src.read_u16_le().await?;
            }

            let text = String::from_utf16(&buf[..])?;

            Ok(ColumnData::String(Some(text.into())))
        }
    }

    async fn decode_image<R>(src: &mut R) -> crate::Result<ColumnData<'static>>
    where
        R: SqlReadBytes + Unpin,
    {
        let ptr_len = src.read_u8().await? as usize;

        if ptr_len == 0 {
            Ok(ColumnData::Binary(None))
        } else {
            let _ = src.read_exact(&mut vec![0; ptr_len][0..ptr_len]).await?; // text ptr

            src.read_i32_le().await?; // days
            src.read_u32_le().await?; // second fractions

            let len = src.read_u32_le().await? as usize;
            let mut buf = vec![0; len];
            src.read_exact(&mut buf[0..len]).await?;

            Ok(ColumnData::Binary(Some(buf.into())))
        }
    }

    async fn decode_variable_string<R>(
        src: &mut R,
        ty: VarLenType,
        len: usize,
        collation: Option<Collation>,
    ) -> crate::Result<Option<String>>
    where
        R: SqlReadBytes + Unpin,
    {
        let mode = if ty == VarLenType::NChar || ty == VarLenType::BigChar {
            ReadTyMode::FixedSize(len)
        } else {
            ReadTyMode::auto(len)
        };

        let data = Self::decode_plp_type(src, mode).await?;

        let res = if let Some(buf) = data {
            if ty == VarLenType::BigChar {
                let collation = collation.as_ref().unwrap();
                let encoder = collation.encoding()?;

                let s: String = encoder
                    .decode(buf.as_ref(), DecoderTrap::Strict)
                    .map_err(Error::Encoding)?;

                Some(s)
            } else {
                if buf.len() % 2 != 0 {
                    return Err(Error::Protocol("nvarchar: invalid plp length".into()));
                }

                let buf: Vec<_> = buf.chunks(2).map(LittleEndian::read_u16).collect();
                Some(String::from_utf16(&buf)?)
            }
        } else {
            None
        };

        Ok(res)
    }

    async fn decode_big_varchar<R>(
        src: &mut R,
        len: usize,
        collation: Option<Collation>,
    ) -> crate::Result<ColumnData<'static>>
    where
        R: SqlReadBytes + Unpin,
    {
        let mode = ReadTyMode::auto(len);
        let data = Self::decode_plp_type(src, mode).await?;

        let res = if let Some(bytes) = data {
            let collation = collation.as_ref().unwrap();
            let encoder = collation.encoding()?;

            let s: String = encoder
                .decode(bytes.as_ref(), DecoderTrap::Strict)
                .map_err(Error::Encoding)?;

            ColumnData::String(Some(s.into()))
        } else {
            ColumnData::String(None)
        };

        Ok(res)
    }

    async fn decode_money<R>(src: &mut R, len: u8) -> crate::Result<ColumnData<'static>>
    where
        R: SqlReadBytes + Unpin,
    {
        let res = match len {
            0 => ColumnData::F64(None),
            4 => ColumnData::F64(Some(src.read_i32_le().await? as f64 / 1e4)),
            8 => ColumnData::F64(Some({
                let high = src.read_i32_le().await? as i64;
                let low = src.read_u32_le().await? as f64;
                ((high << 32) as f64 + low) / 1e4
            })),
            _ => {
                return Err(Error::Protocol(
                    format!("money: length of {} is invalid", len).into(),
                ))
            }
        };

        Ok(res)
    }

    async fn decode_binary<R>(src: &mut R, len: usize) -> crate::Result<ColumnData<'static>>
    where
        R: SqlReadBytes + Unpin,
    {
        let mode = ReadTyMode::auto(len);
        let data = Self::decode_plp_type(src, mode).await?;

        let res = if let Some(buf) = data {
            ColumnData::Binary(Some(buf.into()))
        } else {
            ColumnData::Binary(None)
        };

        Ok(res)
    }

    pub(crate) async fn decode_plp_type<R>(
        src: &mut R,
        mode: ReadTyMode,
    ) -> crate::Result<Option<Vec<u8>>>
    where
        R: SqlReadBytes + Unpin,
    {
        let mut read_state = ReadTyState::new(mode);

        // If we did not read anything yet, initialize the reader.
        if read_state.data.is_none() {
            let size = match read_state.mode {
                ReadTyMode::FixedSize(_) => src.read_u16_le().await? as u64,
                ReadTyMode::Plp => src.read_u64_le().await?,
            };

            read_state.data = match (size, read_state.mode) {
                (0xffff, ReadTyMode::FixedSize(_)) => None, // NULL value
                (0xffffffffffffffff, ReadTyMode::Plp) => None, // NULL value
                (0xfffffffffffffffe, ReadTyMode::Plp) => Some(Vec::new()), // unknown size
                (len, _) => Some(Vec::with_capacity(len as usize)), // given size
            };

            // If this is not PLP, treat everything as a single chunk.
            if let ReadTyMode::FixedSize(_) = read_state.mode {
                read_state.chunk_data_left = size as usize;
            }
        }

        // If there is a buffer, we have something to read.
        if let Some(ref mut buf) = read_state.data {
            loop {
                if read_state.chunk_data_left == 0 {
                    // We have no chunk. Start a new one.
                    let chunk_size = match read_state.mode {
                        ReadTyMode::FixedSize(_) => 0,
                        ReadTyMode::Plp => src.read_u32_le().await? as usize,
                    };

                    if chunk_size == 0 {
                        break; // found a sentinel, we're done
                    } else {
                        read_state.chunk_data_left = chunk_size
                    }
                } else {
                    // Just read a byte
                    let byte = src.read_u8().await?;
                    read_state.chunk_data_left -= 1;

                    buf.push(byte);
                }
            }
        }

        Ok(read_state.data.take())
    }
}

impl<'a> Encode<BytesMut> for ColumnData<'a> {
    fn encode(self, dst: &mut BytesMut) -> crate::Result<()> {
        match self {
            ColumnData::Bit(Some(val)) => {
                let header = [&[VarLenType::Bitn as u8, 1, 1][..]].concat();

                dst.extend_from_slice(&header);
                dst.put_u8(val as u8);
            }
            ColumnData::U8(Some(val)) => {
                let header = [&[VarLenType::Intn as u8, 1, 1][..]].concat();

                dst.extend_from_slice(&header);
                dst.put_u8(val);
            }
            ColumnData::I16(Some(val)) => {
                let header = [&[VarLenType::Intn as u8, 2, 2][..]].concat();

                dst.extend_from_slice(&header);
                dst.put_i16_le(val);
            }
            ColumnData::I32(Some(val)) => {
                let header = [&[VarLenType::Intn as u8, 4, 4][..]].concat();

                dst.extend_from_slice(&header);
                dst.put_i32_le(val);
            }
            ColumnData::I64(Some(val)) => {
                let header = [&[VarLenType::Intn as u8, 8, 8][..]].concat();

                dst.extend_from_slice(&header);
                dst.put_i64_le(val);
            }
            ColumnData::F32(Some(val)) => {
                let header = [&[VarLenType::Floatn as u8, 4, 4][..]].concat();

                dst.extend_from_slice(&header);
                dst.put_f32_le(val);
            }
            ColumnData::F64(Some(val)) => {
                let header = [&[VarLenType::Floatn as u8, 8, 8][..]].concat();

                dst.extend_from_slice(&header);
                dst.put_f64_le(val);
            }
            ColumnData::Guid(Some(uuid)) => {
                let header = [&[VarLenType::Guid as u8, 16, 16][..]].concat();

                dst.extend_from_slice(&header);
                let mut data = *uuid.as_bytes();
                guid::reorder_bytes(&mut data);
                dst.extend_from_slice(&data);
            }
            ColumnData::String(Some(ref s)) if s.len() <= 4000 => {
                dst.put_u8(VarLenType::NVarchar as u8);
                dst.put_u16_le(8000);
                dst.extend_from_slice(&[0u8; 5][..]);

                let mut length = 0u16;
                let len_pos = dst.len();

                dst.put_u16_le(length);

                for chr in s.encode_utf16() {
                    length += 1;
                    dst.put_u16_le(chr);
                }

                let dst: &mut [u8] = dst.borrow_mut();
                let bytes = (length * 2).to_le_bytes(); // u16, two bytes

                for (i, byte) in bytes.iter().enumerate() {
                    dst[len_pos + i] = *byte;
                }
            }
            ColumnData::String(Some(ref s)) => {
                // length: 0xffff and raw collation
                dst.put_u8(VarLenType::NVarchar as u8);
                dst.extend_from_slice(&[0xff_u8; 2][..]);
                dst.extend_from_slice(&[0u8; 5][..]);

                // we cannot cheaply predetermine the length of the UCS2 string beforehand
                // (2 * bytes(UTF8) is not always right) - so just let the SQL server handle it
                dst.put_u64_le(0xfffffffffffffffe_u64);

                // Write the varchar length
                let mut length = 0u32;
                let len_pos = dst.len();

                dst.put_u32_le(length);

                for chr in s.encode_utf16() {
                    length += 1;
                    dst.put_u16_le(chr);
                }

                // PLP_TERMINATOR
                dst.put_u32_le(0);

                let dst: &mut [u8] = dst.borrow_mut();
                let bytes = (length * 2).to_le_bytes(); // u32, four bytes

                for (i, byte) in bytes.iter().enumerate() {
                    dst[len_pos + i] = *byte;
                }
            }
            ColumnData::Binary(Some(bytes)) if bytes.len() <= 8000 => {
                dst.put_u8(VarLenType::BigVarBin as u8);
                dst.put_u16_le(8000);
                dst.put_u16_le(bytes.len() as u16);
                dst.extend(bytes.into_owned());
            }
            ColumnData::Binary(Some(bytes)) => {
                dst.put_u8(VarLenType::BigVarBin as u8);
                // Max length
                dst.put_u16_le(0xffff_u16);
                // Also the length is unknown
                dst.put_u64_le(0xfffffffffffffffe_u64);
                // We'll write in one chunk, length is the whole bytes length
                dst.put_u32_le(bytes.len() as u32);
                // Payload
                dst.extend(bytes.into_owned());
                // PLP_TERMINATOR
                dst.put_u32_le(0);
            }
            ColumnData::DateTime(Some(dt)) => {
                dst.extend_from_slice(&[VarLenType::Datetimen as u8, 8, 8]);
                dt.encode(dst)?;
            }
            ColumnData::SmallDateTime(Some(dt)) => {
                dst.extend_from_slice(&[VarLenType::Datetimen as u8, 4, 4]);
                dt.encode(dst)?;
            }
            #[cfg(feature = "tds73")]
            ColumnData::Time(Some(time)) => {
                dst.extend_from_slice(&[VarLenType::Timen as u8, time.scale(), time.len()?]);

                time.encode(dst)?;
            }
            #[cfg(feature = "tds73")]
            ColumnData::Date(Some(date)) => {
                dst.extend_from_slice(&[VarLenType::Daten as u8, 3]);
                date.encode(dst)?;
            }
            #[cfg(feature = "tds73")]
            ColumnData::DateTime2(Some(dt)) => {
                let len = dt.time().len()? + 3;

                dst.extend_from_slice(&[VarLenType::Datetime2 as u8, dt.time().scale(), len]);

                dt.encode(dst)?;
            }
            #[cfg(feature = "tds73")]
            ColumnData::DateTimeOffset(Some(dto)) => {
                dst.extend_from_slice(&[
                    VarLenType::DatetimeOffsetn as u8,
                    dto.datetime2().time().scale(),
                    dto.datetime2().time().len()? + 5,
                ]);

                dto.encode(dst)?;
            }
            ColumnData::Xml(Some(xml)) => {
                dst.put_u8(VarLenType::Xml as u8);
                xml.into_owned().encode(dst)?;
            }
            ColumnData::Numeric(Some(num)) => {
                dst.extend_from_slice(&[
                    VarLenType::Numericn as u8,
                    num.len(),
                    num.precision(),
                    num.scale(),
                ]);
                num.encode(dst)?;
            }
            _ => {
                // None/null
                dst.put_u8(FixedLenType::Null as u8);
            }
        }

        Ok(())
    }
}

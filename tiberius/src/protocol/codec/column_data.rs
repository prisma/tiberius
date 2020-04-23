use super::{read_varchar, Encode, FixedLenType, TypeInfo, VarLenType};
use crate::{
    async_read_le_ext::AsyncReadLeExt,
    protocol::{self, types::Numeric},
    Error,
};
use byteorder::{ByteOrder, LittleEndian};
use bytes::{BufMut, BytesMut};
use encoding::DecoderTrap;
use protocol::types::Collation;
use std::borrow::Cow;
use tokio::io::AsyncReadExt;
use uuid::Uuid;

#[derive(Clone, Debug)]
pub enum ColumnData<'a> {
    None,
    I8(i8),
    I16(i16),
    I32(i32),
    I64(i64),
    F32(f32),
    F64(f64),
    Bit(bool),
    String(Cow<'a, str>),
    Guid(Uuid),
    Binary(Cow<'a, [u8]>),
    Numeric(Numeric),
    /*
    DateTime(time::DateTime),
    SmallDateTime(time::SmallDateTime),
    Time(time::Time),
    Date(time::Date),
    DateTime2(time::DateTime2),
    /// a buffer string which is a reference to a buffer of a received packet
    BString(Str),
     */
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
        R: AsyncReadLeExt + Unpin,
    {
        let res = match ctx {
            TypeInfo::FixedLen(fixed_ty) => Self::decode_fixed_len(src, &fixed_ty).await?,
            TypeInfo::VarLenSized(ty, max_len, collation) => {
                let context = VariableLengthContext::new(*ty, *max_len, *collation);
                Self::decode_var_len(src, &context).await?
            }
            TypeInfo::VarLenSizedPrecision { ty, scale, .. } => match ty {
                VarLenType::Decimaln | VarLenType::Numericn => {
                    Self::decode_var_len_precision(src, *scale).await?
                }
                _ => todo!(),
            },
        };

        Ok(res)
    }

    async fn decode_fixed_len<R>(src: &mut R, ty: &FixedLenType) -> crate::Result<ColumnData<'a>>
    where
        R: AsyncReadLeExt + Unpin,
    {
        let ret = match ty {
            FixedLenType::Null => ColumnData::None,
            FixedLenType::Bit => ColumnData::Bit(src.read_u8().await? != 0),
            FixedLenType::Int1 => ColumnData::I8(src.read_i8().await?),
            FixedLenType::Int2 => ColumnData::I16(src.read_i16_le().await?),
            FixedLenType::Int4 => ColumnData::I32(src.read_i32_le().await?),
            FixedLenType::Int8 => ColumnData::I64(src.read_i64_le().await?),
            FixedLenType::Float4 => ColumnData::F32(src.read_f32_le().await?),
            FixedLenType::Float8 => ColumnData::F64(src.read_f64_le().await?),
            // FixedLenType::Datetime => parse_datetimen(trans, 8)?,
            // FixedLenType::Datetime4 => parse_datetimen(trans, 4)?,
            _ => {
                return Err(Error::Protocol(
                    format!("unsupported fixed type decoding: {:?}", ty).into(),
                ))
            }
        };

        Ok(ret)
    }

    async fn decode_var_len_precision<R>(src: &mut R, scale: u8) -> crate::Result<ColumnData<'a>>
    where
        R: AsyncReadLeExt + Unpin,
    {
        fn decode_d128(buf: &[u8]) -> u128 {
            let low_part = LittleEndian::read_u64(&buf[0..]) as u128;

            if !buf[8..].iter().any(|x| *x != 0) {
                return low_part;
            }

            let high_part = match buf.len() {
                12 => LittleEndian::read_u32(&buf[8..]) as u128,
                16 => LittleEndian::read_u64(&buf[8..]) as u128,
                _ => unreachable!(),
            };

            // swap high&low for big endian
            #[cfg(target_endian = "big")]
            let (low_part, high_part) = (high_part, low_part);

            let high_part = high_part * (u64::max_value() as u128 + 1);
            low_part + high_part
        }

        let len = src.read_u8().await?;

        if len == 0 {
            Ok(ColumnData::None)
        } else {
            let sign = match src.read_u8().await? {
                0 => -1i128,
                1 => 1i128,
                _ => return Err(Error::Protocol("decimal: invalid sign".into())),
            };

            let value = match len {
                5 => src.read_u32_le().await? as i128 * sign,
                9 => src.read_u64_le().await? as i128 * sign,
                13 => {
                    let mut bytes = [0u8; 12]; //u96
                    for i in 0..12 {
                        bytes[i] = src.read_u8().await?;
                    }
                    decode_d128(&bytes) as i128 * sign
                }
                17 => {
                    let mut bytes = [0u8; 16]; //u96
                    for i in 0..16 {
                        bytes[i] = src.read_u8().await?;
                    }
                    decode_d128(&bytes) as i128 * sign
                }
                x => {
                    return Err(Error::Protocol(
                        format!("decimal/numeric: invalid length of {} received", x).into(),
                    ))
                }
            };

            Ok(ColumnData::Numeric(Numeric::new_with_scale(value, scale)))
        }
    }

    async fn decode_var_len<R>(
        src: &mut R,
        ctx: &VariableLengthContext,
    ) -> crate::Result<ColumnData<'a>>
    where
        R: AsyncReadLeExt + Unpin,
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
            VarLenType::NChar | VarLenType::NVarchar => {
                Self::decode_variable_string(src, ty, len).await?
            }
            VarLenType::BigVarChar => Self::decode_big_varchar(src, len, collation).await?,
            VarLenType::Money => Self::decode_money(src).await?,
            VarLenType::Datetimen => {
                /*
                let len = self.read_u8().await?;
                parse_datetimen(trans, len)?
                 */
                todo!()
            }
            VarLenType::Daten => {
                /*
                    let len = trans.inner.read_u8()?;
                    match len {
                    0 => ColumnData::None,
                    3 => {
                    let mut bytes = [0u8; 4];
                    try_ready!(trans.inner.read_bytes_to(&mut bytes[..3]));
                    ColumnData::Date(time::Date::new(LittleEndian::read_u32(&bytes)))
                }
                    _ => {
                    return Err(Error::Protocol(
                    format!("daten: length of {} is invalid", len).into(),
                ))
                }
                }
                     */
                todo!()
            }
            VarLenType::Timen => {
                /*
                let rlen = trans.inner.read_u8()?;
                ColumnData::Time(time::Time::decode(&mut *trans.inner, *len, rlen)?)
                 */
                todo!()
            }
            VarLenType::Datetime2 => {
                /*
                let rlen = trans.inner.read_u8()? - 3;
                let time = time::Time::decode(&mut *trans.inner, *len, rlen)?;
                let mut bytes = [0u8; 4];
                try_ready!(trans.inner.read_bytes_to(&mut bytes[..3]));
                let date = time::Date::new(LittleEndian::read_u32(&bytes));
                ColumnData::DateTime2(time::DateTime2(date, time))
                 */
                todo!()
            }
            VarLenType::BigBinary | VarLenType::BigVarBin => Self::decode_binary(src, len).await?,
            VarLenType::Text => Self::decode_text(src).await?,
            VarLenType::NText => Self::decode_ntext(src).await?,
            t => unimplemented!("{:?}", t),
        };

        Ok(res)
    }
}

impl<'a> Encode<BytesMut> for ColumnData<'a> {
    fn encode(self, dst: &mut BytesMut) -> crate::Result<()> {
        match self {
            ColumnData::Bit(val) => {
                let header = [&[VarLenType::Bitn as u8, 1, 1][..]].concat();

                dst.extend_from_slice(&header);
                dst.put_u8(val as u8);
            }
            ColumnData::I8(val) => {
                let header = [&[VarLenType::Intn as u8, 1, 1][..]].concat();

                dst.extend_from_slice(&header);
                dst.put_i8(val);
            }
            ColumnData::I16(val) => {
                let header = [&[VarLenType::Intn as u8, 2, 2][..]].concat();

                dst.extend_from_slice(&header);
                dst.put_i16_le(val);
            }
            ColumnData::I32(val) => {
                let header = [&[VarLenType::Intn as u8, 4, 4][..]].concat();

                dst.extend_from_slice(&header);
                dst.put_i32_le(val);
            }
            ColumnData::I64(val) => {
                let header = [&[VarLenType::Intn as u8, 8, 8][..]].concat();

                dst.extend_from_slice(&header);
                dst.put_i64_le(val);
            }
            ColumnData::F32(val) => {
                let header = [&[VarLenType::Floatn as u8, 4, 4][..]].concat();

                dst.extend_from_slice(&header);
                dst.put_f32_le(val);
            }
            ColumnData::F64(val) => {
                let header = [&[VarLenType::Floatn as u8, 8, 8][..]].concat();

                dst.extend_from_slice(&header);
                dst.put_f64_le(val);
            }
            ColumnData::Guid(uuid) => {
                let header = [&[VarLenType::Guid as u8, 16, 16][..]].concat();

                dst.extend_from_slice(&header);
                dst.extend_from_slice(uuid.as_bytes());
            }
            ColumnData::String(ref s) if s.len() <= 4000 => {
                dst.put_u8(VarLenType::NVarchar as u8);
                dst.put_u16_le(8000);
                dst.extend_from_slice(&[0u8; 5][..]);

                dst.put_u16_le(2 * s.encode_utf16().count() as u16);

                for chr in s.encode_utf16() {
                    dst.put_u16_le(chr);
                }
            }
            ColumnData::String(ref s) => {
                // length: 0xffff and raw collation
                dst.put_u8(VarLenType::NVarchar as u8);
                dst.extend_from_slice(&[0xff as u8; 2][..]);
                dst.extend_from_slice(&[0u8; 5][..]);

                // we cannot cheaply predetermine the length of the UCS2 string beforehand
                // (2 * bytes(UTF8) is not always right) - so just let the SQL server handle it
                dst.put_u64_le(0xfffffffffffffffe as u64);

                // Write the varchar length
                dst.put_u32_le(2 * s.encode_utf16().count() as u32);

                // And the PLP data
                for chr in s.encode_utf16() {
                    dst.put_u16_le(chr);
                }

                // PLP_TERMINATOR
                dst.put_u32_le(0);
            }
            ColumnData::None => {
                dst.put_u8(FixedLenType::Null as u8);
            }
            ColumnData::Binary(bytes) if bytes.len() <= 8000 => {
                dst.put_u8(VarLenType::BigVarBin as u8);
                dst.put_u16_le(8000);
                dst.put_u16_le(bytes.len() as u16);
                dst.extend(bytes.into_owned());
            }
            ColumnData::Binary(bytes) => {
                dst.put_u8(VarLenType::BigVarBin as u8);
                // Max length
                dst.put_u16_le(0xffff as u16);
                // Also the length is unknown
                dst.put_u64_le(0xfffffffffffffffe as u64);
                // We'll write in one chunk, length is the whole bytes length
                dst.put_u32_le(bytes.len() as u32);
                // Payload
                dst.extend(bytes.into_owned());
                // PLP_TERMINATOR
                dst.put_u32_le(0);
            }
            ColumnData::Numeric(_) => todo!(),
        }

        Ok(())
    }
}

impl<'a> ColumnData<'a> {
    async fn decode_bit<R>(src: &mut R) -> crate::Result<ColumnData<'static>>
    where
        R: AsyncReadLeExt + Unpin,
    {
        let recv_len = src.read_u8().await? as usize;

        let res = match recv_len {
            0 => ColumnData::None,
            1 => ColumnData::Bit(src.read_u8().await? > 0),
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
        R: AsyncReadLeExt + Unpin,
    {
        let recv_len = src.read_u8().await? as usize;

        let res = match recv_len {
            0 => ColumnData::None,
            1 => ColumnData::I8(src.read_i8().await?),
            2 => ColumnData::I16(src.read_i16_le().await?),
            4 => ColumnData::I32(src.read_i32_le().await?),
            8 => ColumnData::I64(src.read_i64_le().await?),
            _ => unimplemented!(),
        };

        Ok(res)
    }

    async fn decode_float<R>(src: &mut R) -> crate::Result<ColumnData<'static>>
    where
        R: AsyncReadLeExt + Unpin,
    {
        let len = src.read_u8().await? as usize;

        let res = match len {
            0 => ColumnData::None,
            4 => ColumnData::F32(src.read_f32_le().await?),
            8 => ColumnData::F64(src.read_f64_le().await?),
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
        R: AsyncReadLeExt + Unpin,
    {
        let len = src.read_u8().await? as usize;

        let res = match len {
            0 => ColumnData::None,
            16 => {
                let mut data = [0u8; 16];

                for i in 0..16 {
                    data[i] = src.read_u8().await?;
                }

                ColumnData::Guid(Uuid::from_slice(&data)?)
            }
            _ => {
                return Err(Error::Protocol(
                    format!("guid: length of {} is invalid", len).into(),
                ))
            }
        };

        Ok(res)
    }

    async fn decode_text<R>(src: &mut R) -> crate::Result<ColumnData<'static>>
    where
        R: AsyncReadLeExt + Unpin,
    {
        let ptr_len = src.read_u8().await? as usize;

        if ptr_len == 0 {
            Ok(ColumnData::None)
        } else {
            let _ = src.read_exact(&mut vec![0; ptr_len][0..ptr_len]).await?; // text ptr

            src.read_i32_le().await?; // days
            src.read_u32_le().await?; // second fractions

            let text_len = src.read_u32_le().await? as usize;
            let mut buf = vec![0; text_len];

            src.read_exact(&mut buf[0..text_len]).await?;
            let text = String::from_utf8(buf)?;

            Ok(ColumnData::String(text.into()))
        }
    }

    async fn decode_ntext<R>(src: &mut R) -> crate::Result<ColumnData<'static>>
    where
        R: AsyncReadLeExt + Unpin,
    {
        let ptr_len = src.read_u8().await? as usize;

        if ptr_len == 0 {
            Ok(ColumnData::None)
        } else {
            let _ = src.read_exact(&mut vec![0; ptr_len][0..ptr_len]).await?; // text ptr

            src.read_i32_le().await?; // days
            src.read_u32_le().await?; // second fractions

            let text_len = src.read_u32_le().await? as usize / 2;
            let text = read_varchar(src, text_len).await?;

            Ok(ColumnData::String(text.into()))
        }
    }

    async fn decode_variable_string<R>(
        src: &mut R,
        ty: VarLenType,
        len: usize,
    ) -> crate::Result<ColumnData<'static>>
    where
        R: AsyncReadLeExt + Unpin,
    {
        let mode = if ty == VarLenType::NChar {
            ReadTyMode::FixedSize(len)
        } else {
            ReadTyMode::auto(len)
        };

        let data = Self::decode_plp_type(src, mode).await?;

        let res = if let Some(buf) = data {
            if buf.len() % 2 != 0 {
                return Err(Error::Protocol("nvarchar: invalid plp length".into()));
            }

            let buf: Vec<_> = buf.chunks(2).map(LittleEndian::read_u16).collect();
            let s = String::from_utf16(&buf)?;

            ColumnData::String(s.into())
        } else {
            ColumnData::None
        };

        Ok(res)
    }

    async fn decode_big_varchar<R>(
        src: &mut R,
        len: usize,
        collation: Option<Collation>,
    ) -> crate::Result<ColumnData<'static>>
    where
        R: AsyncReadLeExt + Unpin,
    {
        let mode = ReadTyMode::auto(len);
        let data = Self::decode_plp_type(src, mode).await?;

        let res = if let Some(bytes) = data {
            let encoder = collation
                .as_ref()
                .unwrap()
                .encoding()
                .ok_or(Error::Encoding("encoding: unspported encoding".into()))?;

            let s: String = encoder
                .decode(bytes.as_ref(), DecoderTrap::Strict)
                .map_err(Error::Encoding)?;

            ColumnData::String(s.into())
        } else {
            ColumnData::None
        };

        Ok(res)
    }

    async fn decode_money<R>(src: &mut R) -> crate::Result<ColumnData<'static>>
    where
        R: AsyncReadLeExt + Unpin,
    {
        let len = src.read_u8().await?;

        let res = match len {
            0 => ColumnData::None,
            4 => ColumnData::F64(src.read_i32_le().await? as f64 / 1e4),
            8 => ColumnData::F64({
                let high = src.read_i32_le().await? as i64;
                let low = src.read_u32_le().await? as f64;
                ((high << 32) as f64 + low) / 1e4
            }),
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
        R: AsyncReadLeExt + Unpin,
    {
        let mode = ReadTyMode::auto(len);
        let data = Self::decode_plp_type(src, mode).await?;

        let res = if let Some(buf) = data {
            ColumnData::Binary(buf.into())
        } else {
            ColumnData::None
        };

        Ok(res)
    }

    pub(crate) async fn decode_plp_type<R>(
        src: &mut R,
        mode: ReadTyMode,
    ) -> crate::Result<Option<Vec<u8>>>
    where
        R: AsyncReadLeExt + Unpin,
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

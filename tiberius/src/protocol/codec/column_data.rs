use super::{BaseMetaDataColumn, BytesData, Decode, Encode, FixedLenType, TypeInfo, VarLenType};
use crate::{
    protocol::{self, types::Numeric},
    Error,
};
use byteorder::{ByteOrder, LittleEndian};
use bytes::{Buf, BufMut, BytesMut};
use encoding::DecoderTrap;
use protocol::types::{Collation, Guid};
use std::borrow::Cow;

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
    Guid(Cow<'a, Guid>),
    Binary(Cow<'a, [u8]>),
    Numeric(Numeric),
    /* Guid(Cow<'a, Guid>),
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
    scale: u8,
}

impl VariableLengthContext {
    pub fn new(ty: VarLenType, len: usize, collation: Option<Collation>) -> Self {
        Self { ty, len, collation }
    }
}

impl<'a> Decode<BytesData<'a, BaseMetaDataColumn>> for ColumnData<'static> {
    fn decode(src: &mut BytesData<'a, BaseMetaDataColumn>) -> crate::Result<Self>
    where
        Self: Sized,
    {
        let ret = match src.context().ty {
            TypeInfo::FixedLen(fixed_ty) => {
                let mut src: BytesData<FixedLenType> = BytesData::new(src.inner(), &fixed_ty);

                ColumnData::decode(&mut src)?
            }
            TypeInfo::VarLenSized(ty, len, collation) => {
                let context = VariableLengthContext::new(ty, len, collation);

                let mut src: BytesData<VariableLengthContext> =
                    BytesData::new(src.inner(), &context);

                ColumnData::decode(&mut src)?
            }
            TypeInfo::VarLenSizedPrecision { ty, scale, .. } => {
                match ty {
                    // Our representation causes loss of information and is only a very approximate representation
                    // while decimal on the side of MSSQL is an exact representation
                    // TODO: better representation
                    VarLenType::Decimaln | VarLenType::Numericn => {
                        let context = VariableLengthPrecisionContext { scale };
                        let mut src = BytesData::new(src.inner(), &context);

                        ColumnData::decode(&mut src)?
                    }
                    _ => todo!(),
                }
            }
        };

        Ok(ret)
    }
}

impl<'a> Decode<BytesData<'a, VariableLengthContext>> for ColumnData<'static> {
    fn decode(src: &mut BytesData<'a, VariableLengthContext>) -> crate::Result<Self>
    where
        Self: Sized,
    {
        let ty = src.context().ty;
        let len = src.context().len;
        let collation = src.context().collation;
        let src = src.inner();

        let res = match ty {
            VarLenType::Bitn => Self::decode_bit(src)?,
            VarLenType::Intn => Self::decode_int(src)?,
            // 2.2.5.5.1.5 IEEE754
            VarLenType::Floatn => Self::decode_float(src)?,
            VarLenType::Guid => Self::decode_guid(src)?,
            VarLenType::NChar | VarLenType::NVarchar => Self::decode_variable_string(ty, len, src)?,
            VarLenType::BigVarChar => Self::decode_big_varchar(len, collation, src)?,
            VarLenType::Money => Self::decode_money(src)?,
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
            VarLenType::BigBinary => Self::decode_binary(len, src)?,

            _ => unimplemented!(),
        };

        Ok(res)
    }
}

impl<'a> Decode<BytesData<'a, FixedLenType>> for ColumnData<'static> {
    fn decode(src: &mut BytesData<'a, FixedLenType>) -> crate::Result<Self>
    where
        Self: Sized,
    {
        let ret = match *src.context() {
            FixedLenType::Null => ColumnData::None,
            FixedLenType::Bit => ColumnData::Bit(src.get_u8() != 0),
            FixedLenType::Int1 => ColumnData::I8(src.get_i8()),
            FixedLenType::Int2 => ColumnData::I16(src.get_i16_le()),
            FixedLenType::Int4 => ColumnData::I32(src.get_i32_le()),
            FixedLenType::Int8 => ColumnData::I64(src.get_i64_le()),
            FixedLenType::Float4 => ColumnData::F32(src.get_f32_le()),
            FixedLenType::Float8 => ColumnData::F64(src.get_f64_le()),
            // FixedLenType::Datetime => parse_datetimen(trans, 8)?,
            // FixedLenType::Datetime4 => parse_datetimen(trans, 4)?,
            _ => {
                return Err(Error::Protocol(
                    format!("unsupported fixed type decoding: {:?}", src.context()).into(),
                ))
            }
        };

        Ok(ret)
    }
}

impl<'a> Decode<BytesData<'a, VariableLengthPrecisionContext>> for ColumnData<'static> {
    fn decode(src: &mut BytesData<'a, VariableLengthPrecisionContext>) -> crate::Result<Self>
    where
        Self: Sized,
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

        let len = src.get_u8();

        if len == 0 {
            Ok(ColumnData::None)
        } else {
            let sign = match src.get_u8() {
                0 => -1i128,
                1 => 1i128,
                _ => return Err(Error::Protocol("decimal: invalid sign".into())),
            };

            let value = match len {
                5 => src.get_u32_le() as i128 * sign,
                9 => src.get_u64_le() as i128 * sign,
                13 => {
                    let mut bytes = [0u8; 12]; //u96
                    for i in 0..12 {
                        bytes[i] = src.get_u8();
                    }
                    decode_d128(&bytes) as i128 * sign
                }
                17 => {
                    let mut bytes = [0u8; 16]; //u96
                    for i in 0..16 {
                        bytes[i] = src.get_u8();
                    }
                    decode_d128(&bytes) as i128 * sign
                }
                x => {
                    return Err(Error::Protocol(
                        format!("decimal/numeric: invalid length of {} received", x).into(),
                    ))
                }
            };

            Ok(ColumnData::Numeric(Numeric::new_with_scale(
                value,
                src.context().scale,
            )))
        }
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
            ColumnData::String(ref s) if s.len() <= 4000 => {
                dst.put_u8(VarLenType::NVarchar as u8);
                dst.put_u16_le(8000);
                dst.extend_from_slice(&[0u8; 5][..]);
                dst.put_u16_le(2 * s.chars().count() as u16);

                for chr in s.encode_utf16() {
                    dst.put_u16_le(chr);
                }
            }
            ColumnData::String(ref str_) => {
                // length: 0xffff and raw collation
                dst.put_u8(VarLenType::NVarchar as u8);
                dst.extend_from_slice(&[0xff as u8; 2][..]);
                dst.extend_from_slice(&[0u8; 5][..]);

                // we cannot cheaply predetermine the length of the UCS2 string beforehand
                // (2 * bytes(UTF8) is not always right) - so just let the SQL server handle it
                dst.put_u64_le(0xfffffffffffffffe as u64);

                // Write the varchar length
                let ary: Vec<_> = str_.encode_utf16().collect();
                dst.put_u32_le((ary.len() * 2) as u32);

                // And the PLP data
                for chr in ary {
                    dst.put_u16_le(chr);
                }

                // PLP_TERMINATOR
                dst.put_u32_le(0);
            }
            // TODO
            ColumnData::None => {}
            ColumnData::Guid(_) => {}
            ColumnData::Binary(_) => {}
            ColumnData::Numeric(_) => {}
        }

        Ok(())
    }
}

impl<'a> ColumnData<'a> {
    fn decode_bit(src: &mut BytesMut) -> crate::Result<ColumnData<'static>> {
        let recv_len = src.get_u8() as usize;

        let res = match recv_len {
            0 => ColumnData::None,
            1 => ColumnData::Bit(src.get_u8() > 0),
            v => {
                return Err(Error::Protocol(
                    format!("bitn: length of {} is invalid", v).into(),
                ))
            }
        };

        Ok(res)
    }

    fn decode_int(src: &mut BytesMut) -> crate::Result<ColumnData<'static>> {
        let recv_len = src.get_u8() as usize;

        let res = match recv_len {
            0 => ColumnData::None,
            1 => ColumnData::I8(src.get_i8()),
            2 => ColumnData::I16(src.get_i16_le()),
            4 => ColumnData::I32(src.get_i32_le()),
            8 => ColumnData::I64(src.get_i64_le()),
            _ => unimplemented!(),
        };

        Ok(res)
    }

    fn decode_float(src: &mut BytesMut) -> crate::Result<ColumnData<'static>> {
        let len = src.get_u8() as usize;

        let res = match len {
            0 => ColumnData::None,
            4 => ColumnData::F32(src.get_f32_le()),
            8 => ColumnData::F64(src.get_f64_le()),
            _ => {
                return Err(Error::Protocol(
                    format!("floatn: length of {} is invalid", len).into(),
                ))
            }
        };

        Ok(res)
    }

    fn decode_guid(src: &mut BytesMut) -> crate::Result<ColumnData<'static>> {
        let len = src.get_u8() as usize;

        let res = match len {
            0 => ColumnData::None,
            16 => {
                let mut data = [0u8; 16];

                for i in 0..16 {
                    data[i] = src.get_u8();
                }

                ColumnData::Guid(Cow::Owned(Guid(data)))
            }
            _ => {
                return Err(Error::Protocol(
                    format!("guid: length of {} is invalid", len).into(),
                ))
            }
        };

        Ok(res)
    }

    fn decode_variable_string(
        ty: VarLenType,
        len: usize,
        src: &mut BytesMut,
    ) -> crate::Result<ColumnData<'static>> {
        let mode = if ty == VarLenType::NChar {
            ReadTyMode::FixedSize(len)
        } else {
            ReadTyMode::auto(len)
        };

        let data = Self::decode_plp_type(mode, src)?;

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

    fn decode_big_varchar(
        len: usize,
        collation: Option<Collation>,
        src: &mut BytesMut,
    ) -> crate::Result<ColumnData<'static>> {
        let mode = ReadTyMode::auto(len);
        let data = Self::decode_plp_type(mode, src)?;

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

    fn decode_money(src: &mut BytesMut) -> crate::Result<ColumnData<'static>> {
        let len = src.get_u8();

        let res = match len {
            0 => ColumnData::None,
            4 => ColumnData::F64(src.get_i32_le() as f64 / 1e4),
            8 => ColumnData::F64({
                let high = src.get_i32_le() as i64;
                let low = src.get_u32_le() as f64;
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

    fn decode_binary(len: usize, src: &mut BytesMut) -> crate::Result<ColumnData<'static>> {
        let mode = ReadTyMode::auto(len);
        let data = Self::decode_plp_type(mode, src)?;

        let res = if let Some(buf) = data {
            ColumnData::Binary(buf.into())
        } else {
            ColumnData::None
        };

        Ok(res)
    }

    /// read byte string with or without PLP
    pub fn decode_plp_type(mode: ReadTyMode, src: &mut BytesMut) -> crate::Result<Option<Vec<u8>>> {
        let mut read_state = ReadTyState::new(mode);

        // If we did not read anything yet, initialize the reader.
        if read_state.data.is_none() {
            let size = match read_state.mode {
                ReadTyMode::FixedSize(_) => src.get_u16_le() as u64,
                ReadTyMode::Plp => src.get_u64_le(),
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
                        ReadTyMode::Plp => src.get_u32_le() as usize,
                    };

                    if chunk_size == 0 {
                        break; // found a sentinel, we're done
                    } else {
                        read_state.chunk_data_left = chunk_size
                    }
                } else {
                    // Just read a byte
                    let byte = src.get_u8();
                    read_state.chunk_data_left -= 1;

                    buf.push(byte);
                }
            }
        }

        Ok(read_state.data.take())
    }
}

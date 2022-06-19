use super::{AllHeaderTy, Encode, ALL_HEADERS_LEN_TX};
use crate::{tds::codec::ColumnData, FixedLenType, Result, VarLenType};
use bytes::{BufMut, BytesMut};
use enumflags2::{bitflags, BitFlags};
use std::borrow::BorrowMut;
use std::borrow::Cow;

#[bitflags]
#[repr(u8)]
#[derive(Copy, Clone, Debug, PartialEq)]
pub enum RpcStatus {
    ByRefValue = 1 << 0,
    DefaultValue = 1 << 1,
    // reserved
    Encrypted = 1 << 3,
}

#[bitflags]
#[repr(u16)]
#[derive(Copy, Clone, Debug, PartialEq)]
pub enum RpcOption {
    WithRecomp = 1 << 0,
    NoMeta = 1 << 1,
    ReuseMeta = 1 << 2,
}

#[derive(Debug)]
pub struct TokenRpcRequest<'a> {
    proc_id: RpcProcIdValue<'a>,
    flags: BitFlags<RpcOption>,
    params: Vec<RpcParam<'a>>,
    transaction_desc: [u8; 8],
}

impl<'a> TokenRpcRequest<'a> {
    pub fn new<I>(proc_id: I, params: Vec<RpcParam<'a>>, transaction_desc: [u8; 8]) -> Self
    where
        I: Into<RpcProcIdValue<'a>>,
    {
        Self {
            proc_id: proc_id.into(),
            flags: BitFlags::empty(),
            params,
            transaction_desc,
        }
    }
}

#[derive(Debug)]
pub struct RpcParam<'a> {
    pub name: Cow<'a, str>,
    pub flags: BitFlags<RpcStatus>,
    pub value: ColumnData<'a>,
}

/// 2.2.6.6 RPC Request
#[allow(dead_code)]
#[repr(u8)]
#[derive(Clone, Copy, Debug)]
pub enum RpcProcId {
    CursorOpen = 2,
    CursorFetch = 7,
    CursorClose = 9,
    ExecuteSQL = 10,
    Prepare = 11,
    Execute = 12,
    PrepExec = 13,
    Unprepare = 15,
}

#[derive(Debug)]
#[allow(dead_code)]
pub enum RpcProcIdValue<'a> {
    Name(Cow<'a, str>),
    Id(RpcProcId),
}

impl<'a, S> From<S> for RpcProcIdValue<'a>
where
    S: Into<Cow<'a, str>>,
{
    fn from(s: S) -> Self {
        Self::Name(s.into())
    }
}

impl<'a> From<RpcProcId> for RpcProcIdValue<'a> {
    fn from(id: RpcProcId) -> Self {
        Self::Id(id)
    }
}

impl<'a> Encode<BytesMut> for TokenRpcRequest<'a> {
    fn encode(self, dst: &mut BytesMut) -> Result<()> {
        dst.put_u32_le(ALL_HEADERS_LEN_TX as u32);
        dst.put_u32_le(ALL_HEADERS_LEN_TX as u32 - 4);
        dst.put_u16_le(AllHeaderTy::TransactionDescriptor as u16);
        dst.put_slice(&self.transaction_desc);
        dst.put_u32_le(1);

        match self.proc_id {
            RpcProcIdValue::Id(ref id) => {
                let val = (0xffff_u32) | ((*id as u16) as u32) << 16;
                dst.put_u32_le(val);
            }
            RpcProcIdValue::Name(ref _name) => {
                //let (left_bytes, _) = try!(write_varchar::<u16>(&mut cursor, name, 0));
                //assert_eq!(left_bytes, 0);
                todo!()
            }
        }

        dst.put_u16_le(self.flags.bits() as u16);

        for param in self.params.into_iter() {
            param.encode(dst)?;
        }

        Ok(())
    }
}

impl<'a> Encode<BytesMut> for RpcParam<'a> {
    fn encode(self, dst: &mut BytesMut) -> Result<()> {
        let len_pos = dst.len();
        let mut length = 0u8;

        dst.put_u8(length);

        for codepoint in self.name.encode_utf16() {
            length += 1;
            dst.put_u16_le(codepoint);
        }

        dst.put_u8(self.flags.bits());

        match self.value {
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
                // let header = [VarLenType::Intn as u8, 8, 8];
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
                super::guid::reorder_bytes(&mut data);
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
                dt.encode(&mut *dst)?;
            }
            ColumnData::SmallDateTime(Some(dt)) => {
                dst.extend_from_slice(&[VarLenType::Datetimen as u8, 4, 4]);
                dt.encode(&mut *dst)?;
            }
            #[cfg(feature = "tds73")]
            ColumnData::Time(Some(time)) => {
                dst.extend_from_slice(&[VarLenType::Timen as u8, time.scale(), time.len()?]);
                time.encode(&mut *dst)?;
            }
            #[cfg(feature = "tds73")]
            ColumnData::Date(Some(date)) => {
                dst.extend_from_slice(&[VarLenType::Daten as u8, 3]);
                date.encode(&mut *dst)?;
            }
            #[cfg(feature = "tds73")]
            ColumnData::DateTime2(Some(dt)) => {
                let len = dt.time().len()? + 3;
                dst.extend_from_slice(&[VarLenType::Datetime2 as u8, dt.time().scale(), len]);
                dt.encode(&mut *dst)?;
            }
            #[cfg(feature = "tds73")]
            ColumnData::DateTimeOffset(Some(dto)) => {
                let headers = &[
                    VarLenType::DatetimeOffsetn as u8,
                    dto.datetime2().time().scale(),
                    dto.datetime2().time().len()? + 5,
                ];

                dst.extend_from_slice(headers);
                dto.encode(&mut *dst)?;
            }
            ColumnData::Xml(Some(xml)) => {
                dst.put_u8(VarLenType::Xml as u8);
                dst.put_u8(0);
                xml.into_owned().encode(&mut *dst)?;
            }
            ColumnData::Numeric(Some(num)) => {
                let headers = &[
                    VarLenType::Numericn as u8,
                    num.len(),
                    num.precision(),
                    num.scale(),
                ];

                dst.extend_from_slice(headers);
                num.encode(&mut *dst)?;
            }
            _ => {
                // None/null
                dst.put_u8(FixedLenType::Null as u8);
            }
        }

        let dst: &mut [u8] = dst.borrow_mut();
        dst[len_pos] = length;

        Ok(())
    }
}

mod binary;
mod bit;
#[cfg(feature = "tds73")]
mod date;
#[cfg(feature = "tds73")]
mod datetime2;
mod datetimen;
#[cfg(feature = "tds73")]
mod datetimeoffsetn;
mod fixed_len;
mod float;
mod guid;
mod image;
mod int;
mod money;
mod plp;
mod string;
mod text;
#[cfg(feature = "tds73")]
mod time;
mod var_len;
mod xml;

use super::{Encode, FixedLenType, TypeInfo, VarLenType};
#[cfg(feature = "tds73")]
use crate::tds::time::{Date, DateTime2, DateTimeOffset, Time};
use crate::{
    tds::{codec::TypeInfoInner, time::DateTime, time::SmallDateTime, xml::XmlData, Numeric},
    SqlReadBytes,
};
use bytes::{BufMut, BytesMut};
use encoding::EncoderTrap;
use std::borrow::{BorrowMut, Cow};
use uuid::Uuid;

const MAX_NVARCHAR_SIZE: usize = 1 << 30;

#[derive(Clone, Debug, PartialEq)]
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

    pub(crate) async fn decode<R>(src: &mut R, ctx: &TypeInfo) -> crate::Result<ColumnData<'a>>
    where
        R: SqlReadBytes + Unpin,
    {
        let res = match &ctx.inner {
            TypeInfoInner::FixedLen(fixed_ty) => fixed_len::decode(src, fixed_ty).await?,
            TypeInfoInner::VarLenSized(cx) => var_len::decode(src, cx).await?,
            TypeInfoInner::VarLenSizedPrecision { ty, scale, .. } => match ty {
                VarLenType::Decimaln | VarLenType::Numericn => {
                    ColumnData::Numeric(Numeric::decode(src, *scale).await?)
                }
                _ => todo!(),
            },
            TypeInfoInner::Xml { schema, size } => xml::decode(src, *size, schema.clone()).await?,
        };

        Ok(res)
    }

    pub(crate) fn encode(self, dst: &mut BytesMut, ctx: &TypeInfo) -> crate::Result<()> {
        match (self, &ctx.inner) {
            (ColumnData::Bit(opt), TypeInfoInner::VarLenSized(vlc))
                if vlc.r#type() == VarLenType::Bitn =>
            {
                if let Some(val) = opt {
                    dst.put_u8(1);
                    dst.put_u8(val as u8);
                } else {
                    dst.put_u8(0);
                }
            }
            (ColumnData::Bit(Some(val)), TypeInfoInner::FixedLen(FixedLenType::Bit)) => {
                dst.put_u8(val as u8);
            }
            (ColumnData::U8(opt), TypeInfoInner::VarLenSized(vlc))
                if vlc.r#type() == VarLenType::Intn =>
            {
                if let Some(val) = opt {
                    dst.put_u8(1);
                    dst.put_u8(val);
                } else {
                    dst.put_u8(0);
                }
            }
            (ColumnData::U8(Some(val)), TypeInfoInner::FixedLen(FixedLenType::Int1)) => {
                dst.put_u8(val);
            }
            (ColumnData::I16(Some(val)), TypeInfoInner::FixedLen(FixedLenType::Int2)) => {
                dst.put_i16_le(val);
            }
            (ColumnData::I16(opt), TypeInfoInner::VarLenSized(vlc))
                if vlc.r#type() == VarLenType::Intn =>
            {
                if let Some(val) = opt {
                    dst.put_u8(2);
                    dst.put_i16_le(val);
                } else {
                    dst.put_u8(0);
                }
            }
            (ColumnData::I32(Some(val)), TypeInfoInner::FixedLen(FixedLenType::Int4)) => {
                dst.put_i32_le(val);
            }
            (ColumnData::I32(opt), TypeInfoInner::VarLenSized(vlc))
                if vlc.r#type() == VarLenType::Intn =>
            {
                if let Some(val) = opt {
                    dst.put_u8(4);
                    dst.put_i32_le(val);
                } else {
                    dst.put_u8(0);
                }
            }
            (ColumnData::I64(Some(val)), TypeInfoInner::FixedLen(FixedLenType::Int8)) => {
                dst.put_i64_le(val);
            }
            (ColumnData::I64(opt), TypeInfoInner::VarLenSized(vlc))
                if vlc.r#type() == VarLenType::Intn =>
            {
                if let Some(val) = opt {
                    dst.put_u8(8);
                    dst.put_i64_le(val);
                } else {
                    dst.put_u8(0);
                }
            }
            (ColumnData::F32(Some(val)), TypeInfoInner::FixedLen(FixedLenType::Float4)) => {
                dst.put_f32_le(val);
            }
            (ColumnData::F32(opt), TypeInfoInner::VarLenSized(vlc))
                if vlc.r#type() == VarLenType::Floatn =>
            {
                if let Some(val) = opt {
                    dst.put_u8(4);
                    dst.put_f32_le(val);
                } else {
                    dst.put_u8(0);
                }
            }
            (ColumnData::F64(Some(val)), TypeInfoInner::FixedLen(FixedLenType::Float8)) => {
                dst.put_f64_le(val);
            }
            (ColumnData::F64(opt), TypeInfoInner::VarLenSized(vlc))
                if vlc.r#type() == VarLenType::Floatn =>
            {
                if let Some(val) = opt {
                    dst.put_u8(8);
                    dst.put_f64_le(val);
                } else {
                    dst.put_u8(0);
                }
            }
            (ColumnData::Guid(opt), TypeInfoInner::VarLenSized(vlc))
                if vlc.r#type() == VarLenType::Guid =>
            {
                if let Some(uuid) = opt {
                    dst.put_u8(16);

                    let mut data = *uuid.as_bytes();
                    super::guid::reorder_bytes(&mut data);
                    dst.extend_from_slice(&data);
                } else {
                    dst.put_u8(0);
                }
            }
            (ColumnData::String(opt), TypeInfoInner::VarLenSized(vlc))
                if vlc.r#type() == VarLenType::BigChar
                    || vlc.r#type() == VarLenType::BigVarChar =>
            {
                if let Some(str) = opt {
                    let len_pos = dst.len();

                    dst.put_u16_le(0u16);

                    let encoder = vlc.collation().as_ref().unwrap().encoding()?;

                    let bytes = encoder
                        .encode(str.as_ref(), EncoderTrap::Strict)
                        .map_err(crate::Error::Encoding)?;
                    dst.extend_from_slice(bytes.as_slice());
                    let length = (dst.len() - len_pos - 2) as u16;

                    let dst: &mut [u8] = dst.borrow_mut();
                    let mut dst = &mut dst[len_pos..];
                    dst.put_u16_le(length);
                } else {
                    dst.put_u16_le(0xffff);
                }
            }
            (ColumnData::String(opt), TypeInfoInner::VarLenSized(vlc))
                if vlc.r#type() == VarLenType::NVarchar || vlc.r#type() == VarLenType::NChar =>
            {
                if let Some(str) = opt {
                    if str.len() > vlc.len() {
                        Err(crate::Error::BulkInput(
                            format!(
                                "String length {} exceed column limit {}",
                                str.len(),
                                vlc.len()
                            )
                            .into(),
                        ))?;
                    }

                    if vlc.len() < 0xffff {
                        let len_pos = dst.len();
                        dst.put_u16_le(0u16);

                        for chr in str.encode_utf16() {
                            dst.put_u16_le(chr);
                        }

                        let length = (dst.len() - len_pos - 2) as u16;

                        let dst: &mut [u8] = dst.borrow_mut();
                        let mut dst = &mut dst[len_pos..];
                        dst.put_u16_le(length);
                    } else {
                        // unknown size
                        dst.put_u64_le(0xfffffffffffffffe);

                        assert!(
                            str.len() < 0xffffffff,
                            "if str longer than this, need to implement multiple blobs"
                        );

                        let len_pos = dst.len();
                        dst.put_u32_le(0u32);

                        for chr in str.encode_utf16() {
                            dst.put_u16_le(chr);
                        }

                        let length = (dst.len() - len_pos - 4) as u32;

                        // no next blob
                        dst.put_u32_le(0u32);

                        let dst: &mut [u8] = dst.borrow_mut();
                        let mut dst = &mut dst[len_pos..];
                        dst.put_u32_le(length);
                    }
                } else {
                    if vlc.len() < 0xffff {
                        dst.put_u16_le(0xffff);
                    } else {
                        dst.put_u64_le(0xffffffffffffffff)
                    }
                }
            }
            (ColumnData::Binary(opt), TypeInfoInner::VarLenSized(vlc))
                if vlc.r#type() == VarLenType::BigBinary
                    || vlc.r#type() == VarLenType::BigVarBin =>
            {
                if let Some(bytes) = opt {
                    dst.put_u16_le(bytes.len() as u16);
                    dst.extend(bytes.into_owned());
                } else {
                    dst.put_u16_le(0xffff);
                }
            }
            (ColumnData::DateTime(opt), TypeInfoInner::VarLenSized(vlc))
                if vlc.r#type() == VarLenType::Datetimen =>
            {
                if let Some(dt) = opt {
                    dst.put_u8(8);
                    dt.encode(dst)?;
                } else {
                    dst.put_u8(0);
                }
            }
            (ColumnData::DateTime(Some(dt)), TypeInfoInner::FixedLen(FixedLenType::Datetime)) => {
                dt.encode(dst)?;
            }
            (ColumnData::SmallDateTime(opt), TypeInfoInner::VarLenSized(vlc))
                if vlc.r#type() == VarLenType::Datetimen =>
            {
                if let Some(dt) = opt {
                    dst.put_u8(4);
                    dt.encode(dst)?;
                } else {
                    dst.put_u8(0);
                }
            }
            (
                ColumnData::SmallDateTime(Some(dt)),
                TypeInfoInner::FixedLen(FixedLenType::Datetime4),
            ) => {
                dt.encode(dst)?;
            }
            #[cfg(feature = "tds73")]
            (ColumnData::Date(opt), TypeInfoInner::VarLenSized(vlc))
                if vlc.r#type() == VarLenType::Daten =>
            {
                if let Some(dt) = opt {
                    dst.put_u8(3);
                    dt.encode(dst)?;
                } else {
                    dst.put_u8(0);
                }
            }
            #[cfg(feature = "tds73")]
            (ColumnData::Time(opt), TypeInfoInner::VarLenSized(vlc))
                if vlc.r#type() == VarLenType::Timen =>
            {
                if let Some(time) = opt {
                    dst.put_u8(time.len()?);
                    time.encode(dst)?;
                } else {
                    dst.put_u8(0);
                }
            }
            #[cfg(feature = "tds73")]
            (ColumnData::DateTime2(opt), TypeInfoInner::VarLenSized(vlc))
                if vlc.r#type() == VarLenType::Datetime2 =>
            {
                if let Some(dt2) = opt {
                    dst.put_u8(dt2.time().len()? + 3);
                    dt2.encode(dst)?;
                } else {
                    dst.put_u8(0);
                }
            }
            #[cfg(feature = "tds73")]
            (ColumnData::DateTimeOffset(opt), TypeInfoInner::VarLenSized(vlc))
                if vlc.r#type() == VarLenType::DatetimeOffsetn =>
            {
                if let Some(dto) = opt {
                    dst.put_u8(dto.datetime2().time().len()? + 5);
                    dto.encode(dst)?;
                } else {
                    dst.put_u8(0);
                }
            }
            (ColumnData::Xml(opt), TypeInfoInner::Xml { .. }) => {
                if let Some(xml) = opt {
                    xml.into_owned().encode(dst)?;
                } else {
                    dst.put_u64_le(0xffffffffffffffff_u64);
                }
            }
            (ColumnData::Numeric(opt), TypeInfoInner::VarLenSizedPrecision { ty, scale, .. })
                if ty == &VarLenType::Numericn || ty == &VarLenType::Decimaln =>
            {
                if let Some(num) = opt {
                    if scale != &num.scale() {
                        todo!("this still need some work, if client scale not aligned with server, we need to do conversion but will lose precision")
                    }
                    num.encode(&mut *dst)?;
                } else {
                    dst.put_u8(0);
                }
            }
            (v, ref ti) => Err(crate::Error::BulkInput(
                format!("invalid data type, expecting {:?} but found {:?}", ti, v).into(),
            ))?,
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql_read_bytes::test_utils::IntoSqlReadBytes;
    use crate::tds::Collation;
    use crate::{Error, VarLenContext};
    use bytes::BytesMut;

    async fn test_round_trip<'a>(inner: TypeInfoInner, d: ColumnData<'a>) {
        let mut buf = BytesMut::new();
        let ti = TypeInfo { inner };

        d.clone()
            .encode(&mut buf, &ti)
            .expect("encode must succeed");

        let nd = ColumnData::decode(&mut buf.into_sql_read_bytes(), &ti)
            .await
            .expect("decode must succeed");

        assert_eq!(nd, d)
    }

    #[tokio::test]
    async fn i32_with_varlen_int() {
        test_round_trip(
            TypeInfoInner::VarLenSized(VarLenContext::new(VarLenType::Intn, 4, None)),
            ColumnData::I32(Some(42)),
        )
        .await;
    }

    #[tokio::test]
    async fn none_with_varlen_int() {
        test_round_trip(
            TypeInfoInner::VarLenSized(VarLenContext::new(VarLenType::Intn, 4, None)),
            ColumnData::I32(None),
        )
        .await;
    }

    #[tokio::test]
    async fn i32_with_fixedlen_int() {
        test_round_trip(
            TypeInfoInner::FixedLen(FixedLenType::Int4),
            ColumnData::I32(Some(42)),
        )
        .await;
    }

    #[tokio::test]
    async fn bit_with_varlen_bit() {
        test_round_trip(
            TypeInfoInner::VarLenSized(VarLenContext::new(VarLenType::Bitn, 1, None)),
            ColumnData::Bit(Some(true)),
        )
        .await;
    }

    #[tokio::test]
    async fn none_with_varlen_bit() {
        test_round_trip(
            TypeInfoInner::VarLenSized(VarLenContext::new(VarLenType::Bitn, 1, None)),
            ColumnData::Bit(None),
        )
        .await;
    }

    #[tokio::test]
    async fn bit_with_fixedlen_bit() {
        test_round_trip(
            TypeInfoInner::FixedLen(FixedLenType::Bit),
            ColumnData::Bit(Some(true)),
        )
        .await;
    }

    #[tokio::test]
    async fn u8_with_varlen_int() {
        test_round_trip(
            TypeInfoInner::VarLenSized(VarLenContext::new(VarLenType::Intn, 1, None)),
            ColumnData::U8(Some(8u8)),
        )
        .await;
    }

    #[tokio::test]
    async fn none_u8_with_varlen_int() {
        test_round_trip(
            TypeInfoInner::VarLenSized(VarLenContext::new(VarLenType::Intn, 1, None)),
            ColumnData::U8(None),
        )
        .await;
    }

    #[tokio::test]
    async fn u8_with_fixedlen_int() {
        test_round_trip(
            TypeInfoInner::FixedLen(FixedLenType::Int1),
            ColumnData::U8(Some(8u8)),
        )
        .await;
    }

    #[tokio::test]
    async fn i16_with_varlen_intn() {
        test_round_trip(
            TypeInfoInner::VarLenSized(VarLenContext::new(VarLenType::Intn, 2, None)),
            ColumnData::I16(Some(8i16)),
        )
        .await;
    }

    #[tokio::test]
    async fn none_i16_with_varlen_intn() {
        test_round_trip(
            TypeInfoInner::VarLenSized(VarLenContext::new(VarLenType::Intn, 2, None)),
            ColumnData::I16(None),
        )
        .await;
    }

    #[tokio::test]
    async fn none_with_varlen_intn() {
        test_round_trip(
            TypeInfoInner::FixedLen(FixedLenType::Int2),
            ColumnData::I16(Some(8i16)),
        )
        .await;
    }

    #[tokio::test]
    async fn i64_with_varlen_intn() {
        test_round_trip(
            TypeInfoInner::VarLenSized(VarLenContext::new(VarLenType::Intn, 8, None)),
            ColumnData::I64(Some(8i64)),
        )
        .await;
    }

    #[tokio::test]
    async fn i64_none_with_varlen_intn() {
        test_round_trip(
            TypeInfoInner::VarLenSized(VarLenContext::new(VarLenType::Intn, 8, None)),
            ColumnData::I64(None),
        )
        .await;
    }

    #[tokio::test]
    async fn i64_with_fixedlen_int8() {
        test_round_trip(
            TypeInfoInner::FixedLen(FixedLenType::Int8),
            ColumnData::I64(Some(8i64)),
        )
        .await;
    }

    #[tokio::test]
    async fn f32_with_varlen_floatn() {
        test_round_trip(
            TypeInfoInner::VarLenSized(VarLenContext::new(VarLenType::Floatn, 4, None)),
            ColumnData::F32(Some(8f32)),
        )
        .await;
    }

    #[tokio::test]
    async fn null_f32_with_varlen_floatn() {
        test_round_trip(
            TypeInfoInner::VarLenSized(VarLenContext::new(VarLenType::Floatn, 4, None)),
            ColumnData::F32(None),
        )
        .await;
    }

    #[tokio::test]
    async fn f32_with_fixedlen_float4() {
        test_round_trip(
            TypeInfoInner::FixedLen(FixedLenType::Float4),
            ColumnData::F32(Some(8f32)),
        )
        .await;
    }

    #[tokio::test]
    async fn f64_with_varlen_floatn() {
        test_round_trip(
            TypeInfoInner::VarLenSized(VarLenContext::new(VarLenType::Floatn, 8, None)),
            ColumnData::F64(Some(8f64)),
        )
        .await;
    }

    #[tokio::test]
    async fn none_f64_with_varlen_floatn() {
        test_round_trip(
            TypeInfoInner::VarLenSized(VarLenContext::new(VarLenType::Floatn, 8, None)),
            ColumnData::F64(None),
        )
        .await;
    }

    #[tokio::test]
    async fn f64_with_fixedlen_float8() {
        test_round_trip(
            TypeInfoInner::FixedLen(FixedLenType::Float8),
            ColumnData::F64(Some(8f64)),
        )
        .await;
    }

    #[tokio::test]
    async fn guid_with_varlen_guid() {
        test_round_trip(
            TypeInfoInner::VarLenSized(VarLenContext::new(VarLenType::Guid, 16, None)),
            ColumnData::Guid(Some(Uuid::new_v4())),
        )
        .await;
    }

    #[tokio::test]
    async fn none_guid_with_varlen_guid() {
        test_round_trip(
            TypeInfoInner::VarLenSized(VarLenContext::new(VarLenType::Guid, 16, None)),
            ColumnData::Guid(None),
        )
        .await;
    }

    #[tokio::test]
    async fn numeric_with_varlen_sized_precision() {
        test_round_trip(
            TypeInfoInner::VarLenSizedPrecision {
                ty: VarLenType::Numericn,
                size: 17,
                precision: 18,
                scale: 0,
            },
            ColumnData::Numeric(Some(Numeric::new_with_scale(23, 0))),
        )
        .await;
    }

    #[tokio::test]
    async fn none_numeric_with_varlen_sized_precision() {
        test_round_trip(
            TypeInfoInner::VarLenSizedPrecision {
                ty: VarLenType::Numericn,
                size: 17,
                precision: 18,
                scale: 0,
            },
            ColumnData::Numeric(None),
        )
        .await;
    }

    #[tokio::test]
    async fn string_with_varlen_bigchar() {
        test_round_trip(
            TypeInfoInner::VarLenSized(VarLenContext::new(
                VarLenType::BigChar,
                40,
                Some(Collation::new(13632521, 52)),
            )),
            ColumnData::String(Some("aaa".into())),
        )
        .await;
    }

    #[tokio::test]
    async fn none_string_with_varlen_bigchar() {
        test_round_trip(
            TypeInfoInner::VarLenSized(VarLenContext::new(
                VarLenType::BigChar,
                40,
                Some(Collation::new(13632521, 52)),
            )),
            ColumnData::String(None),
        )
        .await;
    }

    #[tokio::test]
    async fn string_with_varlen_bigvarchar() {
        test_round_trip(
            TypeInfoInner::VarLenSized(VarLenContext::new(
                VarLenType::BigVarChar,
                40,
                Some(Collation::new(13632521, 52)),
            )),
            ColumnData::String(Some("aaa".into())),
        )
        .await;
    }

    #[tokio::test]
    async fn none_string_with_varlen_bigvarchar() {
        test_round_trip(
            TypeInfoInner::VarLenSized(VarLenContext::new(
                VarLenType::BigVarChar,
                40,
                Some(Collation::new(13632521, 52)),
            )),
            ColumnData::String(None),
        )
        .await;
    }

    #[tokio::test]
    async fn string_with_varlen_nvarchar() {
        test_round_trip(
            TypeInfoInner::VarLenSized(VarLenContext::new(
                VarLenType::NVarchar,
                40,
                Some(Collation::new(13632521, 52)),
            )),
            ColumnData::String(Some("hhh".into())),
        )
        .await;
    }

    #[tokio::test]
    async fn none_string_with_varlen_nvarchar() {
        test_round_trip(
            TypeInfoInner::VarLenSized(VarLenContext::new(
                VarLenType::NVarchar,
                40,
                Some(Collation::new(13632521, 52)),
            )),
            ColumnData::String(None),
        )
        .await;
    }

    #[tokio::test]
    async fn string_with_varlen_nchar() {
        test_round_trip(
            TypeInfoInner::VarLenSized(VarLenContext::new(
                VarLenType::NChar,
                40,
                Some(Collation::new(13632521, 52)),
            )),
            ColumnData::String(Some("hhh".into())),
        )
        .await;
    }

    #[tokio::test]
    async fn long_string_with_varlen_nchar() {
        test_round_trip(
            TypeInfoInner::VarLenSized(VarLenContext::new(
                VarLenType::NChar,
                0x8ffff,
                Some(Collation::new(13632521, 52)),
            )),
            ColumnData::String(Some("hhh".into())),
        )
        .await;
    }

    #[tokio::test]
    async fn none_string_with_varlen_nchar() {
        test_round_trip(
            TypeInfoInner::VarLenSized(VarLenContext::new(
                VarLenType::NChar,
                40,
                Some(Collation::new(13632521, 52)),
            )),
            ColumnData::String(None),
        )
        .await;
    }

    #[tokio::test]
    async fn binary_with_varlen_bigbinary() {
        test_round_trip(
            TypeInfoInner::VarLenSized(VarLenContext::new(VarLenType::BigBinary, 40, None)),
            ColumnData::Binary(Some(b"aaa".as_slice().into())),
        )
        .await;
    }

    #[tokio::test]
    async fn none_binary_with_varlen_bigbinary() {
        test_round_trip(
            TypeInfoInner::VarLenSized(VarLenContext::new(VarLenType::BigBinary, 40, None)),
            ColumnData::Binary(None),
        )
        .await;
    }

    #[tokio::test]
    async fn binary_with_varlen_bigvarbin() {
        test_round_trip(
            TypeInfoInner::VarLenSized(VarLenContext::new(VarLenType::BigVarBin, 40, None)),
            ColumnData::Binary(Some(b"aaa".as_slice().into())),
        )
        .await;
    }

    #[tokio::test]
    async fn none_binary_with_varlen_bigvarbin() {
        test_round_trip(
            TypeInfoInner::VarLenSized(VarLenContext::new(VarLenType::BigVarBin, 40, None)),
            ColumnData::Binary(None),
        )
        .await;
    }

    #[tokio::test]
    async fn datetime_with_varlen_datetimen() {
        test_round_trip(
            TypeInfoInner::VarLenSized(VarLenContext::new(VarLenType::Datetimen, 8, None)),
            ColumnData::DateTime(Some(DateTime::new(200, 3000))),
        )
        .await;
    }

    // this is inconsistent: decode will decode any None datetime to smalldatetime, ignoring size
    // but it's non-critical, so let it be here
    #[tokio::test]
    async fn none_datetime_with_varlen_datetimen() {
        test_round_trip(
            TypeInfoInner::VarLenSized(VarLenContext::new(VarLenType::Datetimen, 8, None)),
            ColumnData::DateTime(None),
        )
        .await;
    }

    #[tokio::test]
    async fn datetime_with_fixedlen_datetime() {
        test_round_trip(
            TypeInfoInner::FixedLen(FixedLenType::Datetime),
            ColumnData::DateTime(Some(DateTime::new(200, 3000))),
        )
        .await;
    }

    #[tokio::test]
    async fn smalldatetime_with_varlen_datetimen() {
        test_round_trip(
            TypeInfoInner::VarLenSized(VarLenContext::new(VarLenType::Datetimen, 4, None)),
            ColumnData::SmallDateTime(Some(SmallDateTime::new(200, 3000))),
        )
        .await;
    }

    #[tokio::test]
    async fn none_smalldatetime_with_varlen_datetimen() {
        test_round_trip(
            TypeInfoInner::VarLenSized(VarLenContext::new(VarLenType::Datetimen, 4, None)),
            ColumnData::SmallDateTime(None),
        )
        .await;
    }

    #[tokio::test]
    async fn smalldatetime_with_fixedlen_datetime4() {
        test_round_trip(
            TypeInfoInner::FixedLen(FixedLenType::Datetime4),
            ColumnData::SmallDateTime(Some(SmallDateTime::new(200, 3000))),
        )
        .await;
    }

    #[cfg(feature = "tds73")]
    #[tokio::test]
    async fn date_with_varlen_daten() {
        test_round_trip(
            TypeInfoInner::VarLenSized(VarLenContext::new(VarLenType::Daten, 3, None)),
            ColumnData::Date(Some(Date::new(200))),
        )
        .await;
    }

    #[cfg(feature = "tds73")]
    #[tokio::test]
    async fn none_date_with_varlen_daten() {
        test_round_trip(
            TypeInfoInner::VarLenSized(VarLenContext::new(VarLenType::Daten, 3, None)),
            ColumnData::Date(None),
        )
        .await;
    }

    #[cfg(feature = "tds73")]
    #[tokio::test]
    async fn time_with_varlen_timen() {
        test_round_trip(
            TypeInfoInner::VarLenSized(VarLenContext::new(VarLenType::Timen, 7, None)),
            ColumnData::Time(Some(Time::new(55, 7))),
        )
        .await;
    }

    #[cfg(feature = "tds73")]
    #[tokio::test]
    async fn none_time_with_varlen_timen() {
        test_round_trip(
            TypeInfoInner::VarLenSized(VarLenContext::new(VarLenType::Timen, 7, None)),
            ColumnData::Time(None),
        )
        .await;
    }

    #[cfg(feature = "tds73")]
    #[tokio::test]
    async fn datetime2_with_varlen_datetime2() {
        test_round_trip(
            TypeInfoInner::VarLenSized(VarLenContext::new(VarLenType::Datetime2, 7, None)),
            ColumnData::DateTime2(Some(DateTime2::new(Date::new(55), Time::new(222, 7)))),
        )
        .await;
    }

    #[cfg(feature = "tds73")]
    #[tokio::test]
    async fn none_datetime2_with_varlen_datetime2() {
        test_round_trip(
            TypeInfoInner::VarLenSized(VarLenContext::new(VarLenType::Datetime2, 7, None)),
            ColumnData::DateTime2(None),
        )
        .await;
    }

    #[cfg(feature = "tds73")]
    #[tokio::test]
    async fn datetimeoffset_with_varlen_datetimeoffsetn() {
        test_round_trip(
            TypeInfoInner::VarLenSized(VarLenContext::new(VarLenType::DatetimeOffsetn, 7, None)),
            ColumnData::DateTimeOffset(Some(DateTimeOffset::new(
                DateTime2::new(Date::new(55), Time::new(222, 7)),
                -8,
            ))),
        )
        .await;
    }

    #[cfg(feature = "tds73")]
    #[tokio::test]
    async fn none_datetimeoffset_with_varlen_datetimeoffsetn() {
        test_round_trip(
            TypeInfoInner::VarLenSized(VarLenContext::new(VarLenType::DatetimeOffsetn, 7, None)),
            ColumnData::DateTimeOffset(None),
        )
        .await;
    }

    #[cfg(feature = "tds73")]
    #[tokio::test]
    async fn xml_with_xml() {
        test_round_trip(
            TypeInfoInner::Xml {
                schema: None,
                size: 0xfffffffffffffffe_usize,
            },
            ColumnData::Xml(Some(Cow::Owned(XmlData::new("<a>ddd</a>")))),
        )
        .await;
    }

    #[cfg(feature = "tds73")]
    #[tokio::test]
    async fn none_xml_with_xml() {
        test_round_trip(
            TypeInfoInner::Xml {
                schema: None,
                size: 0xfffffffffffffffe_usize,
            },
            ColumnData::Xml(None),
        )
        .await;
    }

    #[tokio::test]
    async fn invalid_type_fails() {
        let data = vec![
            (
                TypeInfoInner::VarLenSized(VarLenContext::new(VarLenType::Daten, 4, None)),
                ColumnData::I32(Some(42)),
            ),
            (
                TypeInfoInner::VarLenSized(VarLenContext::new(VarLenType::Daten, 4, None)),
                ColumnData::I32(None),
            ),
            (
                TypeInfoInner::FixedLen(FixedLenType::Int4),
                ColumnData::I32(None),
            ),
        ];

        for (inner, d) in data {
            let mut buf = BytesMut::new();
            let ti = TypeInfo { inner };

            let err = d.encode(&mut buf, &ti).expect_err("encode should fail");

            if let Error::BulkInput(_) = err {
            } else {
                assert!(false);
            }
        }
    }
}

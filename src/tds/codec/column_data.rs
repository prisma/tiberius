mod binary;
mod bit;
mod buf;
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
    SqlReadBytes, VarLenContext,
};
pub(crate) use buf::BufColumnData;
use bytes::{BufMut, BytesMut};
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
            // ColumnData::Bit(Some(val)) => {
            //     if dst.write_headers {
            //         let header = [&[VarLenType::Bitn as u8, 1, 1][..]].concat();
            //         dst.extend_from_slice(&header);
            //     }
            //
            //     dst.put_u8(val as u8);
            // }
            // ColumnData::U8(Some(val)) => {
            //     if dst.write_headers {
            //         let header = [&[VarLenType::Intn as u8, 1, 1][..]].concat();
            //         dst.extend_from_slice(&header);
            //     }
            //
            //     dst.put_u8(val);
            // }
            // ColumnData::I16(Some(val)) => {
            //     if dst.write_headers {
            //         let header = [&[VarLenType::Intn as u8, 2, 2][..]].concat();
            //         dst.extend_from_slice(&header);
            //     }
            //
            //     dst.put_i16_le(val);
            // }
            (ColumnData::I32(Some(val)), TypeInfoInner::FixedLen(_)) => {
                dst.put_i32_le(val);
            }
            (ColumnData::I32(Some(val)), TypeInfoInner::VarLenSized(vlc))
                if vlc.r#type() == VarLenType::Intn =>
            {
                dst.put_u8(4);
                dst.put_i32_le(val);
            }
            (ColumnData::I32(None), TypeInfoInner::VarLenSized(vlc))
                if vlc.r#type() == VarLenType::Intn =>
            {
                dst.put_u8(0);
            }
            // ColumnData::I64(Some(val)) => {
            //     if dst.write_headers {
            //         let header = [&[VarLenType::Intn as u8, 8, 8][..]].concat();
            //         dst.extend_from_slice(&header);
            //     }
            //
            //     dst.put_i64_le(val);
            // }
            // ColumnData::F32(Some(val)) => {
            //     if dst.write_headers {
            //         let header = [&[VarLenType::Floatn as u8, 4, 4][..]].concat();
            //         dst.extend_from_slice(&header);
            //     }
            //
            //     dst.put_f32_le(val);
            // }
            // ColumnData::F64(Some(val)) => {
            //     if dst.write_headers {
            //         let header = [&[VarLenType::Floatn as u8, 8, 8][..]].concat();
            //         dst.extend_from_slice(&header);
            //     }
            //
            //     dst.put_f64_le(val);
            // }
            // ColumnData::Guid(Some(uuid)) => {
            //     if dst.write_headers {
            //         let header = [&[VarLenType::Guid as u8, 16, 16][..]].concat();
            //         dst.extend_from_slice(&header);
            //     }
            //
            //     let mut data = *uuid.as_bytes();
            //     super::guid::reorder_bytes(&mut data);
            //     dst.extend_from_slice(&data);
            // }
            // ColumnData::String(Some(ref s)) if s.len() <= 4000 => {
            //     if dst.write_headers {
            //         dst.put_u8(VarLenType::NVarchar as u8);
            //         dst.put_u16_le(8000);
            //         dst.extend_from_slice(&[0u8; 5][..]);
            //     }
            //
            //     let mut length = 0u16;
            //     let len_pos = dst.len();
            //
            //     dst.put_u16_le(length);
            //
            //     for chr in s.encode_utf16() {
            //         length += 1;
            //         dst.put_u16_le(chr);
            //     }
            //
            //     let dst: &mut [u8] = dst.borrow_mut();
            //     let bytes = (length * 2).to_le_bytes(); // u16, two bytes
            //
            //     for (i, byte) in bytes.iter().enumerate() {
            //         dst[len_pos + i] = *byte;
            //     }
            // }
            // ColumnData::String(Some(ref s)) => {
            //     if dst.write_headers {
            //         // length: 0xffff and raw collation
            //         dst.put_u8(VarLenType::NVarchar as u8);
            //         dst.extend_from_slice(&[0xff_u8; 2][..]);
            //         dst.extend_from_slice(&[0u8; 5][..]);
            //
            //         // we cannot cheaply predetermine the length of the UCS2 string beforehand
            //         // (2 * bytes(UTF8) is not always right) - so just let the SQL server handle it
            //         dst.put_u64_le(0xfffffffffffffffe_u64);
            //     }
            //
            //     // Write the varchar length
            //     let mut length = 0u32;
            //     let len_pos = dst.len();
            //
            //     dst.put_u32_le(length);
            //
            //     for chr in s.encode_utf16() {
            //         length += 1;
            //         dst.put_u16_le(chr);
            //     }
            //
            //     // PLP_TERMINATOR
            //     dst.put_u32_le(0);
            //
            //     let dst: &mut [u8] = dst.borrow_mut();
            //     let bytes = (length * 2).to_le_bytes(); // u32, four bytes
            //
            //     for (i, byte) in bytes.iter().enumerate() {
            //         dst[len_pos + i] = *byte;
            //     }
            // }
            // ColumnData::Binary(Some(bytes)) if bytes.len() <= 8000 => {
            //     if dst.write_headers {
            //         dst.put_u8(VarLenType::BigVarBin as u8);
            //         dst.put_u16_le(8000);
            //     }
            //
            //     dst.put_u16_le(bytes.len() as u16);
            //     dst.extend(bytes.into_owned());
            // }
            // ColumnData::Binary(Some(bytes)) => {
            //     if dst.write_headers {
            //         dst.put_u8(VarLenType::BigVarBin as u8);
            //         // Max length
            //         dst.put_u16_le(0xffff_u16);
            //         // Also the length is unknown
            //         dst.put_u64_le(0xfffffffffffffffe_u64);
            //     }
            //
            //     // We'll write in one chunk, length is the whole bytes length
            //     dst.put_u32_le(bytes.len() as u32);
            //     // Payload
            //     dst.extend(bytes.into_owned());
            //     // PLP_TERMINATOR
            //     dst.put_u32_le(0);
            // }
            // ColumnData::DateTime(Some(dt)) => {
            //     if dst.write_headers {
            //         dst.extend_from_slice(&[VarLenType::Datetimen as u8, 8, 8]);
            //     }
            //
            //     dt.encode(&mut *dst)?;
            // }
            // ColumnData::SmallDateTime(Some(dt)) => {
            //     if dst.write_headers {
            //         dst.extend_from_slice(&[VarLenType::Datetimen as u8, 4, 4]);
            //     }
            //
            //     dt.encode(&mut *dst)?;
            // }
            // #[cfg(feature = "tds73")]
            // ColumnData::Time(Some(time)) => {
            //     if dst.write_headers {
            //         dst.extend_from_slice(&[VarLenType::Timen as u8, time.scale(), time.len()?]);
            //     }
            //
            //     time.encode(&mut *dst)?;
            // }
            // #[cfg(feature = "tds73")]
            // ColumnData::Date(Some(date)) => {
            //     if dst.write_headers {
            //         dst.extend_from_slice(&[VarLenType::Daten as u8, 3]);
            //     }
            //
            //     date.encode(&mut *dst)?;
            // }
            // #[cfg(feature = "tds73")]
            // ColumnData::DateTime2(Some(dt)) => {
            //     if dst.write_headers {
            //         let len = dt.time().len()? + 3;
            //         dst.extend_from_slice(&[VarLenType::Datetime2 as u8, dt.time().scale(), len]);
            //     }
            //
            //     dt.encode(&mut *dst)?;
            // }
            // #[cfg(feature = "tds73")]
            // ColumnData::DateTimeOffset(Some(dto)) => {
            //     if dst.write_headers {
            //         let headers = &[
            //             VarLenType::DatetimeOffsetn as u8,
            //             dto.datetime2().time().scale(),
            //             dto.datetime2().time().len()? + 5,
            //         ];
            //
            //         dst.extend_from_slice(headers);
            //     }
            //
            //     dto.encode(&mut *dst)?;
            // }
            // ColumnData::Xml(Some(xml)) => {
            //     if dst.write_headers {
            //         dst.put_u8(VarLenType::Xml as u8);
            //     }
            //     xml.into_owned().encode(&mut *dst)?;
            // }
            // ColumnData::Numeric(Some(num)) => {
            //     if dst.write_headers {
            //         let headers = &[
            //             VarLenType::Numericn as u8,
            //             num.len(),
            //             num.precision(),
            //             num.scale(),
            //         ];
            //
            //         dst.extend_from_slice(headers);
            //     }
            //
            //     num.encode(&mut *dst)?;
            // }
            (v, ref ti) => Err(crate::Error::BulkInput(
                format!("invalid data type, expecting {:?} but found {:?}", ti, v).into(),
            ))?,
        }

        Ok(())
    }
}

impl<'a> Encode<BufColumnData<'a>> for ColumnData<'a> {
    fn encode(self, dst: &mut BufColumnData<'a>) -> crate::Result<()> {
        match self {
            ColumnData::Bit(Some(val)) => {
                if dst.write_headers {
                    let header = [&[VarLenType::Bitn as u8, 1, 1][..]].concat();
                    dst.extend_from_slice(&header);
                }

                dst.put_u8(val as u8);
            }
            ColumnData::U8(Some(val)) => {
                if dst.write_headers {
                    let header = [&[VarLenType::Intn as u8, 1, 1][..]].concat();
                    dst.extend_from_slice(&header);
                }

                dst.put_u8(val);
            }
            ColumnData::I16(Some(val)) => {
                if dst.write_headers {
                    let header = [&[VarLenType::Intn as u8, 2, 2][..]].concat();
                    dst.extend_from_slice(&header);
                }

                dst.put_i16_le(val);
            }
            ColumnData::I32(Some(val)) => {
                if dst.write_headers {
                    let header = [&[VarLenType::Intn as u8, 4, 4][..]].concat();
                    dst.extend_from_slice(&header);
                }

                dst.put_i32_le(val);
            }
            ColumnData::I64(Some(val)) => {
                if dst.write_headers {
                    let header = [&[VarLenType::Intn as u8, 8, 8][..]].concat();
                    dst.extend_from_slice(&header);
                }

                dst.put_i64_le(val);
            }
            ColumnData::F32(Some(val)) => {
                if dst.write_headers {
                    let header = [&[VarLenType::Floatn as u8, 4, 4][..]].concat();
                    dst.extend_from_slice(&header);
                }

                dst.put_f32_le(val);
            }
            ColumnData::F64(Some(val)) => {
                if dst.write_headers {
                    let header = [&[VarLenType::Floatn as u8, 8, 8][..]].concat();
                    dst.extend_from_slice(&header);
                }

                dst.put_f64_le(val);
            }
            ColumnData::Guid(Some(uuid)) => {
                if dst.write_headers {
                    let header = [&[VarLenType::Guid as u8, 16, 16][..]].concat();
                    dst.extend_from_slice(&header);
                }

                let mut data = *uuid.as_bytes();
                super::guid::reorder_bytes(&mut data);
                dst.extend_from_slice(&data);
            }
            ColumnData::String(Some(ref s)) if s.len() <= 4000 => {
                if dst.write_headers {
                    dst.put_u8(VarLenType::NVarchar as u8);
                    dst.put_u16_le(8000);
                    dst.extend_from_slice(&[0u8; 5][..]);
                }

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
                if dst.write_headers {
                    // length: 0xffff and raw collation
                    dst.put_u8(VarLenType::NVarchar as u8);
                    dst.extend_from_slice(&[0xff_u8; 2][..]);
                    dst.extend_from_slice(&[0u8; 5][..]);

                    // we cannot cheaply predetermine the length of the UCS2 string beforehand
                    // (2 * bytes(UTF8) is not always right) - so just let the SQL server handle it
                    dst.put_u64_le(0xfffffffffffffffe_u64);
                }

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
                if dst.write_headers {
                    dst.put_u8(VarLenType::BigVarBin as u8);
                    dst.put_u16_le(8000);
                }

                dst.put_u16_le(bytes.len() as u16);
                dst.extend(bytes.into_owned());
            }
            ColumnData::Binary(Some(bytes)) => {
                if dst.write_headers {
                    dst.put_u8(VarLenType::BigVarBin as u8);
                    // Max length
                    dst.put_u16_le(0xffff_u16);
                    // Also the length is unknown
                    dst.put_u64_le(0xfffffffffffffffe_u64);
                }

                // We'll write in one chunk, length is the whole bytes length
                dst.put_u32_le(bytes.len() as u32);
                // Payload
                dst.extend(bytes.into_owned());
                // PLP_TERMINATOR
                dst.put_u32_le(0);
            }
            ColumnData::DateTime(Some(dt)) => {
                if dst.write_headers {
                    dst.extend_from_slice(&[VarLenType::Datetimen as u8, 8, 8]);
                }

                dt.encode(&mut *dst)?;
            }
            ColumnData::SmallDateTime(Some(dt)) => {
                if dst.write_headers {
                    dst.extend_from_slice(&[VarLenType::Datetimen as u8, 4, 4]);
                }

                dt.encode(&mut *dst)?;
            }
            #[cfg(feature = "tds73")]
            ColumnData::Time(Some(time)) => {
                if dst.write_headers {
                    dst.extend_from_slice(&[VarLenType::Timen as u8, time.scale(), time.len()?]);
                }

                time.encode(&mut *dst)?;
            }
            #[cfg(feature = "tds73")]
            ColumnData::Date(Some(date)) => {
                if dst.write_headers {
                    dst.extend_from_slice(&[VarLenType::Daten as u8, 3]);
                }

                date.encode(&mut *dst)?;
            }
            #[cfg(feature = "tds73")]
            ColumnData::DateTime2(Some(dt)) => {
                if dst.write_headers {
                    let len = dt.time().len()? + 3;
                    dst.extend_from_slice(&[VarLenType::Datetime2 as u8, dt.time().scale(), len]);
                }

                dt.encode(&mut *dst)?;
            }
            #[cfg(feature = "tds73")]
            ColumnData::DateTimeOffset(Some(dto)) => {
                if dst.write_headers {
                    let headers = &[
                        VarLenType::DatetimeOffsetn as u8,
                        dto.datetime2().time().scale(),
                        dto.datetime2().time().len()? + 5,
                    ];

                    dst.extend_from_slice(headers);
                }

                dto.encode(&mut *dst)?;
            }
            ColumnData::Xml(Some(xml)) => {
                if dst.write_headers {
                    dst.put_u8(VarLenType::Xml as u8);
                }
                xml.into_owned().encode(&mut *dst)?;
            }
            ColumnData::Numeric(Some(num)) => {
                if dst.write_headers {
                    let headers = &[
                        VarLenType::Numericn as u8,
                        num.len(),
                        num.precision(),
                        num.scale(),
                    ];

                    dst.extend_from_slice(headers);
                }

                num.encode(&mut *dst)?;
            }
            _ => {
                // None/null
                dst.put_u8(FixedLenType::Null as u8);
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tds::Context;
    use crate::TypeInfoInner::FixedLen;
    use crate::{Error, VarLenContext};
    use bytes::BytesMut;
    use futures::AsyncRead;
    use std::io;
    use std::io::Cursor;
    use std::pin::Pin;
    use std::task::Poll;

    struct Reader {
        buf: BytesMut,
    }

    impl AsyncRead for Reader {
        fn poll_read(
            self: Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
            buf: &mut [u8],
        ) -> Poll<std::io::Result<usize>> {
            let mut this = self.get_mut();
            let mut cursor = Cursor::new(&mut this.buf);
            let size = buf.len();

            // Got EOF before having all the data.
            if this.buf.len() < size {
                return Poll::Ready(Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "No more packets in the wire",
                )));
            }

            buf.copy_from_slice(this.buf.split_to(size).as_ref());
            Poll::Ready(Ok(size))
        }
    }

    impl SqlReadBytes for Reader {
        fn debug_buffer(&self) {
            todo!()
        }

        fn context(&self) -> &Context {
            todo!()
        }

        fn context_mut(&mut self) -> &mut Context {
            todo!()
        }
    }

    #[tokio::test]
    async fn round_trip() {
        let data = vec![
            (
                TypeInfoInner::VarLenSized(VarLenContext::new(VarLenType::Intn, 4, None)),
                ColumnData::I32(Some(42)),
            ),
            (
                TypeInfoInner::VarLenSized(VarLenContext::new(VarLenType::Intn, 4, None)),
                ColumnData::I32(None),
            ),
        ];

        for (inner, d) in data {
            let mut buf = BytesMut::new();
            let ti = TypeInfo { inner };

            d.clone()
                .encode(&mut buf, &ti)
                .expect("encode must succeed");

            let mut reader = Reader { buf };
            let nd = ColumnData::decode(&mut reader, &ti)
                .await
                .expect("decode must succeed");

            assert_eq!(nd, d)
        }
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

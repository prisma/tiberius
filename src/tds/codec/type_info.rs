use asynchronous_codec::BytesMut;
use bytes::BufMut;

use crate::{tds::Collation, xml::XmlSchema, Error, SqlReadBytes};
use std::{convert::TryFrom, sync::Arc, usize};

use super::Encode;

/// Describes a type of a column.
#[derive(Debug, Clone)]
pub struct TypeInfo {
    pub(crate) inner: TypeInfoInner,
}

/// A length of a column in bytes or characters.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TypeLength {
    /// The number of bytes (or characters) reserved in the column.
    Limited(u16),
    /// Unlimited, stored in the heap outside of the row.
    Max,
}

#[derive(Debug, Clone)]
pub(crate) enum TypeInfoInner {
    FixedLen(FixedLenType),
    VarLenSized(VarLenContext),
    VarLenSizedPrecision {
        ty: VarLenType,
        size: usize,
        precision: u8,
        scale: u8,
    },
    Xml {
        schema: Option<Arc<XmlSchema>>,
        size: usize,
    },
}

#[derive(Clone, Debug, Copy)]
pub struct VarLenContext {
    r#type: VarLenType,
    len: usize,
    collation: Option<Collation>,
}

impl VarLenContext {
    pub fn new(r#type: VarLenType, len: usize, collation: Option<Collation>) -> Self {
        Self {
            r#type,
            len,
            collation,
        }
    }

    /// Get the var len context's r#type.
    pub fn r#type(&self) -> VarLenType {
        self.r#type
    }

    /// Get the var len context's len.
    pub fn len(&self) -> usize {
        self.len
    }

    /// Get the var len context's collation.
    pub fn collation(&self) -> Option<Collation> {
        self.collation
    }
}

impl Encode<BytesMut> for VarLenContext {
    fn encode(self, dst: &mut BytesMut) -> crate::Result<()> {
        dst.put_u8(self.r#type() as u8);

        // length
        match self.r#type {
            #[cfg(feature = "tds73")]
            VarLenType::Daten
            | VarLenType::Timen
            | VarLenType::DatetimeOffsetn
            | VarLenType::Datetime2 => {
                dst.put_u8(self.len() as u8);
            }
            VarLenType::Bitn
            | VarLenType::Intn
            | VarLenType::Floatn
            | VarLenType::Decimaln
            | VarLenType::Numericn
            | VarLenType::Guid
            | VarLenType::Money
            | VarLenType::Datetimen => {
                dst.put_u8(self.len() as u8);
            }
            VarLenType::NChar
            | VarLenType::BigChar
            | VarLenType::NVarchar
            | VarLenType::BigVarChar
            | VarLenType::BigBinary
            | VarLenType::BigVarBin => {
                dst.put_u16_le(self.len() as u16);
            }
            VarLenType::Image | VarLenType::Text | VarLenType::NText => {
                dst.put_u32_le(self.len() as u32);
            }
            VarLenType::Xml => (),
            typ => todo!("encoding {:?} is not supported yet", typ),
        }

        if let Some(collation) = self.collation() {
            dst.put_u32_le(collation.info);
            dst.put_u8(collation.sort_id);
        }

        Ok(())
    }
}

uint_enum! {
    #[repr(u8)]
    pub enum FixedLenType {
        Null = 0x1F,
        Int1 = 0x30,
        Bit = 0x32,
        Int2 = 0x34,
        Int4 = 0x38,
        Datetime4 = 0x3A,
        Float4 = 0x3B,
        Money = 0x3C,
        Datetime = 0x3D,
        Float8 = 0x3E,
        Money4 = 0x7A,
        Int8 = 0x7F,
    }
}

#[cfg(not(feature = "tds73"))]
uint_enum! {
    /// 2.2.5.4.2
    #[repr(u8)]
    pub enum VarLenType {
        Guid = 0x24,
        Intn = 0x26,
        Bitn = 0x68,
        Decimaln = 0x6A,
        Numericn = 0x6C,
        Floatn = 0x6D,
        Money = 0x6E,
        Datetimen = 0x6F,
        BigVarBin = 0xA5,
        BigVarChar = 0xA7,
        BigBinary = 0xAD,
        BigChar = 0xAF,
        NVarchar = 0xE7,
        NChar = 0xEF,
        Xml = 0xF1,
        // not supported yet
        Udt = 0xF0,
        Text = 0x23,
        Image = 0x22,
        NText = 0x63,
        // not supported yet
        SSVariant = 0x62, // legacy types (not supported since post-7.2):
                          // Char = 0x2F,
                          // Binary = 0x2D,
                          // VarBinary = 0x25,
                          // VarChar = 0x27,
                          // Numeric = 0x3F,
                          // Decimal = 0x37
    }
}

#[cfg(feature = "tds73")]
uint_enum! {
    /// 2.2.5.4.2
    #[repr(u8)]
    pub enum VarLenType {
        Guid = 0x24,
        Intn = 0x26,
        Bitn = 0x68,
        Decimaln = 0x6A,
        Numericn = 0x6C,
        Floatn = 0x6D,
        Money = 0x6E,
        Datetimen = 0x6F,
        Daten = 0x28,
        Timen = 0x29,
        Datetime2 = 0x2A,
        DatetimeOffsetn = 0x2B,
        BigVarBin = 0xA5,
        BigVarChar = 0xA7,
        BigBinary = 0xAD,
        BigChar = 0xAF,
        NVarchar = 0xE7,
        NChar = 0xEF,
        Xml = 0xF1,
        // not supported yet
        Udt = 0xF0,
        Text = 0x23,
        Image = 0x22,
        NText = 0x63,
        // not supported yet
        SSVariant = 0x62, // legacy types (not supported since post-7.2):
                          // Char = 0x2F,
                          // Binary = 0x2D,
                          // VarBinary = 0x25,
                          // VarChar = 0x27,
                          // Numeric = 0x3F,
                          // Decimal = 0x37
    }
}

impl Encode<BytesMut> for TypeInfo {
    fn encode(self, dst: &mut BytesMut) -> crate::Result<()> {
        match dbg!(self.inner) {
            TypeInfoInner::FixedLen(ty) => {
                dst.put_u8(ty as u8);
            }
            TypeInfoInner::VarLenSized(ctx) => ctx.encode(dst)?,
            TypeInfoInner::VarLenSizedPrecision {
                ty,
                size,
                precision,
                scale,
            } => {
                dst.put_u8(ty as u8);
                dst.put_u8(size as u8);
                dst.put_u8(precision as u8);
                dst.put_u8(scale as u8);
            }
            TypeInfoInner::Xml { .. } => {
                unreachable!()
            }
        }

        Ok(())
    }
}

impl TypeInfo {
    /// A bit, either zero or one.
    pub fn bit() -> Self {
        Self::fixed(FixedLenType::Bit)
    }

    /// 8-bit integer, unsigned.
    pub fn tinyint() -> Self {
        Self::fixed(FixedLenType::Int1)
    }

    /// 16-bit integer, signed.
    pub fn smallint() -> Self {
        Self::fixed(FixedLenType::Int2)
    }

    /// 32-bit integer, signed.
    pub fn int() -> Self {
        Self::fixed(FixedLenType::Int4)
    }

    /// 64-bit integer, signed.
    pub fn bigint() -> Self {
        Self::fixed(FixedLenType::Int8)
    }

    /// 32-bit floating point number.
    pub fn real() -> Self {
        Self::fixed(FixedLenType::Float4)
    }

    /// 64-bit floating point number.
    pub fn float() -> Self {
        Self::fixed(FixedLenType::Float8)
    }

    /// 32-bit money type.
    pub fn smallmoney() -> Self {
        Self::fixed(FixedLenType::Money4)
    }

    /// 64-bit money type.
    pub fn money() -> Self {
        Self::fixed(FixedLenType::Money)
    }

    /// A small DateTime value.
    pub fn smalldatetime() -> Self {
        Self::fixed(FixedLenType::Datetime4)
    }

    /// A datetime value.
    pub fn datetime() -> Self {
        Self::fixed(FixedLenType::Datetime)
    }

    /// A datetime2 value.
    #[cfg(feature = "tds73")]
    pub fn datetime2() -> Self {
        Self::varlen(VarLenType::Datetime2, 8, None)
    }

    /// A uniqueidentifier value.
    pub fn guid() -> Self {
        Self::varlen(VarLenType::Guid, 16, None)
    }

    /// A date value.
    #[cfg(feature = "tds73")]
    pub fn date() -> Self {
        Self::varlen(VarLenType::Daten, 3, None)
    }

    /// A time value.
    #[cfg(feature = "tds73")]
    pub fn time() -> Self {
        Self::varlen(VarLenType::Timen, 5, None)
    }

    /// A time value.
    #[cfg(feature = "tds73")]
    pub fn datetimeoffset() -> Self {
        Self::varlen(VarLenType::DatetimeOffsetn, 10, None)
    }

    /// A variable binary value. If length is limited and larger than 8000
    /// bytes, the `MAX` variant is used instead.
    pub fn varbinary(length: TypeLength) -> Self {
        let length = match length {
            TypeLength::Limited(n) if n <= 8000 => n,
            _ => u16::MAX,
        };

        Self::varlen(VarLenType::BigVarBin, length as usize, None)
    }

    /// A binary value.
    ///
    /// # Panics
    ///
    /// - If length is more than 8000 bytes.
    pub fn binary(length: u16) -> Self {
        assert!(length <= 8000);
        Self::varlen(VarLenType::BigBinary, length as usize, None)
    }

    /// A variable string value. If length is limited and larger than 8000
    /// characters, the `MAX` variant is used instead.
    pub fn varchar(length: TypeLength) -> Self {
        let length = match length {
            TypeLength::Limited(n) if n <= 8000 => n,
            _ => u16::MAX,
        };

        Self::varlen(VarLenType::BigVarChar, length as usize, None)
    }

    /// A variable UTF-16 string value. If length is limited and larger than
    /// 4000 characters, the `MAX` variant is used instead.
    pub fn nvarchar(length: TypeLength) -> Self {
        let length = match length {
            TypeLength::Limited(n) if n <= 4000 => n,
            _ => u16::MAX,
        };

        Self::varlen(VarLenType::BigVarChar, length as usize, None)
    }

    /// A constant-size string value.
    ///
    /// # Panics
    ///
    /// - If length is more than 8000 characters.
    pub fn char(length: u16) -> Self {
        assert!(length <= 8000);
        Self::varlen(VarLenType::BigChar, length as usize, None)
    }

    /// A constant-size UTF-16 string value.
    ///
    /// # Panics
    ///
    /// - If length is more than 4000 characters.
    pub fn nchar(length: u16) -> Self {
        assert!(length <= 4000);
        Self::varlen(VarLenType::NChar, length as usize, None)
    }

    /// A (deprecated) heap-allocated text storage.
    pub fn text() -> Self {
        Self::varlen(VarLenType::Text, u32::MAX as usize, None)
    }

    /// A (deprecated) heap-allocated UTF-16 text storage.
    pub fn ntext() -> Self {
        Self::varlen(VarLenType::NText, u32::MAX as usize, None)
    }

    /// A (deprecated) heap-allocated binary storage.
    pub fn image() -> Self {
        Self::varlen(VarLenType::Image, u32::MAX as usize, None)
    }

    /// Numeric data types that have fixed precision and scale. Decimal and
    /// numeric are synonyms and can be used interchangeably.
    pub fn decimal(precision: u8, scale: u8) -> Self {
        Self::varlen_precision(VarLenType::Decimaln, precision, scale)
    }

    /// Numeric data types that have fixed precision and scale. Decimal and
    /// numeric are synonyms and can be used interchangeably.
    pub fn numeric(precision: u8, scale: u8) -> Self {
        Self::varlen_precision(VarLenType::Numericn, precision, scale)
    }

    fn varlen_precision(ty: VarLenType, precision: u8, scale: u8) -> Self {
        let size = if precision <= 9 {
            5
        } else if precision <= 19 {
            9
        } else if precision <= 28 {
            13
        } else {
            17
        };

        let inner = TypeInfoInner::VarLenSizedPrecision {
            ty,
            size,
            precision,
            scale,
        };

        Self { inner }
    }

    fn varlen(ty: VarLenType, len: usize, collation: Option<Collation>) -> Self {
        let cx = VarLenContext::new(ty, len, collation);
        let inner = TypeInfoInner::VarLenSized(cx);

        Self { inner }
    }

    fn fixed(ty: FixedLenType) -> Self {
        let inner = TypeInfoInner::FixedLen(ty);
        Self { inner }
    }

    pub(crate) async fn decode<R>(src: &mut R) -> crate::Result<Self>
    where
        R: SqlReadBytes + Unpin,
    {
        let ty = src.read_u8().await?;

        if let Ok(ty) = FixedLenType::try_from(ty) {
            let inner = TypeInfoInner::FixedLen(ty);
            return Ok(TypeInfo { inner });
        }

        match VarLenType::try_from(ty) {
            Err(()) => {
                return Err(Error::Protocol(
                    format!("invalid or unsupported column type: {:?}", ty).into(),
                ))
            }
            Ok(ty) if ty == VarLenType::Xml => {
                let has_schema = src.read_u8().await?;

                let schema = if has_schema == 1 {
                    let db_name = src.read_b_varchar().await?;
                    let owner = src.read_b_varchar().await?;
                    let collection = src.read_us_varchar().await?;

                    Some(Arc::new(XmlSchema::new(db_name, owner, collection)))
                } else {
                    None
                };

                let inner = TypeInfoInner::Xml {
                    schema,
                    size: 0xfffffffffffffffe_usize,
                };

                Ok(TypeInfo { inner })
            }
            Ok(ty) => {
                let len = match ty {
                    #[cfg(feature = "tds73")]
                    VarLenType::Timen | VarLenType::DatetimeOffsetn | VarLenType::Datetime2 => {
                        src.read_u8().await? as usize
                    }
                    #[cfg(feature = "tds73")]
                    VarLenType::Daten => 3,
                    VarLenType::Bitn
                    | VarLenType::Intn
                    | VarLenType::Floatn
                    | VarLenType::Decimaln
                    | VarLenType::Numericn
                    | VarLenType::Guid
                    | VarLenType::Money
                    | VarLenType::Datetimen => src.read_u8().await? as usize,
                    VarLenType::NChar
                    | VarLenType::BigChar
                    | VarLenType::NVarchar
                    | VarLenType::BigVarChar
                    | VarLenType::BigBinary
                    | VarLenType::BigVarBin => src.read_u16_le().await? as usize,
                    VarLenType::Image | VarLenType::Text | VarLenType::NText => {
                        src.read_u32_le().await? as usize
                    }
                    _ => todo!("not yet implemented for {:?}", ty),
                };

                let collation = match ty {
                    VarLenType::NText
                    | VarLenType::Text
                    | VarLenType::BigChar
                    | VarLenType::NChar
                    | VarLenType::NVarchar
                    | VarLenType::BigVarChar => {
                        let info = src.read_u32_le().await?;
                        let sort_id = src.read_u8().await?;

                        Some(Collation::new(info, sort_id))
                    }
                    _ => None,
                };

                let vty = match ty {
                    VarLenType::Decimaln | VarLenType::Numericn => {
                        let precision = src.read_u8().await?;
                        let scale = src.read_u8().await?;

                        let inner = TypeInfoInner::VarLenSizedPrecision {
                            size: len,
                            ty,
                            precision,
                            scale,
                        };

                        TypeInfo { inner }
                    }
                    _ => {
                        let cx = VarLenContext::new(ty, len, collation);
                        let inner = TypeInfoInner::VarLenSized(cx);

                        TypeInfo { inner }
                    }
                };

                Ok(vty)
            }
        }
    }
}

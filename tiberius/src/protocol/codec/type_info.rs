use super::{Decode, ReadTyMode};
use crate::{protocol::types::Collation, uint_enum, Error};
use bytes::{Buf, BytesMut};
use std::convert::TryFrom;

#[derive(Debug)]
pub enum TypeInfo {
    FixedLen(FixedLenType),
    VarLenSized(VarLenType, usize, Option<Collation>),
    VarLenSizedPrecision {
        ty: VarLenType,
        size: usize,
        precision: u8,
        scale: u8,
    },
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

impl FixedLenType {
    pub fn len(&self) -> usize {
        match self {
            Self::Null => 0,
            Self::Int1 => 1,
            Self::Bit => 1,
            Self::Int2 => 2,
            Self::Int4 => 4,
            Self::Datetime4 => 4,
            Self::Float4 => 4,
            Self::Money => 8,
            Self::Datetime => 8,
            Self::Float8 => 8,
            Self::Money4 => 4,
            Self::Int8 => 8,
        }
    }
}

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
        /// introduced in TDS 7.3
        Daten = 0x28,
        /// introduced in TDS 7.3
        Timen = 0x29,
        /// introduced in TDS 7.3
        Datetime2 = 0x2A,
        /// introduced in TDS 7.3
        DatetimeOffsetn = 0x2B,
        BigVarBin = 0xA5,
        BigVarChar = 0xA7,
        BigBinary = 0xAD,
        BigChar = 0xAF,
        NVarchar = 0xE7,
        NChar = 0xEF,
        // not supported yet
        Xml = 0xF1,
        // not supported yet
        Udt = 0xF0,
        Text = 0x23,
        Image = 0x22,
        NText = 0x63,
        // not supported yet
        SSVariant = 0x62, // legacy types (not supported since post-7.2):
                        // Char = 0x2F,
                        // VarChar = 0x27,
                        // Binary = 0x2D,
                        // VarBinary = 0x25,
                        // Numeric = 0x3F,
                        // Decimal = 0x37
    }
}

impl VarLenType {
    pub fn get_size(&self, max_len: usize, mut buf: &[u8]) -> usize {
        use VarLenType::*;

        match self {
            Bitn | Intn | Floatn | Guid | Money => buf.get_u8() as usize + 1, // including size
            NVarchar | BigVarChar | BigBinary => match ReadTyMode::auto(max_len) {
                ReadTyMode::FixedSize(_) => buf.get_u16_le() as usize + 2, // + size
                ReadTyMode::Plp => buf.get_u64_le() as usize + 8,          // + size
            },
            NChar => buf.get_u16_le() as usize + 2, // + size
            VarLenType::Text => {
                buf.advance(17); // skip pointers

                buf.get_i32_le(); // days (+ 4)
                buf.get_u32_le(); // second fractions (+ 4)

                let text_len = buf.get_u32_le() as usize;

                // ptr len byte, ptr len, days, seconds, text_length
                17 + 4 + 4 + text_len
            }
            _ => todo!("{:?}", self),
        }
    }
}

impl Decode<BytesMut> for TypeInfo {
    fn decode(src: &mut BytesMut) -> crate::Result<Self>
    where
        Self: Sized,
    {
        let ty = src.get_u8();

        if let Ok(ty) = FixedLenType::try_from(ty) {
            return Ok(TypeInfo::FixedLen(ty));
        }

        match VarLenType::try_from(ty) {
            Err(()) => {
                return Err(Error::Protocol(
                    format!("invalid or unsupported column type: {:?}", ty).into(),
                ))
            }
            Ok(ty) => {
                let len = match ty {
                    VarLenType::Bitn
                    | VarLenType::Intn
                    | VarLenType::Floatn
                    | VarLenType::Decimaln
                    | VarLenType::Numericn
                    | VarLenType::Guid
                    | VarLenType::Money
                    | VarLenType::Datetimen
                    | VarLenType::Timen
                    | VarLenType::Datetime2 => src.get_u8() as usize,
                    VarLenType::NChar
                    | VarLenType::NVarchar
                    | VarLenType::BigVarChar
                    | VarLenType::BigBinary => src.get_u16_le() as usize,
                    VarLenType::Daten => 3,
                    VarLenType::Text => src.get_u32_le() as usize,
                    _ => unimplemented!(),
                };

                let collation = match ty {
                    VarLenType::NChar | VarLenType::NVarchar | VarLenType::BigVarChar => {
                        Some(Collation::new(src.get_u32_le(), src.get_u8()))
                    }
                    _ => None,
                };

                let vty = match ty {
                    VarLenType::Decimaln | VarLenType::Numericn => TypeInfo::VarLenSizedPrecision {
                        ty,
                        size: len,
                        precision: src.get_u8(),
                        scale: src.get_u8(),
                    },
                    _ => TypeInfo::VarLenSized(ty, len, collation),
                };

                Ok(vty)
            }
        }
    }
}

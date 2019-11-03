use std::convert::TryFrom;

use byteorder::{LittleEndian, ReadBytesExt};
use tokio::io::{AsyncRead, AsyncWrite};

use crate::protocol;
use crate::{Error, Result};

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
    fn bytes_length(&self) -> usize {
        match *self {
            VarLenType::Intn => 1,
            _ => unimplemented!()
        }
    }
}

#[derive(Debug)]
pub struct Collation {
    /// LCID ColFlags Version
    info: u32,
    /// Sortid
    sort_id: u8,
}

impl Collation {
    /// return the locale id part of the LCID (the specification here uses ambiguous terms)
    pub fn lcid(&self) -> u16 {
        (self.info & 0xffff) as u16
    }

    /*/// return an encoding for a given collation
    pub fn encoding(&self) -> Option<&'static Encoding> {
        if self.sort_id == 0 {
            collation::lcid_to_encoding(self.lcid())
        } else {
            collation::sortid_to_encoding(self.sort_id)
        }
    }*/
}

#[derive(Debug)]
pub enum TypeInfo {
    FixedLen(FixedLenType),
    VarLenSized(VarLenType, usize, Option<Collation>),
    /*VarLenSizedPrecision {
        ty: VarLenType,
        size: usize,
        precision: u8,
        scale: u8,
    },*/
}

#[derive(Debug)]
pub enum ColumnData {
    None,
    I8(i8),
    I16(i16),
    I32(i32),
    I64(i64),
    F32(f32),
    F64(f64),
    Bit(bool),
    String(String),
    /* Guid(Cow<'a, Guid>),
    DateTime(time::DateTime),
    SmallDateTime(time::SmallDateTime),
    Time(time::Time),
    Date(time::Date),
    DateTime2(time::DateTime2),
    /// owned/borrowed rust string
    String(Cow<'a, str>),
    /// a buffer string which is a reference to a buffer of a received packet
    BString(Str),
    Binary(Cow<'a, [u8]>),
    Numeric(Numeric),*/
}

impl ColumnData {
    pub async fn write_to<'a, C>(
        &self,
        ctx: &protocol::Context,
        writer: &mut protocol::PacketWriter<'a, C>,
    ) -> Result<()>
    where
        C: AsyncWrite + Unpin,
    {
        match *self {
            ColumnData::I32(val) => {
                let bytes = [&[VarLenType::Intn as u8, 4, 4][..], &val.to_le_bytes()].concat();
                writer.write_bytes(&ctx, &bytes).await?;
            }
            ColumnData::String(ref str_) if str_.len() <= 4000 => {
                let bytes = [
                    &[VarLenType::NVarchar as u8],
                    &8000u16.to_le_bytes()[..],
                    &[0u8; 5][..],
                    &(2 * str_.len() as u16).to_le_bytes(),
                ].concat();
                writer.write_bytes(&ctx, &bytes).await?;
                for codepoint in str_.encode_utf16() {
                    writer.write_bytes(&ctx, &codepoint.to_le_bytes()).await?;
                }
            }
            _ => panic!("TODO: {:?}", *self), // TODO
        }
        Ok(())
    }
}

impl<'a, C: AsyncRead + Unpin> protocol::PacketReader<'a, C> {
    pub async fn read_type_info(&mut self, ctx: &protocol::Context) -> Result<TypeInfo> {
        let ty = self.read_bytes(1).await?[0];

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
                let len = match ty.bytes_length() {
                    1 => self.read_bytes(1).await?[0] as usize,
                    _ => unimplemented!()
                };

                Ok(TypeInfo::VarLenSized(ty, len, None))
            }
        }
    }

    pub async fn read_fixed_len_type(&mut self, ctx: &protocol::Context, ty: FixedLenType) -> Result<ColumnData> {
        let ret = match ty {
            FixedLenType::Null => ColumnData::None,
            FixedLenType::Bit => ColumnData::Bit(self.read_bytes(1).await?[0] != 0),
            FixedLenType::Int1 => ColumnData::I8(self.read_bytes(1).await?.read_i8()?),
            FixedLenType::Int2 => {
                ColumnData::I16(self.read_bytes(2).await?.read_i16::<LittleEndian>()?)
            }
            FixedLenType::Int4 => {
                ColumnData::I32(self.read_bytes(4).await?.read_i32::<LittleEndian>()?)
            }
            FixedLenType::Int8 => {
                ColumnData::I64(self.read_bytes(8).await?.read_i64::<LittleEndian>()?)
            }
            FixedLenType::Float4 => {
                ColumnData::F32(self.read_bytes(4).await?.read_f32::<LittleEndian>()?)
            }
            FixedLenType::Float8 => {
                ColumnData::F64(self.read_bytes(8).await?.read_f64::<LittleEndian>()?)
            }
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

    pub async fn read_column_data(
        &mut self,
        ctx: &protocol::Context,
        meta: &protocol::tokenstream::BaseMetaDataColumn,
    ) -> Result<ColumnData> {
        let ret = match meta.ty {
            TypeInfo::FixedLen(ref fixed_ty) => self.read_fixed_len_type(&ctx, *fixed_ty).await?,
            TypeInfo::VarLenSized(ref ty, ref len, ref collation) => {
                match *ty {
                    VarLenType::Intn => {
                        assert!(collation.is_none());
                        let recv_len = self.read_bytes(1).await?[0] as usize;
                        let translated_ty = match recv_len {
                            0 => FixedLenType::Null,
                            1 => FixedLenType::Int1,
                            2 => FixedLenType::Int2,
                            4 => FixedLenType::Int4,
                            8 =>  FixedLenType::Int8,
                            _ => unimplemented!(),
                        };
                        self.read_fixed_len_type(&ctx, translated_ty).await?
                    }
                    _ => unimplemented!(),
                }
            }
        };
        Ok(ret)
    }
}

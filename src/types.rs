///! type converting, mostly translating the types received from the database into rust types
use std::io;
use byteorder::{LittleEndian, ReadBytesExt};
use tokio_core::io::Io;
use protocol::{self};
use tokens::MetaDataColumn;
use transport::TdsTransport;
use {FromUint, TdsResult, TdsError};

#[derive(Copy, Clone, Debug)]
#[repr(u8)]
pub enum FixedLenType {
    Null        = 0x1F,
    Int1        = 0x30,
    Bit         = 0x32,
    Int2        = 0x34,
    Int4        = 0x38,
    Datetime4   = 0x3A,
    Float4      = 0x3B,
    Money       = 0x3C,
    Datetime    = 0x3D,
    Float8      = 0x3E,
    Money4      = 0x7A,
    Int8        = 0x7F
}
uint_to_enum!(FixedLenType, Null, Int1, Bit, Int2, Int4, Datetime4, Float4, Money, Datetime, Float8, Money4, Int8);

#[derive(Clone, Debug)]
pub enum TypeInfo {
    FixedLen(FixedLenType),
}

#[derive(Debug)]
pub enum ColumnData {
    I32(i32),
}

impl TypeInfo {
    pub fn from_u8(ty: u8) -> Option<TypeInfo> {
        if let Some(ty) = FixedLenType::from_u8(ty) {
            return Some(TypeInfo::FixedLen(ty))
        }
        None
    }
}

impl ColumnData {
    pub fn parse<I: Io>(trans: &mut TdsTransport<I>, meta: &MetaDataColumn) -> TdsResult<ColumnData> {
        Ok(match meta.ty {
            TypeInfo::FixedLen(ref fixed_ty) => {
                match *fixed_ty {
                    FixedLenType::Int4 => ColumnData::I32(try!(trans.read_i32::<LittleEndian>())),
                    _ => panic!("unsupported fixed type decoding: {:?}", fixed_ty)
                }
            }
        })
    }
}

pub trait FromColumnData: Sized {
    fn from_column_data(data: &ColumnData) -> TdsResult<Self>;
}

impl FromColumnData for i32 {
    fn from_column_data(data: &ColumnData) -> TdsResult<i32> {
        match *data {
            ColumnData::I32(value) => Ok(value),
            //_ => Err(TdsError::Conversion("cannot interpret the given column data as an i32 value".into()))
        }
    }
}


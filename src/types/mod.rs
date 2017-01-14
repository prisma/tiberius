///! type converting, mostly translating the types received from the database into rust types
use std::borrow::Cow;
use std::cmp;
use std::fmt;
use std::io::Cursor;
use byteorder::{ByteOrder, LittleEndian, ReadBytesExt, WriteBytesExt};
use encoding::{self, DecoderTrap, Encoding};
use futures::{Async, Poll};
use tokio_core::io::Io;
use tokens::BaseMetaDataColumn;
use transport::{self, TdsBuf, TdsTransport};
use collation;
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

/// 2.2.5.4.2
#[derive(Copy, Clone, Debug)]
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
    SSVariant = 0x62
    // legacy types (not supported since post-7.2):
    // Char = 0x2F,
    // VarChar = 0x27,
    // Binary = 0x2D,
    // VarBinary = 0x25,
    // Numeric = 0x3F,
    // Decimal = 0x37,
}
uint_to_enum!(VarLenType, Guid, Intn, Bitn, Decimaln, Numericn, Floatn, Money, Datetimen, Daten, Timen, Datetime2, DatetimeOffsetn,
    BigVarBin, BigVarChar, BigBinary, BigChar, NVarchar, NChar, Xml, Udt, Text, Image, NText, SSVariant);

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

    /// return an encoding for a given collation
    pub fn encoding(&self) -> Option<&'static Encoding> {
        if self.sort_id == 0 {
            collation::lcid_to_encoding(self.lcid())
        } else {
            collation::sortid_to_encoding(self.sort_id)
        }
    }
}

#[derive(Debug)]
pub enum TypeInfo {
    FixedLen(FixedLenType),
    VarLenSized(VarLenType, usize, Option<Collation>),
    VarLenSizedPrecision {
        ty: VarLenType,
        size: usize,
        precision: u8,
        scale: u8,
    }
}

#[derive(Debug)]
pub enum ColumnData<'a> {
    None,
    I8(i8),
    I16(i16),
    I32(i32),
    I64(i64),
    F32(f32),
    F64(f64),
    Bit(bool),
    Guid(Cow<'a, Guid>),
    /// owned/borrowed rust string
    String(Cow<'a, str>),
    /// a buffer string which is a reference to a buffer of a received packet
    BString(TdsBuf),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Guid([u8; 16]);

impl Guid {
    pub fn from_bytes(input_bytes: &[u8]) -> Guid {
        assert_eq!(input_bytes.len(), 16);
        let mut bytes = [0u8; 16];
        bytes.clone_from_slice(input_bytes);
        Guid(bytes)
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

impl fmt::Display for Guid {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f,
            "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
            self.0[3], self.0[2], self.0[1], self.0[0], self.0[5], self.0[4],
            self.0[7], self.0[6], self.0[8], self.0[9], self.0[10], self.0[11],
            self.0[12], self.0[13], self.0[14], self.0[15]
        )
    }
}

impl TypeInfo {
    pub fn parse<I: Io>(trans: &mut TdsTransport<I>) -> Poll<TypeInfo, TdsError> {
        let ty = try!(trans.read_u8());
        if let Some(ty) = FixedLenType::from_u8(ty) {
            return Ok(Async::Ready(TypeInfo::FixedLen(ty)))
        }
        if let Some(ty) = VarLenType::from_u8(ty) {
            // TODO: add .size() / .has_collation() to VarLenType (?)
            let len = match ty {
                VarLenType::Bitn | VarLenType::Intn | VarLenType::Floatn | VarLenType::Decimaln | VarLenType::Numericn | VarLenType::Guid => try!(trans.read_u8()) as usize,
                VarLenType::NVarchar | VarLenType::BigVarChar => {
                    try!(trans.read_u16::<LittleEndian>()) as usize
                },
                _ => unimplemented!()
            };
            let collation = match ty {
                VarLenType::NVarchar | VarLenType::BigVarChar => {
                    Some(Collation {
                        info: try!(trans.read_u32::<LittleEndian>()),
                        sort_id: try!(trans.read_u8()),
                    })
                },
                _ => None
            };
            let vty = match ty {
                VarLenType::Decimaln | VarLenType::Numericn => TypeInfo::VarLenSizedPrecision {
                    ty: ty,
                    size: len,
                    precision: try!(trans.read_u8()),
                    scale: try!(trans.read_u8()),
                },
                _ => TypeInfo::VarLenSized(ty, len, collation),
            };
            return Ok(Async::Ready(vty))
        }
        return Err(TdsError::Protocol(format!("invalid or unsupported column type: {:?}", ty).into()))
    }
}

impl<'a> ColumnData<'a> {
    pub fn parse<I: Io>(trans: &mut TdsTransport<I>, meta: &BaseMetaDataColumn) -> Poll<ColumnData<'a>, TdsError> {
        Ok(Async::Ready(match meta.ty {
            TypeInfo::FixedLen(ref fixed_ty) => {
                match *fixed_ty {
                    FixedLenType::Int4 => ColumnData::I32(try!(trans.read_i32::<LittleEndian>())),
                    _ => panic!("unsupported fixed type decoding: {:?}", fixed_ty)
                }
            },
            TypeInfo::VarLenSized(ref ty, ref len, ref collation) => {
                match *ty {
                    VarLenType::Bitn => {
                        assert_eq!(try!(trans.read_u8()) as usize, *len);
                        ColumnData::Bit(try!(trans.read_u8()) > 0)
                    },
                    VarLenType::Intn => {
                        assert!(collation.is_none());
                        assert_eq!(try!(trans.read_u8()) as usize, *len);
                        match *len {
                            1 => ColumnData::I8(try!(trans.read_i8())),
                            2 => ColumnData::I16(try!(trans.read_i16::<LittleEndian>())),
                            4 => ColumnData::I32(try!(trans.read_i32::<LittleEndian>())),
                            8 => ColumnData::I64(try!(trans.read_i64::<LittleEndian>())),
                            _ => unimplemented!()
                        }
                    },
                    /// 2.2.5.5.1.5 IEEE754
                    VarLenType::Floatn => {
                        let len = try!(trans.read_u8());
                        match len {
                            0 => ColumnData::None,
                            4 => ColumnData::F32(try!(trans.read_f32::<LittleEndian>())),
                            8 => ColumnData::F64(try!(trans.read_f64::<LittleEndian>())),
                            _ => return Err(TdsError::Protocol(format!("floatn: length of {} is invalid", len).into()))
                        }
                    },
                    VarLenType::Guid => {
                        assert_eq!(try!(trans.read_u8()) as usize, *len);
                        let mut data = [0u8; 16];
                        try_ready!(trans.read_bytes_to(&mut data));
                        ColumnData::Guid(Cow::Owned(Guid(data)))
                    },
                    VarLenType::NVarchar => ColumnData::BString(try_ready!(trans.read_varchar::<u16>(true))),
                    VarLenType::BigVarChar => {
                        let bytes = try_ready!(trans.read_varbyte::<u16>());
                        let encoder = try!(collation.as_ref().unwrap().encoding().ok_or(TdsError::Encoding("encoding: unspported encoding".into())));
                        let str_: String = try!(encoder.decode(bytes.as_ref(), DecoderTrap::Strict).map_err(TdsError::Encoding));
                        ColumnData::String(str_.into())
                    },
                    _ => unimplemented!()
                }
            },
            TypeInfo::VarLenSizedPrecision { ty: ref ty, scale: ref scale, .. } => {
                match *ty {
                    // Our representation causes loss of information and is only a very approximate representation
                    // while decimal on the side of MSSQL is an exact representation
                    // TODO: better representation
                    VarLenType::Decimaln | VarLenType::Numericn => {
                        fn read_d128(buf: &[u8]) -> f64 {
                            let low_part = LittleEndian::read_u64(&buf[0..]) as f64;
                            if !buf[8..].iter().any(|x| *x != 0) {
                                return low_part;
                            }

                            let high_part = match buf.len() {
                                12 => LittleEndian::read_u32(&buf[8..]) as f64,
                                16 => LittleEndian::read_u64(&buf[8..]) as f64,
                                _ => unreachable!()
                            };

                            // swap high&low for big endian
                            #[cfg(target_endian = "big")]
                            let (low_part, high_part) = (high_part, low_part);

                            let high_part = high_part * (u64::max_value() as f64 + 1.0);
                            low_part + high_part
                        }

                        let len = try!(trans.read_u8());
                        let sign = match try!(trans.read_u8()) {
                            0 => -1f64,
                            1 => 1f64,
                            _ => return Err(TdsError::Protocol("decimal: invalid sign".into())),
                        };
                        let value = sign * match len {
                            5 => try!(trans.read_u32::<LittleEndian>()) as f64,
                            9 => try!(trans.read_u64::<LittleEndian>()) as f64,
                            // the following two cases are even more approximate
                            13 => {
                                let mut bytes = [0u8; 12]; //u96
                                try!(trans.read_bytes_to(&mut bytes));
                                read_d128(&bytes)
                            },
                            17 => {
                                let mut bytes = [0u8; 16]; //u128
                                try!(trans.read_bytes_to(&mut bytes));
                                read_d128(&bytes)
                            },
                            x => return Err(TdsError::Protocol(format!("decimal/numeric: invalid length of {} received", x).into()))
                        };
                        ColumnData::F64(value / 10f64.powi(*scale as i32))
                    },
                    _ => unimplemented!(),
                }
            },
        }))
    }

    pub fn serialize(&self, target: &mut Cursor<Vec<u8>>, mut last_pos: usize) -> TdsResult<Option<usize>> {
        // helper to reduce some copy&paste
        // write progressively
        macro_rules! serialize_n_helper {
            ($n_method:ident, $target:expr, $bytes:expr, $last_pos:expr, $val:expr) => {{
                LittleEndian::$n_method(&mut $bytes[3..], $val);
                let (left_bytes, written_bytes) = try!(transport::write_bytes_fragment($target, &$bytes, $last_pos));
                if left_bytes > 0 {
                    return Ok(Some($last_pos + written_bytes))
                }
            }};
        }

        match *self {
            ColumnData::Bit(ref val) => {
                let bytes = [VarLenType::Bitn as u8, 1, 1, *val as u8];
                let (left_bytes, written_bytes) = try!(transport::write_bytes_fragment(target, &bytes, last_pos));
                if left_bytes > 0 {
                    return Ok(Some(last_pos + written_bytes))
                }
            },
            ColumnData::I8(ref val) => {
                let bytes = [VarLenType::Intn as u8, 1, 1, *val as u8];
                let (left_bytes, written_bytes) = try!(transport::write_bytes_fragment(target, &bytes, last_pos));
                if left_bytes > 0 {
                    return Ok(Some(last_pos + written_bytes))
                }
            },
            ColumnData::I16(ref val) => {
                let mut bytes = [VarLenType::Intn as u8, 2, 2, 0, 0];
                serialize_n_helper!(write_i16, target, bytes, last_pos, *val)
            },
            ColumnData::I32(ref val) => {
                let mut bytes = [VarLenType::Intn as u8, 4, 4, 0, 0, 0, 0];
                serialize_n_helper!(write_i32, target, bytes, last_pos, *val)
            },
            ColumnData::I64(ref val) => {
                let mut bytes = [VarLenType::Intn as u8, 8, 8,/**/ 0, 0, 0, 0,/**/ 0, 0, 0, 0];
                serialize_n_helper!(write_i64, target, bytes, last_pos, *val)
            },
            ColumnData::F32(ref val) => {
                let mut bytes = [VarLenType::Floatn as u8, 4, 4, 0, 0, 0, 0];
                serialize_n_helper!(write_f32, target, bytes, last_pos, *val);
            },
            ColumnData::F64(ref val) => {
                let mut bytes = [VarLenType::Floatn as u8, 8, 8,/**/ 0, 0, 0, 0,/**/ 0, 0, 0, 0];
                serialize_n_helper!(write_f64, target, bytes, last_pos, *val);
            },
            ColumnData::Guid(ref guid) => {
                let mut i = 0;
                for slice in &[&[VarLenType::Guid as u8, 0x10, 0x10], guid.as_bytes()] {
                    let (left_bytes, written_bytes) = try!(transport::write_bytes_fragment(target, slice, last_pos - i));
                    last_pos += written_bytes;
                    if left_bytes > 0 {
                        return Ok(Some(last_pos))
                    }
                    i += slice.len();
                }
            },
            ColumnData::String(ref str_) => {
                // type
                if last_pos == 0 {
                    // TODO: for a certain size we need to send it as NVARCHAR(N)/NTEXT...
                    try!(target.write_u8(VarLenType::NVarchar as u8)); // pos:0
                }
                let mut state = cmp::max(last_pos, 1);
                // type length
                if state < 3 {
                    let (left_bytes, written_bytes) = try!(transport::write_u16_fragment::<LittleEndian>(target, 8000, state - 1)); // NVARCHAR(4000)
                    if left_bytes > 0 {
                        return Ok(Some(state + written_bytes))
                    }
                    state = 3;
                }
                // collation (5 bytes)
                if state < 8 {
                    // 0 = RAW COLLATION, may have side effects?
                    let collation = [0u8, 0, 0, 0, 0]; // pos: [3,4,5,6,7]
                    let (left_bytes, written_bytes) = try!(transport::write_bytes_fragment(target, &collation, state - 3));
                    if left_bytes > 0 {
                        return Ok(Some(state + written_bytes))
                    }
                    state = 8;
                }
                // body length
                if state < 10 {
                    assert!(2*str_.len() < u16::max_value() as usize);
                    let length = 2*str_.len() as u16;
                    let (left_bytes, written_bytes) = try!(transport::write_u16_fragment::<LittleEndian>(target, length, state - 8));
                    if left_bytes > 0 {
                        return Ok(Some(state + written_bytes))
                    }
                    state = 10;
                }
                // encoded string pos:>=8
                if state >= 10 {
                    let (left_bytes, written_bytes) = try!(transport::write_varchar_fragment(target, str_, state - 10));
                    if left_bytes > 0 {
                        return Ok(Some(state + written_bytes))
                    }
                }
            },
            _ => unimplemented!()
        }
        Ok(None)
    }
}

pub trait FromColumnData<'a>: Sized {
    fn from_column_data(data: &'a ColumnData) -> TdsResult<Self>;
}

pub trait ToColumnData {
    fn to_column_data(&self) -> ColumnData;
}

/// a type which can be translated as an SQL type (e.g. nvarchar) and is serializable (as `ColumnData`)
/// e.g. for usage within a ROW token
pub trait ToSql : ToColumnData {
    fn to_sql(&self) -> &'static str;
}

macro_rules! from_column_data {
    ($( $ty:ty: $($pat:pat => $val:expr),* );* ) => {
        $(
            impl<'a> FromColumnData<'a> for $ty {
                fn from_column_data(data: &'a ColumnData) -> TdsResult<Self> {
                    match *data {
                        $( $pat => Ok($val), )*
                        _ => Err(TdsError::Conversion(concat!("cannot interpret the given column data as an ", stringify!($ty), "value").into()))
                    }
                }
            }
        )*
    };
}

macro_rules! to_column_data {
    ($target:ident, $( $ty:ty => $val:expr ),* ) => {
        $(
            impl<'a> ToColumnData for $ty {
                fn to_column_data(&self) -> ColumnData {
                    let $target = self;
                    $val
                }
            }
        )*
    };
}

macro_rules! to_sql {
    ($($ty:ty => $sql:expr),*) => {
        $(
            impl<'a> ToSql for $ty {
                fn to_sql(&self) -> &'static str {
                    $sql
                }
            }
        )*
    }
}

from_column_data!(
    // integers are auto-castable on receiving
    bool:       ColumnData::Bit(val) => val;
    i8:         ColumnData::I8(val) => val;
    i16:        ColumnData::I16(val) => val;
    i32:        ColumnData::I32(val) => val;
    i64:        ColumnData::I64(val) => val;
    f32:        ColumnData::F32(val) => val;
    f64:        ColumnData::F64(val) => val;
    &'a str:    ColumnData::BString(ref buf) => buf.as_str(),
                ColumnData::String(ref buf) => buf;
    &'a Guid:   ColumnData::Guid(ref guid) => guid
);

to_column_data!(self_,
    bool =>     ColumnData::Bit(*self_),
    i8  =>      ColumnData::I8(*self_),
    i16 =>      ColumnData::I16(*self_),
    i32 =>      ColumnData::I32(*self_),
    i64 =>      ColumnData::I64(*self_),
    f32 =>      ColumnData::F32(*self_),
    f64 =>      ColumnData::F64(*self_),
    &'a str =>  ColumnData::String((*self_).into()),
    Guid     => ColumnData::Guid(Cow::Borrowed(self_)),
    &'a Guid => ColumnData::Guid(Cow::Borrowed(self_))
);

to_sql!(
    bool => "bit",
    i8  => "tinyint",
    i16 => "smallint",
    i32 => "int",
    i64 => "bigint",
    f32 => "float(24)",
    f64 => "float(53)",
    Guid =>  "uniqueidentifier",
    &'a Guid => "uniqueidentifier"
);

impl<'a> ToSql for &'a str {
    fn to_sql(&self) -> &'static str {
        const max: usize = 1<<30;
        match self.len() {
            0...8000 => "NVARCHAR(4000)",
            8000...max => "NVARCHAR(MAX)",
            _ => "NTEXT",
        }
    }
}

#[cfg(test)]
mod tests {
    use tokio_core::reactor::Core;
    use futures::Future;
    use stmt::ForEachRow;
    use super::Guid;
    use SqlConnection;

    /// prepares a statement which selects a passed value
    /// this tests serialization of a parameter and deserialization
    /// atlast it checks if the received value is the same as the sent value
    macro_rules! test_datatype {
        ( $($name:ident: $ty:ty => $val:expr),* ) => {
            $(
                #[test]
                fn $name() {
                    let mut lp = Core::new().unwrap();
                    let future = SqlConnection::connect(lp.handle(), "server=tcp:127.0.0.1,1433;integratedSecurity=true;")
                        .map(|conn| (conn.prepare("SELECT @P1"), conn))
                        .and_then(|(stmt, conn)| {
                            conn.query(&stmt, &[&$val]).for_each_row(|row| {
                                assert_eq!(row.get::<_, $ty>(0), $val);
                                Ok(())
                            })
                        });
                    lp.run(future).unwrap();
                }
            )*
        }
    }

    test_datatype!(
        test_bit_1: bool => true,
        test_bit_0: bool => false,
        test_i8 :  i8 => 127i8,
        test_i16: i16 => 16100i16,
        test_i32: i32 => -4i32,
        test_i64: i64 => 1i64<<33,
        test_f32: f32 => 42.42f32,
        test_f64: f64 => 26.26f64,
        test_str: &str => "hello world",
        // TODO: Guid parsing
        test_guid: &Guid => &Guid::from_bytes(&[0, 0, 0, 0, 1, 0, 0, 0, 0, 1, 0, 0, 0, 0, 1, 0])
    );

    #[test]
    fn test_decimal_numeric() {
        let mut lp = Core::new().unwrap();
        let future = SqlConnection::connect(lp.handle(), "server=tcp:127.0.0.1,1433;integratedSecurity=true;")
            .and_then(|conn| conn.simple_query("select 18446744073709554899982888888888").for_each_row(|row| {
                assert_eq!(row.get::<_, f64>(0), 18446744073709554000000000000000f64);
                Ok(())
            }));
        lp.run(future).unwrap();
    }
}

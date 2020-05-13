use crate::{tds::Numeric, xml::XmlData, ColumnData};
use uuid::Uuid;

/// A conversion trait from a TDS type by-reference.
pub trait FromSql<'a>
where
    Self: Sized + 'a,
{
    /// Returns the value, `None` being a null value, copying the value.
    fn from_sql(value: &'a ColumnData<'static>) -> crate::Result<Option<Self>>;
}

/// A conversion trait from a TDS type by-value.
pub trait FromSqlOwned
where
    Self: Sized,
{
    /// Returns the value, `None` being a null value, taking the ownership.
    fn from_sql_owned(value: ColumnData<'static>) -> crate::Result<Option<Self>>;
}

from_sql!(bool: ColumnData::Bit(val) => (*val, val));
from_sql!(i8: ColumnData::I8(val) => (*val, val));
from_sql!(i16: ColumnData::I16(val) => (*val, val));
from_sql!(i32: ColumnData::I32(val) => (*val, val));
from_sql!(i64: ColumnData::I64(val) => (*val, val));
from_sql!(f32: ColumnData::F32(val) => (*val, val));
from_sql!(f64: ColumnData::F64(val) => (*val, val));
from_sql!(Uuid: ColumnData::Guid(val) => (*val, val));
from_sql!(Numeric: ColumnData::Numeric(n) => (*n, n));

impl FromSqlOwned for XmlData {
    fn from_sql_owned(value: ColumnData<'static>) -> crate::Result<Option<Self>> {
        match value {
            ColumnData::Xml(data) => Ok(data.map(|data| data.into_owned())),
            v => Err(crate::Error::Conversion(
                format!("cannot interpret {:?} as a String value", v).into(),
            )),
        }
    }
}

impl<'a> FromSql<'a> for &'a XmlData {
    fn from_sql(value: &'a ColumnData<'static>) -> crate::Result<Option<Self>> {
        match value {
            ColumnData::Xml(data) => Ok(data.as_ref().map(|s| s.as_ref())),
            v => Err(crate::Error::Conversion(
                format!("cannot interpret {:?} as a String value", v).into(),
            )),
        }
    }
}

impl FromSqlOwned for String {
    fn from_sql_owned(value: ColumnData<'static>) -> crate::Result<Option<Self>> {
        match value {
            ColumnData::String(s) => Ok(s.map(|s| s.into_owned())),
            v => Err(crate::Error::Conversion(
                format!("cannot interpret {:?} as a String value", v).into(),
            )),
        }
    }
}

impl<'a> FromSql<'a> for &'a str {
    fn from_sql(value: &'a ColumnData<'static>) -> crate::Result<Option<Self>> {
        match value {
            ColumnData::String(s) => Ok(s.as_ref().map(|s| s.as_ref())),
            v => Err(crate::Error::Conversion(
                format!("cannot interpret {:?} as a String value", v).into(),
            )),
        }
    }
}

impl FromSqlOwned for Vec<u8> {
    fn from_sql_owned(value: ColumnData<'static>) -> crate::Result<Option<Self>> {
        match value {
            ColumnData::Binary(b) => Ok(b.map(|s| s.into_owned())),
            v => Err(crate::Error::Conversion(
                format!("cannot interpret {:?} as a String value", v).into(),
            )),
        }
    }
}

impl<'a> FromSql<'a> for &'a [u8] {
    fn from_sql(value: &'a ColumnData<'static>) -> crate::Result<Option<Self>> {
        match value {
            ColumnData::Binary(b) => Ok(b.as_ref().map(|s| s.as_ref())),
            v => Err(crate::Error::Conversion(
                format!("cannot interpret {:?} as a &[u8] value", v).into(),
            )),
        }
    }
}

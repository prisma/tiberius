use crate::{
    tds::{codec::ColumnData, Numeric},
    xml::XmlData,
};
use std::borrow::Cow;
use uuid::Uuid;

/// A conversion trait to a TDS type.
pub trait ToSql {
    /// Convert to a value understood by the SQL Server.
    fn to_sql(&self) -> ColumnData<'_>;
}

to_sql!(self_,
        bool: (ColumnData::Bit, *self_);
        i8: (ColumnData::I8, *self_);
        i16: (ColumnData::I16, *self_);
        i32: (ColumnData::I32, *self_);
        i64: (ColumnData::I64, *self_);
        f32: (ColumnData::F32, *self_);
        f64: (ColumnData::F64, *self_);
        &str: (ColumnData::String, Cow::from(*self_));
        String: (ColumnData::String, Cow::from(self_));
        &[u8]: (ColumnData::Binary, Cow::from(*self_));
        Vec<u8>: (ColumnData::Binary, Cow::from(self_));
        Numeric: (ColumnData::Numeric, *self_);
        XmlData: (ColumnData::Xml, Cow::Borrowed(self_));
        Uuid: (ColumnData::Guid, *self_);
);

use crate::{
    tds::{codec::ColumnData, Numeric},
    xml::XmlData,
};
use std::borrow::Cow;
use uuid::Uuid;

/// A conversion trait to a TDS type.
pub trait ToSql: Send + Sync {
    /// Convert to a value understood by the SQL Server. Conversion
    /// by-reference.
    fn to_sql(&self) -> ColumnData<'_>;
}

/// A by-value conversion trait to a TDS type.
pub trait IntoSql: Send + Sync {
    /// Convert to a value understood by the SQL Server. Conversion by-value.
    fn into_sql(self) -> ColumnData<'static>;
}

into_sql!(self_,
          String: (ColumnData::String, Cow::from(self_));
          Vec<u8>: (ColumnData::Binary, Cow::from(self_));
          XmlData: (ColumnData::Xml, Cow::Owned(self_));
);

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
        Cow<'_, str>: (ColumnData::String, self_.clone());
        &[u8]: (ColumnData::Binary, Cow::from(*self_));
        Cow<'_, [u8]>: (ColumnData::Binary, self_.clone());
        Vec<u8>: (ColumnData::Binary, Cow::from(self_));
        Numeric: (ColumnData::Numeric, *self_);
        XmlData: (ColumnData::Xml, Cow::Borrowed(self_));
        Uuid: (ColumnData::Guid, *self_);
);

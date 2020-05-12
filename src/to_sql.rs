use crate::{
    tds::{codec::ColumnData, Numeric},
    xml::XmlData,
};
use std::borrow::Cow;
use uuid::Uuid;

const MAX_NVARCHAR_SIZE: usize = 1 << 30;

to_sql!(self_,
        bool: ("bit", ColumnData::Bit(*self_));
        i8: ("tinyint", ColumnData::I8(*self_));
        i16: ("smallint", ColumnData::I16(*self_));
        i32: ("int", ColumnData::I32(*self_));
        i64: ("bigint", ColumnData::I64(*self_));
        f32: ("float(24)", ColumnData::F32(*self_));
        f64: ("float(53)", ColumnData::F64(*self_));
        Uuid: ("uniqueidentifier", ColumnData::Guid(*self_));
);

/// A conversion trait from Rust to a TDS type.
pub trait ToSql {
    /// Returns a tuple of the type name in TDS and the `ColumnData`
    /// representation of the value.
    fn to_sql(&self) -> (Cow<'static, str>, ColumnData<'_>);
}

impl ToSql for Numeric {
    fn to_sql(&self) -> (Cow<'static, str>, ColumnData<'static>) {
        let type_name = format!("numeric({},{})", self.precision(), self.scale());
        (type_name.into(), ColumnData::Numeric(*self))
    }
}

impl ToSql for str {
    fn to_sql(&self) -> (Cow<'static, str>, ColumnData<'_>) {
        let sql_type = match self.len() {
            0..=4000 => "nvarchar(4000)",
            4001..=MAX_NVARCHAR_SIZE => "nvarchar(MAX)",
            _ => "ntext",
        };

        (Cow::Borrowed(sql_type), ColumnData::String(Cow::from(self)))
    }
}

impl ToSql for &str {
    fn to_sql(&self) -> (Cow<'static, str>, ColumnData<'_>) {
        let sql_type = match self.len() {
            0..=4000 => "nvarchar(4000)",
            4001..=MAX_NVARCHAR_SIZE => "nvarchar(MAX)",
            _ => "ntext",
        };

        (
            Cow::Borrowed(sql_type),
            ColumnData::String(Cow::from(*self)),
        )
    }
}

impl ToSql for Option<&str> {
    fn to_sql(&self) -> (Cow<'static, str>, ColumnData<'_>) {
        match self {
            Some(s) => {
                let sql_type = match s.len() {
                    0..=4000 => "nvarchar(4000)",
                    4001..=MAX_NVARCHAR_SIZE => "nvarchar(MAX)",
                    _ => "ntext",
                };

                (sql_type.into(), ColumnData::String(Cow::from(*s)))
            }
            None => ("nvarchar(4000)".into(), ColumnData::None),
        }
    }
}

impl<'a> ToSql for String {
    fn to_sql(&self) -> (Cow<'static, str>, ColumnData<'_>) {
        let sql_type = match self.len() {
            0..=4000 => "nvarchar(4000)",
            4001..=MAX_NVARCHAR_SIZE => "nvarchar(MAX)",
            _ => "ntext",
        };

        (sql_type.into(), ColumnData::String(Cow::from(self)))
    }
}

impl ToSql for Option<String> {
    fn to_sql(&self) -> (Cow<'static, str>, ColumnData<'_>) {
        match self {
            Some(s) => {
                let sql_type = match s.len() {
                    0..=4000 => "nvarchar(4000)",
                    4001..=MAX_NVARCHAR_SIZE => "nvarchar(MAX)",
                    _ => "ntext",
                };

                (sql_type.into(), ColumnData::String(Cow::from(s)))
            }
            None => ("nvarchar(4000)".into(), ColumnData::None),
        }
    }
}

impl ToSql for &[u8] {
    fn to_sql(&self) -> (Cow<'static, str>, ColumnData<'_>) {
        let sql_type = match self.len() {
            0..=8000 => "varbinary(8000)",
            _ => "varbinary(MAX)",
        };

        (sql_type.into(), ColumnData::Binary(Cow::from(*self)))
    }
}

impl ToSql for Option<&[u8]> {
    fn to_sql(&self) -> (Cow<'static, str>, ColumnData<'_>) {
        match self {
            Some(s) => {
                let sql_type = match s.len() {
                    0..=4000 => "varbinary(8000)",
                    _ => "varbinary(MAX)",
                };

                (sql_type.into(), ColumnData::Binary(Cow::from(*s)))
            }
            None => ("varbinary(4000)".into(), ColumnData::None),
        }
    }
}

impl ToSql for Vec<u8> {
    fn to_sql(&self) -> (Cow<'static, str>, ColumnData<'_>) {
        let sql_type = match self.len() {
            0..=8000 => "varbinary(8000)",
            _ => "varbinary(MAX)",
        };

        (sql_type.into(), ColumnData::Binary(Cow::from(self)))
    }
}

impl ToSql for Option<Vec<u8>> {
    fn to_sql(&self) -> (Cow<'static, str>, ColumnData<'_>) {
        match self {
            Some(s) => {
                let sql_type = match s.len() {
                    0..=4000 => "varbinary(8000)",
                    _ => "varbinary(MAX)",
                };

                (sql_type.into(), ColumnData::Binary(Cow::from(s)))
            }
            None => ("varbinary(8000)".into(), ColumnData::None),
        }
    }
}

impl ToSql for XmlData {
    fn to_sql(&self) -> (Cow<'static, str>, ColumnData<'_>) {
        ("xml".into(), ColumnData::Xml(Cow::Borrowed(self)))
    }
}

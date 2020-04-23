use crate::protocol::codec::ColumnData;
use std::borrow::Cow;
use uuid::Uuid;

const MAX_NVARCHAR_SIZE: usize = 1 << 30;

pub trait ToSql {
    fn to_sql(&self) -> (&'static str, ColumnData);
}

impl ToSql for str {
    fn to_sql(&self) -> (&'static str, ColumnData) {
        let sql_type = match self.len() {
            0..=4000 => "NVARCHAR(4000)",
            4001..=MAX_NVARCHAR_SIZE => "NVARCHAR(MAX)",
            _ => "NTEXT",
        };

        (sql_type, ColumnData::String(Cow::from(self)))
    }
}

impl ToSql for &str {
    fn to_sql(&self) -> (&'static str, ColumnData) {
        let sql_type = match self.len() {
            0..=4000 => "NVARCHAR(4000)",
            4001..=MAX_NVARCHAR_SIZE => "NVARCHAR(MAX)",
            _ => "NTEXT",
        };

        (sql_type, ColumnData::String(Cow::from(*self)))
    }
}

impl ToSql for Option<&str> {
    fn to_sql(&self) -> (&'static str, ColumnData) {
        match self {
            Some(s) => {
                let sql_type = match s.len() {
                    0..=4000 => "NVARCHAR(4000)",
                    4001..=MAX_NVARCHAR_SIZE => "NVARCHAR(MAX)",
                    _ => "NTEXT",
                };

                (sql_type, ColumnData::String(Cow::from(*s)))
            }
            None => ("NVARCHAR(4000)", ColumnData::None),
        }
    }
}

impl<'a> ToSql for String {
    fn to_sql(&self) -> (&'static str, ColumnData) {
        let sql_type = match self.len() {
            0..=4000 => "NVARCHAR(4000)",
            4001..=MAX_NVARCHAR_SIZE => "NVARCHAR(MAX)",
            _ => "NTEXT",
        };

        (sql_type, ColumnData::String(Cow::from(self)))
    }
}

impl ToSql for Option<String> {
    fn to_sql(&self) -> (&'static str, ColumnData) {
        match self {
            Some(s) => {
                let sql_type = match s.len() {
                    0..=4000 => "NVARCHAR(4000)",
                    4001..=MAX_NVARCHAR_SIZE => "NVARCHAR(MAX)",
                    _ => "NTEXT",
                };

                (sql_type, ColumnData::String(Cow::from(s)))
            }
            None => ("NVARCHAR(4000)", ColumnData::None),
        }
    }
}

impl ToSql for &[u8] {
    fn to_sql(&self) -> (&'static str, ColumnData) {
        let sql_type = match self.len() {
            0..=8000 => "VARBINARY(8000)",
            _ => "VARBINARY(MAX)",
        };

        (sql_type, ColumnData::Binary(Cow::from(*self)))
    }
}

impl ToSql for Option<&[u8]> {
    fn to_sql(&self) -> (&'static str, ColumnData) {
        match self {
            Some(s) => {
                let sql_type = match s.len() {
                    0..=4000 => "VARBINARY(8000)",
                    _ => "VARBINARY(MAX)",
                };

                (sql_type, ColumnData::Binary(Cow::from(*s)))
            }
            None => ("VARBINARY(4000)", ColumnData::None),
        }
    }
}

impl ToSql for Vec<u8> {
    fn to_sql(&self) -> (&'static str, ColumnData) {
        let sql_type = match self.len() {
            0..=8000 => "VARBINARY(8000)",
            _ => "VARBINARY(MAX)",
        };

        (sql_type, ColumnData::Binary(Cow::from(self)))
    }
}

impl ToSql for Option<Vec<u8>> {
    fn to_sql(&self) -> (&'static str, ColumnData) {
        match self {
            Some(s) => {
                let sql_type = match s.len() {
                    0..=4000 => "VARBINARY(8000)",
                    _ => "VARBINARY(MAX)",
                };

                (sql_type, ColumnData::Binary(Cow::from(s)))
            }
            None => ("VARBINARY(8000)", ColumnData::None),
        }
    }
}

impl ToSql for bool {
    fn to_sql(&self) -> (&'static str, ColumnData) {
        ("bit", ColumnData::Bit(*self))
    }
}

impl ToSql for Option<bool> {
    fn to_sql(&self) -> (&'static str, ColumnData) {
        match self {
            None => ("bit", ColumnData::None),
            Some(item) => ("bit", ColumnData::Bit(*item)),
        }
    }
}

impl ToSql for i8 {
    fn to_sql(&self) -> (&'static str, ColumnData) {
        ("tinyint", ColumnData::I8(*self))
    }
}

impl ToSql for Option<i8> {
    fn to_sql(&self) -> (&'static str, ColumnData) {
        match self {
            None => ("tinyint", ColumnData::None),
            Some(item) => ("tinyint", ColumnData::I8(*item)),
        }
    }
}

impl ToSql for i16 {
    fn to_sql(&self) -> (&'static str, ColumnData) {
        ("smallint", ColumnData::I16(*self))
    }
}

impl ToSql for Option<i16> {
    fn to_sql(&self) -> (&'static str, ColumnData) {
        match self {
            None => ("smallint", ColumnData::None),
            Some(item) => ("smallint", ColumnData::I16(*item)),
        }
    }
}

impl ToSql for i32 {
    fn to_sql(&self) -> (&'static str, ColumnData) {
        ("int", ColumnData::I32(*self))
    }
}

impl ToSql for Option<i32> {
    fn to_sql(&self) -> (&'static str, ColumnData) {
        match self {
            None => ("int", ColumnData::None),
            Some(item) => ("int", ColumnData::I32(*item)),
        }
    }
}

impl ToSql for i64 {
    fn to_sql(&self) -> (&'static str, ColumnData) {
        ("bigint", ColumnData::I64(*self))
    }
}

impl ToSql for Option<i64> {
    fn to_sql(&self) -> (&'static str, ColumnData) {
        match self {
            None => ("bigint", ColumnData::None),
            Some(item) => ("bigint", ColumnData::I64(*item)),
        }
    }
}

impl ToSql for f32 {
    fn to_sql(&self) -> (&'static str, ColumnData) {
        ("float(24)", ColumnData::F32(*self))
    }
}

impl ToSql for Option<f32> {
    fn to_sql(&self) -> (&'static str, ColumnData) {
        match self {
            None => ("float(24)", ColumnData::None),
            Some(item) => ("float(24)", ColumnData::F32(*item)),
        }
    }
}

impl ToSql for f64 {
    fn to_sql(&self) -> (&'static str, ColumnData) {
        ("float(53)", ColumnData::F64(*self))
    }
}

impl ToSql for Option<f64> {
    fn to_sql(&self) -> (&'static str, ColumnData) {
        match self {
            None => ("float(53)", ColumnData::None),
            Some(item) => ("float(24)", ColumnData::F64(*item)),
        }
    }
}

impl ToSql for Uuid {
    fn to_sql(&self) -> (&'static str, ColumnData) {
        ("uniqueidentifier", ColumnData::Guid(*self))
    }
}

impl ToSql for Option<Uuid> {
    fn to_sql(&self) -> (&'static str, ColumnData) {
        match self {
            None => ("uniqueidentifier", ColumnData::None),
            Some(item) => ("uniqueidentifier", ColumnData::Guid(*item)),
        }
    }
}

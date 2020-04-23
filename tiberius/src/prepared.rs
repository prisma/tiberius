use crate::protocol::codec::ColumnData;
use crate::to_sql;
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

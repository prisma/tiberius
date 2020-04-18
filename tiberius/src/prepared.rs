use crate::protocol::codec::ColumnData;
use std::borrow::Cow;

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

macro_rules! to_sql {
    ($target:ident, $( $ty:ty: $val:expr ;)* ) => {
        $(
            impl ToSql for $ty {
                fn to_sql(&self) -> (&'static str, ColumnData) {
                    let $target = self;
                    $val
                }
            }
        )*
    };
}

to_sql!(self_,
    bool: ("bit", ColumnData::Bit(*self_));
    i8: ("tinyint", ColumnData::I8(*self_));
    i16: ("smallint", ColumnData::I16(*self_));
    i32: ("int", ColumnData::I32(*self_));
    i64: ("bigint", ColumnData::I64(*self_));
    f32: ("float(24)", ColumnData::F32(*self_));
    f64: ("float(53)", ColumnData::F64(*self_));
);

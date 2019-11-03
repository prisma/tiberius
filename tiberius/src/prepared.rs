use crate::protocol::ColumnData;

pub trait ToSql {
    fn to_sql(&self) -> (&'static str, ColumnData);
}

impl ToSql for i32 {
    fn to_sql(&self) -> (&'static str, ColumnData) {
        ("int", ColumnData::I32(*self))
    }
}

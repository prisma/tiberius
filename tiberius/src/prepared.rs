use crate::protocol::ColumnData;

pub trait ToSql {
    fn to_sql(&self) -> (&'static str, ColumnData);
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

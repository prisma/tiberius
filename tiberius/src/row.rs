use crate::protocol;
use crate::protocol::ColumnData;
use std::convert::TryFrom;

use crate::error::Error;
use crate::Result;

macro_rules! from_column_data {
    ($( $ty:ty: $($pat:pat => $val:expr),* );* ) => {
        $(
            impl<'a> TryFrom<&'a ColumnData> for $ty {
                type Error = Error;

                fn try_from(data: &ColumnData) -> Result<Self> {
                    match *data {
                        $( $pat => Ok($val), )*
                        _ => Err(Error::Conversion(format!("cannot interpret {:?} as an {} value", data, stringify!($ty)).into()))
                    }
                }
            }
        )*
    };
}

#[derive(Debug)] // TODO
pub struct Row(pub(crate) protocol::TokenRow);

pub trait QueryIdx {
    fn idx(&self, row: &Row) -> Option<usize>;
}

impl QueryIdx for usize {
    fn idx(&self, _row: &Row) -> Option<usize> {
        Some(*self)
    }
}

impl Row {
    /// Returns the amount of columns in the row
    pub fn len(&self) -> usize {
        self.0.columns.len()
    }

    /// Retrieve a column's value for a given column index
    ///
    /// # Panics
    /// This panics if:
    ///
    /// - the requested type conversion (SQL->Rust) is not possible
    /// - the given index is out of bounds (column does not exist)
    pub fn get<'a, I, R>(&'a self, idx: I) -> R
    where
        I: QueryIdx,
        R: TryFrom<&'a protocol::ColumnData, Error = Error>,
    {
        self.try_get(idx)
            .expect("given index out of bounds")
            .unwrap()
    }

    pub fn try_get<'a, I, R>(&'a self, idx: I) -> Result<Option<R>>
    where
        I: QueryIdx,
        R: TryFrom<&'a protocol::ColumnData, Error = Error>,
    {
        let idx = match idx.idx(self) {
            Some(x) => x,
            None => return Ok(None),
        };

        let col_data = &self.0.columns[idx];
        R::try_from(col_data).map(Some)
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
    f64:        ColumnData::F64(val) => val
                // ColumnData::Numeric(val) => val.into();
    // TODO &'a str:    ColumnData::BString(ref buf) => buf.as_str(),
    //             ColumnData::String(ref buf) => buf;
    // TODO &'a Guid:   ColumnData::Guid(ref guid) => guid;
    // &'a [u8]:   ColumnData::Binary(ref buf) => buf
    // TODO  Numeric:    ColumnData::Numeric(val) => val
);

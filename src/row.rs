use crate::{
    error::Error,
    tds::{
        codec::{ColumnData, TokenRow},
        xml::XmlData,
    },
};
use std::{convert::TryFrom, sync::Arc};
use uuid::Uuid;

impl<'a> TryFrom<&'a ColumnData<'a>> for &'a str {
    type Error = Error;

    fn try_from(value: &'a ColumnData<'a>) -> Result<Self, Self::Error> {
        match value {
            ColumnData::String(s) => Ok(s.as_ref()),
            _ => Err(Error::Conversion(
                format!("cannot interpret {:?} as an str value", value).into(),
            )),
        }
    }
}

impl<'a> TryFrom<&'a ColumnData<'a>> for &'a [u8] {
    type Error = Error;

    fn try_from(value: &'a ColumnData<'a>) -> Result<Self, Self::Error> {
        match value {
            ColumnData::Binary(s) => Ok(s.as_ref()),
            _ => Err(Error::Conversion(
                format!("cannot interpret {:?} as a [u8] value", value).into(),
            )),
        }
    }
}

impl<'a> TryFrom<&ColumnData<'a>> for String {
    type Error = Error;

    fn try_from(value: &ColumnData<'a>) -> Result<Self, Self::Error> {
        match value {
            ColumnData::String(s) => Ok(s.to_string()),
            _ => Err(Error::Conversion(
                format!("cannot interpret {:?} as a String value", value).into(),
            )),
        }
    }
}

impl<'a> TryFrom<ColumnData<'a>> for String {
    type Error = Error;

    fn try_from(value: ColumnData<'a>) -> Result<Self, Self::Error> {
        match value {
            ColumnData::String(s) => Ok(s.into_owned()),
            _ => Err(Error::Conversion(
                format!("cannot interpret {:?} as a String value", value).into(),
            )),
        }
    }
}

impl<'a> TryFrom<&ColumnData<'a>> for Vec<u8> {
    type Error = Error;

    fn try_from(value: &ColumnData<'a>) -> Result<Self, Self::Error> {
        match value {
            ColumnData::Binary(b) => Ok(b.to_vec()),
            _ => Err(Error::Conversion(
                format!("cannot interpret {:?} as a Vec<u8> value", value).into(),
            )),
        }
    }
}

impl<'a> TryFrom<ColumnData<'a>> for Vec<u8> {
    type Error = Error;

    fn try_from(value: ColumnData<'a>) -> Result<Self, Self::Error> {
        match value {
            ColumnData::Binary(b) => Ok(b.into_owned()),
            _ => Err(Error::Conversion(
                format!("cannot interpret {:?} as a Vec<u8> value", value).into(),
            )),
        }
    }
}

impl<'a> TryFrom<&'a ColumnData<'a>> for XmlData {
    type Error = Error;

    fn try_from(value: &'a ColumnData<'a>) -> Result<Self, Self::Error> {
        match value {
            ColumnData::Xml(s) => Ok(s.clone().into_owned()),
            _ => Err(Error::Conversion(
                format!("cannot interpret {:?} as an XML value", value).into(),
            )),
        }
    }
}

impl<'a> TryFrom<ColumnData<'a>> for XmlData {
    type Error = Error;

    fn try_from(value: ColumnData<'a>) -> Result<Self, Self::Error> {
        match value {
            ColumnData::Xml(s) => Ok(s.into_owned()),
            _ => Err(Error::Conversion(
                format!("cannot interpret {:?} as an XML value", value).into(),
            )),
        }
    }
}

#[derive(Debug)] // TODO
pub struct Column {
    pub(crate) name: String,
}

impl Column {
    pub fn name(&self) -> &str {
        &self.name
    }
}

/// A row of data from a query.
#[derive(Debug)]
pub struct Row {
    pub(crate) columns: Arc<Vec<Column>>,
    pub(crate) data: TokenRow,
}

pub trait QueryIdx {
    fn idx(&self, row: &Row) -> Option<usize>;
}

impl QueryIdx for usize {
    fn idx(&self, _row: &Row) -> Option<usize> {
        Some(*self)
    }
}

impl Row {
    /// Columns defining the row data. Columns listed here are in the same order
    /// as the resulting data.
    pub fn columns(&self) -> &[Column] {
        &self.columns
    }

    /// Returns the amount of columns in the row
    pub fn len(&self) -> usize {
        self.data.columns.len()
    }

    /// Retrieve a column's value for a given column index
    ///
    /// # Panics
    ///
    /// - the requested type conversion (SQL->Rust) is not possible
    /// - the given index is out of bounds (column does not exist)
    pub fn get<'a, I, R>(&'a self, idx: I) -> R
    where
        I: QueryIdx,
        R: TryFrom<&'a ColumnData<'a>, Error = Error>,
    {
        self.try_get(idx)
            .expect("given index out of bounds")
            .unwrap()
    }

    /// Retrieve a column's value for a given column index.
    ///
    /// Returns `None` if:
    ///
    /// - Index is out of bounds.
    /// - Column is null.
    ///
    /// Returns an error if:
    ///
    /// - Column data conversion fails.
    pub fn try_get<'a, I, R>(&'a self, idx: I) -> crate::Result<Option<R>>
    where
        I: QueryIdx,
        R: TryFrom<&'a ColumnData<'a>, Error = Error>,
    {
        let idx = match idx.idx(self) {
            Some(x) => x,
            None => return Ok(None),
        };

        let col_data = &self.data.columns[idx];

        if let ColumnData::None = col_data {
            Ok(None)
        } else {
            R::try_from(col_data).map(Some)
        }
    }

    /// Takes the first column data out from the row, consuming the row.
    ///
    /// Returns `None` if:
    ///
    /// - Row has no data.
    /// - Column is null.
    ///
    /// Returns an error if:
    ///
    /// - Column data conversion fails.
    pub fn into_first<'a, R>(self) -> crate::Result<Option<R>>
    where
        R: TryFrom<ColumnData<'a>, Error = Error>,
    {
        match self.into_iter().next() {
            None => Ok(None),
            Some(ColumnData::None) => Ok(None),
            Some(col_data) => R::try_from(col_data).map(Some),
        }
    }
}

impl IntoIterator for Row {
    type Item = ColumnData<'static>;
    type IntoIter = std::vec::IntoIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        self.data.columns.into_iter()
    }
}

from_column_data!(
    bool:       ColumnData::Bit(val) => *val;
    i8:         ColumnData::I8(val) => *val;
    i16:        ColumnData::I16(val) => *val;
    i32:        ColumnData::I32(val) => *val;
    i64:        ColumnData::I64(val) => *val;
    f32:        ColumnData::F32(val) => *val;
    f64:        ColumnData::F64(val) => *val;
    Uuid:       ColumnData::Guid(val) => *val

    // ColumnData::Numeric(val) => val.into();
    // TODO &'a str:    ColumnData::BString(ref buf) => buf.as_str(),
    //             ColumnData::String(ref buf) => buf;
    // TODO &'a Guid:   ColumnData::Guid(ref guid) => guid;
    // &'a [u8]:   ColumnData::Binary(ref buf) => buf
    // TODO  Numeric:    ColumnData::Numeric(val) => val
);

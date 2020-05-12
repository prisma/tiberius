use crate::{
    error::Error,
    tds::{
        codec::{ColumnData, FixedLenType, TokenRow, TypeInfo, VarLenType},
        xml::XmlData,
        Numeric,
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

impl<'a> TryFrom<ColumnData<'a>> for Numeric {
    type Error = Error;

    fn try_from(value: ColumnData<'a>) -> Result<Self, Self::Error> {
        match value {
            ColumnData::Numeric(n) => Ok(n),
            _ => Err(Error::Conversion(
                format!("cannot interpret {:?} as a Numeric value", value).into(),
            )),
        }
    }
}

impl<'a> TryFrom<&'a ColumnData<'a>> for Numeric {
    type Error = Error;

    fn try_from(value: &'a ColumnData<'a>) -> Result<Self, Self::Error> {
        match value {
            ColumnData::Numeric(n) => Ok(*n),
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

/// A column of data from a query.
#[derive(Debug)]
pub struct Column {
    pub(crate) name: String,
    pub(crate) column_type: ColumnType,
}

impl Column {
    /// The name of the column.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// The type of the column.
    pub fn column_type(&self) -> ColumnType {
        self.column_type
    }
}

#[derive(Debug, Clone, Copy)]
/// The type of the column.
pub enum ColumnType {
    /// The column doesn't have a specified type.
    Null,
    /// A bit or boolean value.
    Bit,
    /// An 8-bit integer value.
    Int1,
    /// A 16-bit integer value.
    Int2,
    /// A 32-bit integer value.
    Int4,
    /// A 64-bit integer value.
    Int8,
    /// A 32-bit datetime value.
    Datetime4,
    /// A 32-bit floating point value.
    Float4,
    /// A 64-bit floating point value.
    Float8,
    /// Money value.
    Money,
    /// A TDS 7.2 datetime value.
    Datetime,
    /// A 32-bit money value.
    Money4,
    /// A unique identifier, UUID.
    Guid,
    /// N-bit integer value (variable).
    Intn,
    /// A bit value in a variable-length type.
    Bitn,
    /// A decimal value (same as `Numericn`).
    Decimaln,
    /// A numeric value (same as `Decimaln`).
    Numericn,
    /// A n-bit floating point value.
    Floatn,
    /// A n-bit datetime value (TDS 7.2).
    Datetimen,
    /// A n-bit date value (TDS 7.3).
    Daten,
    /// A n-bit time value (TDS 7.3).
    Timen,
    /// A n-bit datetime2 value (TDS 7.3).
    Datetime2,
    /// A n-bit datetime value with an offset (TDS 7.3).
    DatetimeOffsetn,
    /// A variable binary value.
    BigVarBin,
    /// A large variable string value.
    BigVarChar,
    /// A binary value.
    BigBinary,
    /// A string value.
    BigChar,
    /// A variable string value with UTF-16 encoding.
    NVarchar,
    /// A string value with UTF-16 encoding.
    NChar,
    /// A XML value.
    Xml,
    /// User-defined type.
    Udt,
    /// A text value (deprecated).
    Text,
    /// A image value (deprecated).
    Image,
    /// A text value with UTF-16 encoding (deprecated).
    NText,
    /// An SQL variant type.
    SSVariant,
}

impl From<&TypeInfo> for ColumnType {
    fn from(ti: &TypeInfo) -> Self {
        match ti {
            TypeInfo::FixedLen(flt) => match flt {
                FixedLenType::Int1 => Self::Int1,
                FixedLenType::Bit => Self::Bit,
                FixedLenType::Int2 => Self::Int2,
                FixedLenType::Int4 => Self::Int4,
                FixedLenType::Datetime4 => Self::Datetime4,
                FixedLenType::Float4 => Self::Float4,
                FixedLenType::Money => Self::Money,
                FixedLenType::Datetime => Self::Datetime,
                FixedLenType::Float8 => Self::Float8,
                FixedLenType::Money4 => Self::Money4,
                FixedLenType::Int8 => Self::Int8,
                FixedLenType::Null => Self::Null,
            },
            TypeInfo::VarLenSized(vlt, _, _) => match vlt {
                VarLenType::Guid => Self::Guid,
                VarLenType::Intn => Self::Intn,
                VarLenType::Bitn => Self::Bitn,
                VarLenType::Decimaln => Self::Decimaln,
                VarLenType::Numericn => Self::Numericn,
                VarLenType::Floatn => Self::Floatn,
                VarLenType::Money => Self::Money,
                VarLenType::Datetimen => Self::Datetimen,
                VarLenType::Daten => Self::Daten,
                VarLenType::Timen => Self::Timen,
                VarLenType::Datetime2 => Self::Datetime2,
                VarLenType::DatetimeOffsetn => Self::DatetimeOffsetn,
                VarLenType::BigVarBin => Self::BigVarBin,
                VarLenType::BigVarChar => Self::BigVarChar,
                VarLenType::BigBinary => Self::BigBinary,
                VarLenType::BigChar => Self::BigChar,
                VarLenType::NVarchar => Self::NVarchar,
                VarLenType::NChar => Self::NChar,
                VarLenType::Xml => Self::Xml,
                VarLenType::Udt => Self::Udt,
                VarLenType::Text => Self::Text,
                VarLenType::Image => Self::Image,
                VarLenType::NText => Self::NText,
                VarLenType::SSVariant => Self::SSVariant,
            },
            TypeInfo::VarLenSizedPrecision { ty, .. } => match ty {
                VarLenType::Guid => Self::Guid,
                VarLenType::Intn => Self::Intn,
                VarLenType::Bitn => Self::Bitn,
                VarLenType::Decimaln => Self::Decimaln,
                VarLenType::Numericn => Self::Numericn,
                VarLenType::Floatn => Self::Floatn,
                VarLenType::Money => Self::Money,
                VarLenType::Datetimen => Self::Datetimen,
                VarLenType::Daten => Self::Daten,
                VarLenType::Timen => Self::Timen,
                VarLenType::Datetime2 => Self::Datetime2,
                VarLenType::DatetimeOffsetn => Self::DatetimeOffsetn,
                VarLenType::BigVarBin => Self::BigVarBin,
                VarLenType::BigVarChar => Self::BigVarChar,
                VarLenType::BigBinary => Self::BigBinary,
                VarLenType::BigChar => Self::BigChar,
                VarLenType::NVarchar => Self::NVarchar,
                VarLenType::NChar => Self::NChar,
                VarLenType::Xml => Self::Xml,
                VarLenType::Udt => Self::Udt,
                VarLenType::Text => Self::Text,
                VarLenType::Image => Self::Image,
                VarLenType::NText => Self::NText,
                VarLenType::SSVariant => Self::SSVariant,
            },
            TypeInfo::Xml { .. } => Self::Xml,
        }
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
    /// # Returns `None` if:
    ///
    /// - Index is out of bounds.
    /// - Column is null.
    ///
    /// # Returns an error if:
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
    /// # Returns `None` if:
    ///
    /// - Row has no data.
    /// - Column is null.
    ///
    /// # Returns an error if:
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

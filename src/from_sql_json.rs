use serde_json::{json,Value};
#[cfg(feature = "chrono")]
use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use super::{FromSql,ColumnData};


impl<'a> FromSql<'a> for Value {
    fn from_sql(value: &'a ColumnData<'static>) -> crate::Result<Option<Self>> {
        match value {
            ColumnData::U8(None) => Ok(None),
            ColumnData::U8(Some(val)) => Ok(Some(json!(*val))),
            ColumnData::I16(None) => Ok(None),
            ColumnData::I16(Some(val)) => Ok(Some(json!(*val))),
            ColumnData::I32(None) => Ok(None),
            ColumnData::I32(Some(val)) => Ok(Some(json!(*val))),
            ColumnData::I64(None) => Ok(None),
            ColumnData::I64(Some(val)) => Ok(Some(json!(*val))),
            ColumnData::F32(None) => Ok(None),
            ColumnData::F32(Some(val)) => Ok(Some(json!(*val))),
            ColumnData::F64(None) => Ok(None),
            ColumnData::F64(Some(val)) => Ok(Some(json!(*val))),
            ColumnData::Bit(None) => Ok(None),
            ColumnData::Bit(Some(val)) => Ok(Some(json!(*val))),
            ColumnData::String(None) => Ok(None),
            ColumnData::String(Some(val)) => Ok(Some(json!(*val))),
            ColumnData::Numeric(None) => Ok(None),
            ColumnData::Numeric(Some(val)) => Ok(Some(json!(f64::from(*val)))),
            ColumnData::DateTime(None) => Ok(None),
            #[cfg(feature = "chrono")]
            ColumnData::DateTime(Some(_)) => {
                let val = NaiveDateTime::from_sql(value).unwrap().unwrap();
                Ok(Some(json!(val)))
            }
            ColumnData::SmallDateTime(None) => Ok(None),
            #[cfg(feature = "chrono")]
            ColumnData::SmallDateTime(Some(_)) => {
                let val = NaiveDateTime::from_sql(value).unwrap().unwrap();
                Ok(Some(json!(val)))
            }
            ColumnData::DateTime2(None) => Ok(None),
            #[cfg(feature = "chrono")]
            ColumnData::DateTime2(Some(_)) => {
                let val = NaiveDateTime::from_sql(value).unwrap().unwrap();
                Ok(Some(json!(val)))
            }
            ColumnData::DateTimeOffset(None) => Ok(None),
            #[cfg(feature = "chrono")]
            ColumnData::DateTimeOffset(Some(_)) => {
                let val: DateTime<Utc> = DateTime::from_sql(value).unwrap().unwrap();
                Ok(Some(json!(val)))
            }
            ColumnData::Date(None) => Ok(None),
            #[cfg(feature = "chrono")]
            ColumnData::Date(Some(_)) => Ok(Some(json!(NaiveDate::from_sql(value)
                .unwrap()
                .unwrap()))),
            ColumnData::Time(None) => Ok(None),
            #[cfg(feature = "chrono")]
            ColumnData::Time(Some(_)) => Ok(Some(json!(NaiveTime::from_sql(value)
                .unwrap()
                .unwrap()))),
            ColumnData::Guid(None) => Ok(None),
            ColumnData::Guid(Some(val)) => Ok(Some(json!(val.to_string()))),
            ColumnData::Binary(None) => Ok(None),
            ColumnData::Binary(Some(val)) => Ok(Some(json!(*val))),
            ColumnData::Xml(None) => Ok(None),
            ColumnData::Xml(Some(val)) => Ok(Some(json!(*val.to_string()))),
            v => Err(crate::Error::Conversion(
                format!("cannot interpret {:?} as JsonValue", v).into(),
            )),
        }
    }
}
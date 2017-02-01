///! time type implementations
use std::borrow::Cow;
use super::{ColumnData, FromColumnData, ToColumnData, ToSql};
use {TdsError, TdsResult};

/// prepares a statement which selects a passed value
/// this tests serialization of a parameter and deserialization
/// atlast it checks if the received value is the same as the sent value
/// it also checks if the time formatted is correct
#[cfg(test)]
macro_rules! test_timedatatype {
    ( $($name:ident: $ty:ty = $val:expr => $str_val:expr),* ) => {
        $(
            #[test]
            fn $name() {
                let mut lp = Core::new().unwrap();
                let future = SqlConnection::connect(lp.handle(), connection_string().as_ref())
                    .map(|conn| (conn.prepare("SELECT @P1, convert(varchar, @P1, 121)"), conn))
                    .and_then(|(stmt, conn)| {
                        conn.query(&stmt, &[&$val]).for_each_row(|row| {
                            assert_eq!(row.get::<_, $ty>(0), $val);
                            assert_eq!(row.get::<_, &str>(1), $str_val);
                            Ok(())
                        })
                    });
                lp.run(future).unwrap();
            }
        )*
    }
}

/// # Warning
/// It isn't recommended to use this
/// If you want to deal with date types, use the chrono feature of this crate instead!
///
/// This is merely exported not to limit flexibility
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DateTime {
    /// Days since 1.1.1900 (including the negative range until 1.1.1753)
    pub days: i32,
    /// 1/300 of a second, so a value of 300 equals 1 second [since 12 AM]
    pub seconds_fragments: u32,
}

/// # Warning
/// It isn't recommended to use this
/// If you want to deal with date types, use the chrono feature of this crate instead!
///
/// This is merely exported not to limit flexibility
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SmallDateTime {
    /// Days since 1.1.1900 (including the negative range until 1.1.1753)
    pub days: u16,
    /// 1/300 of a second, so a value of 300 equals 1 second [since 12 AM]
    pub seconds_fragments: u16,
}

/// Number of days since January 1 in year 1, with
///
/// # Panics
/// max value of 3 bytes (u32::max_value() > 8)
///
/// # Warning
/// It isn't recommended to use this
/// If you want to deal with date types, use the chrono feature of this crate instead!
///
/// This is merely exported not to limit flexibility
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct Date(u32);

impl Date {
    #[inline]
    pub fn new(days: u32) -> Date {
        assert_eq!(days >> 24, 0);
        Date(days)
    }

    #[inline]
    pub fn days(&self) -> u32 {
        self.0
    }
}

to_sql!(
    DateTime => "datetime",
    &'a DateTime => "datetime",
    SmallDateTime => "smalldatetime",
    &'a SmallDateTime => "smalldatetime",
    Date => "date"
);

from_column_data!(
    &'a DateTime:           ColumnData::DateTime(ref dt) => dt;
    &'a SmallDateTime:      ColumnData::SmallDateTime(ref dt) => dt;
    Date:                   ColumnData::Date(dt) => dt
);

to_column_data!(self_,
    DateTime     =>         ColumnData::DateTime(Cow::Borrowed(self_)),
    &'a DateTime =>         ColumnData::DateTime(Cow::Borrowed(self_)),
    SmallDateTime     =>    ColumnData::SmallDateTime(Cow::Borrowed(self_)),
    &'a SmallDateTime =>    ColumnData::SmallDateTime(Cow::Borrowed(self_)),
    Date     =>             ColumnData::Date(*self_)
);

#[feature(chrono)]
mod chrono {
    extern crate chrono;

    use std::borrow::Cow;
    use self::chrono::{Duration, NaiveDate, NaiveDateTime, NaiveTime};
    use types::{ColumnData, FromColumnData, ToColumnData, ToSql};
    use super::{Date, DateTime};
    use {TdsError, TdsResult};

    #[inline]
    fn from_days(days: i64, start_year: i32) -> NaiveDate {
        NaiveDate::from_ymd(start_year, 1,1) + Duration::days(days as i64)
    }

    #[inline]
    fn from_sec_fragments(sec_fragments: i64) -> NaiveTime {
        NaiveTime::from_hms(0,0,0) + Duration::nanoseconds(sec_fragments * (1e9 as i64) / 300)
    }

    #[inline]
    fn to_days(date: &NaiveDate, start_year: i32) -> i64 {
        (*date - NaiveDate::from_ymd(start_year, 1, 1)).num_days()
    }

    #[inline]
    fn to_sec_fragments(time: &NaiveTime) -> i64 {
        (*time - NaiveTime::from_hms(0, 0, 0)).num_nanoseconds().unwrap() * 300 / (1e9 as i64)
    }

    from_column_data!(
        NaiveDateTime:
            ColumnData::SmallDateTime(ref dt) => NaiveDateTime::new(
                from_days(dt.days as i64, 1900),
                from_sec_fragments(dt.seconds_fragments as i64),
            ),
            ColumnData::DateTime(ref dt) => NaiveDateTime::new(
                from_days(dt.days as i64, 1900),
                from_sec_fragments(dt.seconds_fragments as i64)
            );
        NaiveDate:      ColumnData::Date(ref date) => from_days(date.days() as i64, 1)
    );
    to_column_data!(self_,
        NaiveDateTime =>
            // TODO: also use datetime2 here for TDS>=7.3
            ColumnData::DateTime(Cow::Owned(DateTime {
                days: to_days(&self_.date(), 1900) as i32,
                seconds_fragments: to_sec_fragments(&self_.time()) as u32,
            })),
        NaiveDate => ColumnData::Date(Date::new(to_days(self_, 1) as u32))
    );
    to_sql!(
        NaiveDate => "date",
        // TODO: use datetime2 instead ( TDS 7.3>= )
        NaiveDateTime => "datetime"
    );

    #[cfg(test)]
    mod tests {
        use futures::Future;
        use tokio_core::reactor::Core;
        use tests::connection_string;
        use super::chrono::{NaiveDate, NaiveDateTime};
        use stmt::ResultStreamExt;
        use SqlConnection;

        static DATETIME_TEST_STR: &'static str = "2015-09-05 23:56:04.000";

        test_timedatatype!(
            test_chrono_date: NaiveDate = NaiveDate::from_ymd(1223, 11, 4) => "1223-11-04",
            test_chrono_datetime: NaiveDateTime
                =  NaiveDateTime::parse_from_str(DATETIME_TEST_STR, "%Y-%m-%d %H:%M:%S.%f").unwrap()
                => DATETIME_TEST_STR
        );
    }
}

#[cfg(test)]
mod tests {
    use futures::Future;
    use tokio_core::reactor::Core;
    use super::{Date, DateTime, SmallDateTime};
    use stmt::ResultStreamExt;
    use SqlConnection;
    use tests::connection_string;

    test_timedatatype!(
        test_datetime: &DateTime = &DateTime {
            days: 41692, //24.02.2014
            seconds_fragments: (18*3600 + 42*60 + 23) * 300, // 18:42:23
        } => "2014-02-24 18:42:23.000",
        test_smalldatetime: &SmallDateTime = &SmallDateTime {
            days: 41692, //24.02.2014
            seconds_fragments: (12*60 + 45), // 12:45
        } => "2014-02-24 12:45:00.000",
        test_date: Date = Date::new(123) => "0001-05-04"
    );
}

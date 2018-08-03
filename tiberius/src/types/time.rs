///! time type implementations
use std::io::{Read, Write};
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use super::{ColumnData, FromColumnData, ToColumnData, ToSql};
use {Error, Result};

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
                let future = SqlConnection::connect(connection_string().as_ref())
                    .and_then(|conn| {
                        conn.query("SELECT @P1, convert(varchar, @P1, 121)", &[&$val]).for_each(|row| {
                            assert_eq!(row.get::<_, $ty>(0), $val);
                            assert_eq!(row.get::<_, &str>(1), $str_val);
                            Ok(())
                        })
                    });
                current_thread::block_on_all(future).unwrap();
            }
        )*
    }
}

/// # Warning
/// It isn't recommended to use this
/// If you want to deal with date types, use the chrono feature of this crate instead!
///
/// This is merely exported not to limit flexibility
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
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
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
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

/// Number of 10^-n second increments since 12AM
#[derive(Copy, Clone, Debug)]
pub struct Time {
    pub increments: u64,
    pub scale: u8,
}

impl PartialEq for Time {
    fn eq(&self, t: &Time) -> bool {
        self.increments as f64 / 10f64.powi(self.scale as i32)
            == t.increments as f64 / 10f64.powi(t.scale as i32)
    }
}

impl Time {
    #[inline]
    pub fn len(&self) -> Result<u8> {
        Ok(match self.scale {
            0...2 => 3,
            3...4 => 4,
            5...7 => 5,
            _ => {
                return Err(Error::Protocol(
                    format!("timen: invalid scale {}", self.scale).into(),
                ))
            }
        })
    }

    pub fn encode_to<W: Write>(&self, mut wr: W) -> Result<()> {
        match self.len()? {
            3 => {
                assert_eq!(self.increments >> 24, 0);
                wr.write_u16::<LittleEndian>(self.increments as u16)?;
                wr.write_u8((self.increments >> 16) as u8)?;
            }
            4 => {
                assert_eq!(self.increments >> 32, 0);
                wr.write_u32::<LittleEndian>(self.increments as u32)?;
            }
            5 => {
                assert_eq!(self.increments >> 40, 0);
                wr.write_u32::<LittleEndian>(self.increments as u32)?;
                wr.write_u8((self.increments >> 32) as u8)?;
            }
            _ => unreachable!(),
        }
        Ok(())
    }

    pub fn decode<R: Read>(mut rd: R, n: usize, len: u8) -> Result<Time> {
        let val = match (n, len) {
            (0...2, 3) => rd.read_u16::<LittleEndian>()? as u64 | (rd.read_u8()? as u64) << 16,
            (3...4, 4) => rd.read_u32::<LittleEndian>()? as u64,
            (5...7, 5) => rd.read_u32::<LittleEndian>()? as u64 | (rd.read_u8()? as u64) << 32,
            _ => {
                return Err(Error::Protocol(
                    format!("timen: invalid length {}", n).into(),
                ))
            }
        };
        Ok(Time {
            increments: val,
            scale: n as u8,
        })
    }
}

#[derive(Copy, Clone, Debug, PartialEq)]
pub struct DateTime2(pub Date, pub Time);

to_sql!(
    DateTime => "datetime",
    SmallDateTime => "smalldatetime",
    Date => "date",
    Time => "time",
    DateTime2 => "datetime2"
);

from_column_data!(
    DateTime:           ColumnData::DateTime(dt) => dt;
    SmallDateTime:      ColumnData::SmallDateTime(dt) => dt;
    Date:               ColumnData::Date(dt) => dt;
    Time:               ColumnData::Time(t) => t;
    DateTime2:          ColumnData::DateTime2(dt) => dt
);

to_column_data!(self_,
    DateTime     =>         ColumnData::DateTime(*self_),
    SmallDateTime     =>    ColumnData::SmallDateTime(*self_),
    Date     =>             ColumnData::Date(*self_),
    Time     =>             ColumnData::Time(*self_),
    DateTime2 =>            ColumnData::DateTime2(*self_)
);

#[cfg(feature = "chrono")]
mod chrono {
    extern crate chrono;

    use self::chrono::{Duration, NaiveDate, NaiveDateTime, NaiveTime};
    use types::{ColumnData, FromColumnData, ToColumnData, ToSql};
    use super::{Date, DateTime2, Time};
    use {Error, Result};

    #[inline]
    fn from_days(days: i64, start_year: i32) -> NaiveDate {
        NaiveDate::from_ymd(start_year, 1, 1) + Duration::days(days as i64)
    }

    #[inline]
    fn from_sec_fragments(sec_fragments: i64) -> NaiveTime {
        NaiveTime::from_hms(0, 0, 0) + Duration::nanoseconds(sec_fragments * (1e9 as i64) / 300)
    }

    #[inline]
    fn from_sec(sec_fragments: u16) -> NaiveTime {
        NaiveTime::from_num_seconds_from_midnight(sec_fragments as u32, 0)
    }

    #[inline]
    fn to_days(date: &NaiveDate, start_year: i32) -> i64 {
        date.signed_duration_since(NaiveDate::from_ymd(start_year, 1, 1))
            .num_days()
    }

    /// relevant for encoding to datetime1
    #[inline]
    fn to_sec_fragments(time: &NaiveTime) -> i64 {
        time.signed_duration_since(NaiveTime::from_hms(0, 0, 0))
            .num_nanoseconds()
            .unwrap() * 300 / (1e9 as i64)
    }

    from_column_data!(
        NaiveDateTime:
            ColumnData::SmallDateTime(ref dt) => NaiveDateTime::new(
                from_days(dt.days as i64, 1900),
                from_sec(dt.seconds_fragments),
            ),
            ColumnData::DateTime(ref dt) => NaiveDateTime::new(
                from_days(dt.days as i64, 1900),
                from_sec_fragments(dt.seconds_fragments as i64)
            ),
            ColumnData::DateTime2(ref dt) => NaiveDateTime::new(
                from_days(dt.0.days() as i64, 1),
                NaiveTime::from_hms(0,0,0) + Duration::nanoseconds(dt.1.increments as i64 * 10i64.pow(9 - dt.1.scale as u32))
            );
        NaiveDate:      ColumnData::Date(ref date) => from_days(date.days() as i64, 1)
    );
    to_column_data!(self_,
        NaiveDateTime => {
            use types::time::chrono::chrono::Timelike;

            let date = self_.date();
            let time = self_.time();
            let nanos = time.num_seconds_from_midnight() as u64 * 1e9 as u64 + time.nanosecond() as u64;
            // TODO: also use datetime here for TDS<7.3
            ColumnData::DateTime2(DateTime2(Date::new(to_days(&date, 1) as u32), Time { 
                increments: nanos / 100,
                scale: 7,
            }))
        },
        NaiveDate => ColumnData::Date(Date::new(to_days(self_, 1) as u32))
    );
    to_sql!(
        NaiveDate => "date",
        // TODO: use datetime instead ( TDS < 7.3 )
        NaiveDateTime => "datetime2"
    );

    #[cfg(test)]
    mod tests {
        use futures::Future;
        use futures_state_stream::StateStream;
        use tokio::executor::current_thread;
        use tests::connection_string;
        use super::chrono::{NaiveDate, NaiveDateTime};
        use SqlConnection;

        static DATETIME_TEST_STR: &'static str = "2015-09-05 23:56:04.0100020";

        test_timedatatype!(
            test_chrono_date: NaiveDate = NaiveDate::from_ymd(1223, 11, 4) => "1223-11-04",
            test_chrono_datetime: NaiveDateTime
                =  NaiveDateTime::parse_from_str(DATETIME_TEST_STR, "%Y-%m-%d %H:%M:%S%.f").unwrap()
                => DATETIME_TEST_STR
        );

        #[test]
        fn test_datetime2_to_naive_datetime() {
            let future = SqlConnection::connect(connection_string().as_ref())
                .and_then(|conn| {
                    conn.simple_query(format!(
                        "select cast('{}' as datetime2(7))",
                        DATETIME_TEST_STR
                    )).for_each(|row| {
                            assert_eq!(
                                row.get::<_, NaiveDateTime>(0),
                                NaiveDateTime::parse_from_str(
                                    DATETIME_TEST_STR,
                                    "%Y-%m-%d %H:%M:%S%.3f"
                                ).unwrap()
                            );
                            Ok(())
                        })
                });
            current_thread::block_on_all(future).unwrap();
        }
    }
}

#[cfg(test)]
mod tests {
    use futures::Future;
    use futures_state_stream::StateStream;
    use tokio::executor::current_thread;
    use super::{Date, DateTime, DateTime2, SmallDateTime, Time};
    use SqlConnection;
    use tests::connection_string;

    test_timedatatype!(
        test_datetime: DateTime = DateTime {
            days: 41692, //24.02.2014
            seconds_fragments: (18*3600 + 42*60 + 23) * 300, // 18:42:23
        } => "2014-02-24 18:42:23.000",
        test_smalldatetime: SmallDateTime = SmallDateTime {
            days: 41692, //24.02.2014
            seconds_fragments: (12*60 + 45), // 12:45
        } => "2014-02-24 12:45:00.000",
        test_date: Date = Date::new(123) => "0001-05-04",
        test_time: Time = Time { increments: 123, scale: 5} => "00:00:00.0012300",
        test_datetime2: DateTime2 = DateTime2(Date::new(123), Time { increments: 123, scale: 5}) => "0001-05-04 00:00:00.0012300"
    );

    #[test]
    fn test_datetime_fixed() {
        let future = SqlConnection::connect(connection_string().as_ref())
            .and_then(|conn| {
                conn.simple_exec("create table #Temp(gg datetime NOT NULL)")
            })
            .and_then(|(_, conn)| {
                conn.simple_query(
                    "insert into #Temp(gg) OUTPUT Inserted.gg VALUES('2014-02-24T18:42:23.000')",
                ).for_each(|row| {
                        assert_eq!(
                            row.get::<_, DateTime>(0),
                            DateTime {
                                days: 41692,                                         //24.02.2014
                                seconds_fragments: (18 * 3600 + 42 * 60 + 23) * 300, // 18:42:23
                            }
                        );
                        Ok(())
                    })
            });
        current_thread::block_on_all(future).unwrap();
    }
}

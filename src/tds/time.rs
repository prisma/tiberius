//! Date and time handling.
//!
//! When using the `tds73` together with the `chrono` feature flag, the
//! following [`chrono`](https://crates.io/crates/chrono) mappings to and from
//! the database are available:
//!
//! - `crate::time::Time` -> `chrono::NaiveTime`
//! - `crate::time::Date` -> `chrono::NaiveDate`
//! - `crate::time::DateTime` -> `chrono::NaiveDateTime`
//! - `crate::time::DateTime2` -> `chrono::NaiveDateTime`
//! - `crate::time::SmallDateTime` -> `chrono::NaiveDateTime`
//! - `crate::time::DateTimeOffset` -> `chrono::DateTime<Tz>`

use crate::{tds::codec::Encode, SqlReadBytes};
#[cfg(feature = "tds73")]
use byteorder::{ByteOrder, LittleEndian};
use bytes::{BufMut, BytesMut};
#[cfg(feature = "tds73")]
use futures::io::AsyncReadExt;

/// A presentation of `datetime` type in the server.
///
/// # Warning
///
/// It isn't recommended to use this type directly. For dealing with `datetime`,
/// use the `chrono` feature of this crate and its `NaiveDateTime` type.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct DateTime {
    days: i32,
    seconds_fragments: u32,
}

impl DateTime {
    /// Construct a new `DateTime` instance.
    pub fn new(days: i32, seconds_fragments: u32) -> Self {
        Self {
            days,
            seconds_fragments,
        }
    }

    /// Days since 1st of January, 1900 (including the negative range until 1st
    /// of January, 1753).
    pub fn days(self) -> i32 {
        self.days
    }

    /// 1/300 of a second, so a value of 300 equals 1 second (since midnight).
    pub fn seconds_fragments(self) -> u32 {
        self.seconds_fragments
    }

    pub(crate) async fn decode<R>(src: &mut R) -> crate::Result<Self>
    where
        R: SqlReadBytes + Unpin,
    {
        Ok(Self {
            days: src.read_i32_le().await?,
            seconds_fragments: src.read_u32_le().await?,
        })
    }
}

impl Encode<BytesMut> for DateTime {
    fn encode(self, dst: &mut BytesMut) -> crate::Result<()> {
        dst.put_i32_le(self.days);
        dst.put_u32_le(self.seconds_fragments);

        Ok(())
    }
}

/// A presentation of `smalldatetime` type in the server.
///
/// # Warning
///
/// It isn't recommended to use this type directly. For dealing with
/// `smalldatetime`, use the `chrono` feature of this crate and its
/// `NaiveDateTime` type.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct SmallDateTime {
    days: u16,
    seconds_fragments: u16,
}

impl SmallDateTime {
    /// Construct a new `SmallDateTime` instance.
    pub fn new(days: u16, seconds_fragments: u16) -> Self {
        Self {
            days,
            seconds_fragments,
        }
    }
    /// Days since 1st of January, 1900.
    pub fn days(self) -> u16 {
        self.days
    }

    /// 1/300 of a second, so a value of 300 equals 1 second (since midnight)
    pub fn seconds_fragments(self) -> u16 {
        self.seconds_fragments
    }

    pub(crate) async fn decode<R>(src: &mut R) -> crate::Result<Self>
    where
        R: SqlReadBytes + Unpin,
    {
        Ok(Self {
            days: src.read_u16_le().await?,
            seconds_fragments: src.read_u16_le().await?,
        })
    }
}

impl Encode<BytesMut> for SmallDateTime {
    fn encode(self, dst: &mut BytesMut) -> crate::Result<()> {
        dst.put_u16_le(self.days);
        dst.put_u16_le(self.seconds_fragments);

        Ok(())
    }
}

/// A presentation of `date` type in the server.
///
/// # Warning
///
/// It isn't recommended to use this type directly. If you want to deal with
/// `date`, use the chrono feature of this crate and its `NaiveDate` type.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
#[cfg(feature = "tds73")]
pub struct Date(u32);

#[cfg(feature = "tds73")]
impl Date {
    #[inline]
    /// Construct a new `Date`
    ///
    /// # Panics
    /// max value of 3 bytes (`u32::max_value() > 8`)
    pub fn new(days: u32) -> Date {
        assert_eq!(days >> 24, 0);
        Date(days)
    }

    #[inline]
    /// The number of days from 1st of January, year 1.
    pub fn days(self) -> u32 {
        self.0
    }

    pub(crate) async fn decode<R>(src: &mut R) -> crate::Result<Self>
    where
        R: SqlReadBytes + Unpin,
    {
        let mut bytes = [0u8; 4];
        src.read_exact(&mut bytes[..3]).await?;
        Ok(Self::new(LittleEndian::read_u32(&bytes)))
    }
}

#[cfg(feature = "tds73")]
impl Encode<BytesMut> for Date {
    fn encode(self, dst: &mut BytesMut) -> crate::Result<()> {
        let mut tmp = [0u8; 4];
        LittleEndian::write_u32(&mut tmp, self.days());
        assert_eq!(tmp[3], 0);
        dst.extend_from_slice(&tmp[0..3]);

        Ok(())
    }
}

/// A presentation of `time` type in the server.
///
/// # Warning
///
/// It isn't recommended to use this type directly. If you want to deal with
/// `time`, use the chrono feature of this crate and its `NaiveTime` type.
#[derive(Copy, Clone, Debug)]
#[cfg(feature = "tds73")]
pub struct Time {
    increments: u64,
    scale: u8,
}

#[cfg(feature = "tds73")]
impl PartialEq for Time {
    fn eq(&self, t: &Time) -> bool {
        self.increments as f64 / 10f64.powi(self.scale as i32)
            == t.increments as f64 / 10f64.powi(t.scale as i32)
    }
}

#[cfg(feature = "tds73")]
impl Time {
    /// Construct a new `Time`
    pub fn new(increments: u64, scale: u8) -> Self {
        Self { increments, scale }
    }

    #[inline]
    /// Number of 10^-n second increments since midnight, where `n` is defined
    /// in [`scale`].
    ///
    /// [`scale`]: #method.scale
    pub fn increments(self) -> u64 {
        self.increments
    }

    #[inline]
    /// The accuracy of the increments.
    pub fn scale(self) -> u8 {
        self.scale
    }

    #[inline]
    /// Length of the field in number of bytes.
    pub(crate) fn len(self) -> crate::Result<u8> {
        Ok(match self.scale {
            0..=2 => 3,
            3..=4 => 4,
            5..=7 => 5,
            _ => {
                return Err(crate::Error::Protocol(
                    format!("timen: invalid scale {}", self.scale).into(),
                ))
            }
        })
    }

    pub(crate) async fn decode<R>(src: &mut R, n: usize, rlen: usize) -> crate::Result<Time>
    where
        R: SqlReadBytes + Unpin,
    {
        let val = match (n, rlen) {
            (0..=2, 3) => src.read_u16_le().await? as u64 | (src.read_u8().await? as u64) << 16,
            (3..=4, 4) => src.read_u32_le().await? as u64,
            (5..=7, 5) => src.read_u32_le().await? as u64 | (src.read_u8().await? as u64) << 32,
            _ => {
                return Err(crate::Error::Protocol(
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

#[cfg(feature = "tds73")]
impl Encode<BytesMut> for Time {
    fn encode(self, dst: &mut BytesMut) -> crate::Result<()> {
        match self.len()? {
            3 => {
                assert_eq!(self.increments >> 24, 0);
                dst.put_u16_le(self.increments as u16);
                dst.put_u8((self.increments >> 16) as u8);
            }
            4 => {
                assert_eq!(self.increments >> 32, 0);
                dst.put_u32_le(self.increments as u32);
            }
            5 => {
                assert_eq!(self.increments >> 40, 0);
                dst.put_u32_le(self.increments as u32);
                dst.put_u8((self.increments >> 32) as u8);
            }
            _ => unreachable!(),
        }

        Ok(())
    }
}

#[derive(Copy, Clone, Debug, PartialEq)]
#[cfg(feature = "tds73")]
/// A presentation of `datetime2` type in the server.
///
/// # Warning
///
/// It isn't recommended to use this type directly. For dealing with
/// `datetime2`, use the `chrono` feature of this crate and its `NaiveDateTime`
/// type.
pub struct DateTime2 {
    date: Date,
    time: Time,
}

#[cfg(feature = "tds73")]
impl DateTime2 {
    /// Construct a new `DateTime2` from the date and time components.
    pub fn new(date: Date, time: Time) -> Self {
        Self { date, time }
    }

    /// The date component.
    pub fn date(self) -> Date {
        self.date
    }

    /// The time component.
    pub fn time(self) -> Time {
        self.time
    }

    pub(crate) async fn decode<R>(src: &mut R, n: usize, rlen: usize) -> crate::Result<Self>
    where
        R: SqlReadBytes + Unpin,
    {
        let time = Time::decode(src, n, rlen as usize).await?;

        let mut bytes = [0u8; 4];
        src.read_exact(&mut bytes[..3]).await?;
        let date = Date::new(LittleEndian::read_u32(&bytes));

        Ok(Self::new(date, time))
    }
}

#[cfg(feature = "tds73")]
impl Encode<BytesMut> for DateTime2 {
    fn encode(self, dst: &mut BytesMut) -> crate::Result<()> {
        self.time.encode(dst)?;

        let mut tmp = [0u8; 4];
        LittleEndian::write_u32(&mut tmp, self.date.days());
        assert_eq!(tmp[3], 0);
        dst.extend_from_slice(&tmp[0..3]);

        Ok(())
    }
}

#[derive(Copy, Clone, Debug, PartialEq)]
#[cfg(feature = "tds73")]
/// A presentation of `datetimeoffset` type in the server.
///
/// # Warning
///
/// It isn't recommended to use this type directly. For dealing with
/// `datetimeoffset`, use the `chrono` feature of this crate and its `DateTime`
/// type with the correct timezone.
pub struct DateTimeOffset {
    datetime2: DateTime2,
    offset: i16,
}

#[cfg(feature = "tds73")]
impl DateTimeOffset {
    /// Construct a new `DateTimeOffset` from a `datetime2`, offset marking
    /// number of minutes from UTC.
    pub fn new(datetime2: DateTime2, offset: i16) -> Self {
        Self { datetime2, offset }
    }

    /// The date and time part.
    pub fn datetime2(self) -> DateTime2 {
        self.datetime2
    }

    /// Number of minutes from UTC.
    pub fn offset(self) -> i16 {
        self.offset
    }

    pub(crate) async fn decode<R>(src: &mut R, n: usize) -> crate::Result<Self>
    where
        R: SqlReadBytes + Unpin,
    {
        let rlen = src.read_u8().await? - 5;
        let datetime2 = DateTime2::decode(src, n, rlen as usize).await?;
        let offset = src.read_i16_le().await?;

        Ok(Self { datetime2, offset })
    }
}

#[cfg(feature = "tds73")]
impl Encode<BytesMut> for DateTimeOffset {
    fn encode(self, dst: &mut BytesMut) -> crate::Result<()> {
        self.datetime2.encode(dst)?;
        dst.put_i16_le(self.offset);

        Ok(())
    }
}

#[cfg(feature = "chrono")]
mod chrono {
    #[cfg(not(feature = "tds73"))]
    use super::DateTime;
    #[cfg(feature = "tds73")]
    use super::{Date, DateTime2, DateTimeOffset, Time};
    use crate::tds::codec::ColumnData;
    #[cfg(feature = "tds73")]
    use chrono::offset::{FixedOffset, Utc};
    use chrono::{NaiveDate, NaiveDateTime, NaiveTime};

    #[inline]
    fn from_days(days: i64, start_year: i32) -> NaiveDate {
        NaiveDate::from_ymd(start_year, 1, 1) + chrono::Duration::days(days as i64)
    }

    #[inline]
    fn from_sec_fragments(sec_fragments: i64) -> NaiveTime {
        NaiveTime::from_hms(0, 0, 0)
            + chrono::Duration::nanoseconds(sec_fragments * (1e9 as i64) / 300)
    }

    #[inline]
    #[cfg(feature = "tds73")]
    fn from_mins(mins: u32) -> NaiveTime {
        NaiveTime::from_num_seconds_from_midnight(mins, 0)
    }

    #[inline]
    fn to_days(date: NaiveDate, start_year: i32) -> i64 {
        date.signed_duration_since(NaiveDate::from_ymd(start_year, 1, 1))
            .num_days()
    }

    #[inline]
    #[cfg(not(feature = "tds73"))]
    fn to_sec_fragments(time: NaiveTime) -> i64 {
        time.signed_duration_since(NaiveTime::from_hms(0, 0, 0))
            .num_nanoseconds()
            .unwrap()
            * 300
            / (1e9 as i64)
    }

    #[cfg(feature = "tds73")]
    from_sql!(
        NaiveDateTime:
            ColumnData::SmallDateTime(ref dt) => dt.map(|dt| NaiveDateTime::new(
                from_days(dt.days as i64, 1900),
                from_mins(dt.seconds_fragments as u32 * 60),
            )),
            ColumnData::DateTime2(ref dt) => dt.map(|dt| NaiveDateTime::new(
                from_days(dt.date.days() as i64, 1),
                NaiveTime::from_hms(0,0,0) + chrono::Duration::nanoseconds(dt.time.increments as i64 * 10i64.pow(9 - dt.time.scale as u32))
            )),
            ColumnData::DateTime(ref dt) => dt.map(|dt| NaiveDateTime::new(
                from_days(dt.days as i64, 1900),
                from_sec_fragments(dt.seconds_fragments as i64)
            ));
        NaiveTime:
            ColumnData::Time(ref time) => time.map(|time| {
                let ns = time.increments as i64 * 10i64.pow(9 - time.scale as u32);
                NaiveTime::from_hms(0,0,0) + chrono::Duration::nanoseconds(ns)
            });
        NaiveDate:
            ColumnData::Date(ref date) => date.map(|date| from_days(date.days() as i64, 1));
        chrono::DateTime<Utc>:
            ColumnData::DateTimeOffset(ref dto) => dto.map(|dto| {
                let date = from_days(dto.datetime2.date.days() as i64, 1);
                let ns = dto.datetime2.time.increments as i64 * 10i64.pow(9 - dto.datetime2.time.scale as u32);

                let time = NaiveTime::from_hms(0,0,0) + chrono::Duration::nanoseconds(ns) - chrono::Duration::minutes(dto.offset as i64);
                let naive = NaiveDateTime::new(date, time);

                chrono::DateTime::from_utc(naive, Utc)
            });
        chrono::DateTime<FixedOffset>: ColumnData::DateTimeOffset(ref dto) => dto.map(|dto| {
            let date = from_days(dto.datetime2.date.days() as i64, 1);
            let ns = dto.datetime2.time.increments as i64 * 10i64.pow(9 - dto.datetime2.time.scale as u32);
            let time = NaiveTime::from_hms(0,0,0) + chrono::Duration::nanoseconds(ns) - chrono::Duration::minutes(dto.offset as i64);

            let offset = FixedOffset::east((dto.offset as i32) * 60);
            let naive = NaiveDateTime::new(date, time);

            chrono::DateTime::from_utc(naive, offset)
        })
    );

    #[cfg(feature = "tds73")]
    to_sql!(self_,
            NaiveDate: (ColumnData::Date, Date::new(to_days(*self_, 1) as u32));
            NaiveTime: (ColumnData::Time, {
                use chrono::Timelike;

                let nanos = self_.num_seconds_from_midnight() as u64 * 1e9 as u64 + self_.nanosecond() as u64;
                let increments = nanos / 100;

                Time {increments, scale: 7}
            });
            NaiveDateTime: (ColumnData::DateTime2, {
                use chrono::Timelike;

                let time = self_.time();
                let nanos = time.num_seconds_from_midnight() as u64 * 1e9 as u64 + time.nanosecond() as u64;
                let increments = nanos / 100;

                let date = Date::new(to_days(self_.date(), 1) as u32);
                let time = Time {increments, scale: 7};

                DateTime2::new(date, time)
            });
            chrono::DateTime<Utc>: (ColumnData::DateTimeOffset, {
                use chrono::Timelike;

                let naive = self_.naive_utc();
                let time = naive.time();
                let nanos = time.num_seconds_from_midnight() as u64 * 1e9 as u64 + time.nanosecond() as u64;

                let date = Date::new(to_days(naive.date(), 1) as u32);
                let time = Time {increments: nanos / 100, scale: 7};

                DateTimeOffset::new(DateTime2::new(date, time), 0)
            });
            chrono::DateTime<FixedOffset>: (ColumnData::DateTimeOffset, {
                use chrono::Timelike;

                let naive = self_.naive_local();
                let time = naive.time();
                let nanos = time.num_seconds_from_midnight() as u64 * 1e9 as u64 + time.nanosecond() as u64;

                let date = Date::new(to_days(naive.date(), 1) as u32);
                let time = Time { increments: nanos / 100, scale: 7 };

                let tz = self_.timezone();
                let offset = (tz.local_minus_utc() / 60) as i16;

                DateTimeOffset::new(DateTime2::new(date, time), offset)
            });
    );

    #[cfg(not(feature = "tds73"))]
    to_sql!(self_,
            NaiveDateTime: (ColumnData::DateTime, {
                let date = self_.date();
                let time = self_.time();

                let days = to_days(date, 1900) as i32;
                let seconds_fragments = to_sec_fragments(time);

                DateTime::new(days, seconds_fragments as u32)
            });
    );

    #[cfg(not(feature = "tds73"))]
    from_sql!(
        NaiveDateTime:
            ColumnData::DateTime(ref dt) => dt.map(|dt| NaiveDateTime::new(
                from_days(dt.days as i64, 1900),
                from_sec_fragments(dt.seconds_fragments as i64)
            ))
    );
}

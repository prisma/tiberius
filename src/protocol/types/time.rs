use crate::{async_read_le_ext::AsyncReadLeExt, protocol::codec::Encode};
#[cfg(feature = "tds73")]
use byteorder::{ByteOrder, LittleEndian};
use bytes::{BufMut, BytesMut};
#[cfg(feature = "tds73")]
use tokio::io::AsyncReadExt;

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

impl DateTime {
    #[allow(dead_code)]
    pub(crate) fn new(days: i32, seconds_fragments: u32) -> Self {
        Self {
            days,
            seconds_fragments,
        }
    }

    pub(crate) async fn decode<R>(src: &mut R) -> crate::Result<Self>
    where
        R: AsyncReadLeExt + Unpin,
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

impl SmallDateTime {
    pub(crate) async fn decode<R>(src: &mut R) -> crate::Result<Self>
    where
        R: AsyncReadLeExt + Unpin,
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
#[cfg(feature = "tds73")]
pub struct Date(u32);

#[cfg(feature = "tds73")]
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

    pub(crate) async fn decode<R>(src: &mut R) -> crate::Result<Self>
    where
        R: AsyncReadLeExt + Unpin,
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

/// Number of 10^-n second increments since 12AM
#[derive(Copy, Clone, Debug)]
#[cfg(feature = "tds73")]
pub struct Time {
    pub increments: u64,
    pub scale: u8,
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
    #[inline]
    pub fn len(&self) -> crate::Result<u8> {
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
        R: AsyncReadLeExt + Unpin,
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
pub struct DateTime2 {
    pub date: Date,
    pub time: Time,
    pub offset: Option<i16>,
}

#[cfg(feature = "tds73")]
impl DateTime2 {
    pub(crate) fn new(date: Date, time: Time) -> Self {
        Self {
            date,
            time,
            offset: None,
        }
    }

    pub(crate) async fn decode<R>(src: &mut R, n: usize, rlen: usize) -> crate::Result<Self>
    where
        R: AsyncReadLeExt + Unpin,
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
pub struct DateTimeOffset {
    pub datetime2: DateTime2,
    pub offset: i16,
}

#[cfg(feature = "tds73")]
impl DateTimeOffset {
    pub(crate) fn new(datetime2: DateTime2, offset: i16) -> Self {
        Self { datetime2, offset }
    }

    pub(crate) async fn decode<R>(src: &mut R, n: usize) -> crate::Result<Self>
    where
        R: AsyncReadLeExt + Unpin,
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
    use crate::{from_column_data, protocol::codec::ColumnData, to_sql};
    #[cfg(feature = "tds73")]
    use chrono::offset::{FixedOffset, Utc};
    use chrono::{Duration, NaiveDate, NaiveDateTime, NaiveTime};

    #[inline]
    fn from_days(days: i64, start_year: i32) -> NaiveDate {
        NaiveDate::from_ymd(start_year, 1, 1) + Duration::days(days as i64)
    }

    #[inline]
    fn from_sec_fragments(sec_fragments: i64) -> NaiveTime {
        NaiveTime::from_hms(0, 0, 0) + Duration::nanoseconds(sec_fragments * (1e9 as i64) / 300)
    }

    #[inline]
    #[cfg(feature = "tds73")]
    fn from_mins(mins: u32) -> NaiveTime {
        NaiveTime::from_num_seconds_from_midnight(mins, 0)
    }

    #[inline]
    fn to_days(date: &NaiveDate, start_year: i32) -> i64 {
        date.signed_duration_since(NaiveDate::from_ymd(start_year, 1, 1))
            .num_days()
    }

    #[inline]
    #[cfg(not(feature = "tds73"))]
    fn to_sec_fragments(time: &NaiveTime) -> i64 {
        time.signed_duration_since(NaiveTime::from_hms(0, 0, 0))
            .num_nanoseconds()
            .unwrap()
            * 300
            / (1e9 as i64)
    }

    #[cfg(feature = "tds73")]
    from_column_data!(
        NaiveDateTime:
            ColumnData::SmallDateTime(ref dt) => NaiveDateTime::new(
                from_days(dt.days as i64, 1900),
                from_mins(dt.seconds_fragments as u32 * 60),
            ),
            ColumnData::DateTime2(ref dt) => NaiveDateTime::new(
                from_days(dt.date.days() as i64, 1),
                NaiveTime::from_hms(0,0,0) + Duration::nanoseconds(dt.time.increments as i64 * 10i64.pow(9 - dt.time.scale as u32))
            ),
            ColumnData::DateTime(ref dt) => NaiveDateTime::new(
                from_days(dt.days as i64, 1900),
                from_sec_fragments(dt.seconds_fragments as i64)
            );
        NaiveTime:
            ColumnData::Time(ref time) => {
                let ns = time.increments as i64 * 10i64.pow(9 - time.scale as u32);
                NaiveTime::from_hms(0,0,0) + Duration::nanoseconds(ns)
            };
        NaiveDate:
            ColumnData::Date(ref date) => from_days(date.days() as i64, 1);
        chrono::DateTime<Utc>:
            ColumnData::DateTimeOffset(ref dto) => {
                let date = from_days(dto.datetime2.date.days() as i64, 1);
                let ns = dto.datetime2.time.increments as i64 * 10i64.pow(9 - dto.datetime2.time.scale as u32);

                let time = NaiveTime::from_hms(0,0,0) + Duration::nanoseconds(ns) - Duration::minutes(dto.offset as i64);
                let naive = NaiveDateTime::new(date, time);

                chrono::DateTime::from_utc(naive, Utc)
            };
        chrono::DateTime<FixedOffset>: ColumnData::DateTimeOffset(ref dto) => {
            let date = from_days(dto.datetime2.date.days() as i64, 1);
            let ns = dto.datetime2.time.increments as i64 * 10i64.pow(9 - dto.datetime2.time.scale as u32);
            let time = NaiveTime::from_hms(0,0,0) + Duration::nanoseconds(ns) - Duration::minutes(dto.offset as i64);

            let offset = FixedOffset::east((dto.offset as i32) * 60);
            let naive = NaiveDateTime::new(date, time);

            chrono::DateTime::from_utc(naive, offset)
        }
    );

    #[cfg(feature = "tds73")]
    to_sql!(self_,
            NaiveDate: ("date", ColumnData::Date(Date::new(to_days(self_, 1) as u32)));
            NaiveTime: ("time", {
                use chrono::Timelike;

                let nanos = self_.num_seconds_from_midnight() as u64 * 1e9 as u64 + self_.nanosecond() as u64;
                let increments = nanos / 100;

                ColumnData::Time(Time {increments, scale: 7})
            });
            NaiveDateTime: ("datetime2", {
                use chrono::Timelike;

                let time = self_.time();
                let nanos = time.num_seconds_from_midnight() as u64 * 1e9 as u64 + time.nanosecond() as u64;
                let increments = nanos / 100;

                let date = Date::new(to_days(&self_.date(), 1) as u32);
                let time = Time {increments, scale: 7};

                ColumnData::DateTime2(DateTime2::new(date, time))
            });
            chrono::DateTime<Utc>: ("datetimeoffset", {
                use chrono::Timelike;

                let naive = self_.naive_utc();
                let time = naive.time();
                let nanos = time.num_seconds_from_midnight() as u64 * 1e9 as u64 + time.nanosecond() as u64;

                let date = Date::new(to_days(&naive.date(), 1) as u32);
                let time = Time {increments: nanos / 100, scale: 7};

                ColumnData::DateTimeOffset(DateTimeOffset::new(DateTime2::new(date, time), 0))
            });
            chrono::DateTime<FixedOffset>: ("datetimeoffset", {
                use chrono::Timelike;

                let naive = self_.naive_local();
                let time = naive.time();
                let nanos = time.num_seconds_from_midnight() as u64 * 1e9 as u64 + time.nanosecond() as u64;

                let date = Date::new(to_days(&naive.date(), 1) as u32);
                let time = Time { increments: nanos / 100, scale: 7 };

                let tz = self_.timezone();
                let offset = (tz.local_minus_utc() / 60) as i16;

                ColumnData::DateTimeOffset(DateTimeOffset::new(DateTime2::new(date, time), offset))
            });
    );

    #[cfg(not(feature = "tds73"))]
    to_sql!(self_,
            NaiveDateTime: ("datetime", {
                let date = self_.date();
                let time = self_.time();

                let days = to_days(&date, 1900) as i32;
                let seconds_fragments = to_sec_fragments(&time);

                ColumnData::DateTime(DateTime::new(days, seconds_fragments as u32))
            });
    );
}

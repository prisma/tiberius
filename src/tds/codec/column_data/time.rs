use crate::{sql_read_bytes::SqlReadBytes, sql_time::Time, ColumnData};

pub(crate) async fn decode<R>(src: &mut R, len: usize) -> crate::Result<ColumnData<'static>>
where
    R: SqlReadBytes + Unpin,
{
    let rlen = src.read_u8().await?;

    let time = match rlen {
        0 => ColumnData::Time(None),
        _ => {
            let time = Time::decode(src, len as usize, rlen as usize).await?;
            ColumnData::Time(Some(time))
        }
    };

    Ok(time)
}

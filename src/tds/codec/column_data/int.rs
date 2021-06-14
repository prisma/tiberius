use crate::{sql_read_bytes::SqlReadBytes, ColumnData};

pub(crate) async fn decode<R>(src: &mut R) -> crate::Result<ColumnData<'static>>
where
    R: SqlReadBytes + Unpin,
{
    let recv_len = src.read_u8().await? as usize;

    let res = match recv_len {
        0 => ColumnData::U8(None),
        1 => ColumnData::U8(Some(src.read_u8().await?)),
        2 => ColumnData::I16(Some(src.read_i16_le().await?)),
        4 => ColumnData::I32(Some(src.read_i32_le().await?)),
        8 => ColumnData::I64(Some(src.read_i64_le().await?)),
        _ => unimplemented!(),
    };

    Ok(res)
}

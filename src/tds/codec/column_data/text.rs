use encoding::DecoderTrap;

use crate::{error::Error, sql_read_bytes::SqlReadBytes, tds::Collation, ColumnData};

pub(crate) async fn decode<R>(
    src: &mut R,
    collation: Option<Collation>,
) -> crate::Result<ColumnData<'static>>
where
    R: SqlReadBytes + Unpin,
{
    let ptr_len = src.read_u8().await? as usize;

    if ptr_len == 0 {
        return Ok(ColumnData::String(None));
    }

    for _ in 0..ptr_len {
        src.read_u8().await?;
    }

    src.read_i32_le().await?; // days
    src.read_u32_le().await?; // second fractions

    let text = match collation {
        // TEXT
        Some(collation) => {
            let encoder = collation.encoding()?;
            let text_len = src.read_u32_le().await? as usize;
            let mut buf = Vec::with_capacity(text_len);

            for _ in 0..text_len {
                buf.push(src.read_u8().await?);
            }

            encoder
                .decode(buf.as_ref(), DecoderTrap::Strict)
                .map_err(Error::Encoding)?
        }
        // NTEXT
        None => {
            let text_len = src.read_u32_le().await? as usize / 2;
            let mut buf = Vec::with_capacity(text_len);

            for _ in 0..text_len {
                buf.push(src.read_u16_le().await?);
            }

            String::from_utf16(&buf[..])?
        }
    };

    Ok(ColumnData::String(Some(text.into())))
}

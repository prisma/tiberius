use std::borrow::Cow;

use byteorder::{ByteOrder, LittleEndian};
use encoding::DecoderTrap;

use crate::{error::Error, sql_read_bytes::SqlReadBytes, tds::Collation, VarLenType};

pub(crate) async fn decode<R>(
    src: &mut R,
    ty: VarLenType,
    len: usize,
    collation: Option<Collation>,
) -> crate::Result<Option<Cow<'static, str>>>
where
    R: SqlReadBytes + Unpin,
{
    use VarLenType::*;

    let data = super::plp::decode(src, len).await?;
    dbg!(&data);

    match (data, ty) {
        // Codepages other than UTF
        (Some(buf), BigChar) | (Some(buf), BigVarChar) => {
            let collation = collation.as_ref().unwrap();
            let encoder = collation.encoding()?;

            let s: String = encoder
                .decode(buf.as_ref(), DecoderTrap::Strict)
                .map_err(Error::Encoding)?;
            dbg!(&s);

            Ok(Some(s.into()))
        }
        // UTF-16
        (Some(buf), _) => {
            if buf.len() % 2 != 0 {
                return Err(Error::Protocol("nvarchar: invalid plp length".into()));
            }

            let buf: Vec<_> = buf.chunks(2).map(LittleEndian::read_u16).collect();
            Ok(Some(String::from_utf16(&buf)?.into()))
        }
        _ => Ok(None),
    }
}

use crate::protocol::codec::Decode;
use bytes::{Buf, BytesMut};

#[derive(Debug)]
pub struct TokenOrder {
    pub(crate) column_indexes: Vec<u16>,
}

impl Decode<BytesMut> for TokenOrder {
    fn decode(src: &mut BytesMut) -> crate::Result<Self>
    where
        Self: Sized,
    {
        let len = src.get_u16_le() / 2;

        let mut column_indexes = Vec::with_capacity(len as usize);

        for _ in 0..len {
            column_indexes.push(src.get_u16_le());
        }

        Ok(TokenOrder { column_indexes })
    }
}

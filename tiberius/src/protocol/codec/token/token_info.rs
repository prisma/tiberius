use crate::protocol::codec::{read_varchar, Decode};
use bytes::{Buf, BytesMut};

#[derive(Debug)]
pub struct TokenInfo {
    /// info number
    pub(crate) number: u32,
    /// error state
    pub(crate) state: u8,
    /// severity (<10: Info)
    pub(crate) class: u8,
    pub(crate) message: String,
    pub(crate) server: String,
    pub(crate) procedure: String,
    pub(crate) line: u32,
}

impl Decode<BytesMut> for TokenInfo {
    fn decode(src: &mut BytesMut) -> crate::Result<Self>
    where
        Self: Sized,
    {
        let _length = src.get_u16_le();

        let token = TokenInfo {
            number: src.get_u32_le(),
            state: src.get_u8(),
            class: src.get_u8(),
            message: {
                let len = src.get_u16_le();
                read_varchar(src, len)?
            },
            server: {
                let len = src.get_u8();
                read_varchar(src, len)?
            },
            procedure: {
                let len = src.get_u8();
                read_varchar(src, len)?
            },
            line: src.get_u32_le(),
        };

        Ok(token)
    }
}

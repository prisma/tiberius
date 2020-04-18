use super::Decode;
use bytes::{Buf, BytesMut};

#[derive(Debug)]
pub struct TokenSSPI(Vec<u8>);

impl AsRef<[u8]> for TokenSSPI {
    fn as_ref(&self) -> &[u8] {
        self.0.as_ref()
    }
}

impl Decode<BytesMut> for TokenSSPI {
    fn decode(src: &mut BytesMut) -> crate::Result<Self>
    where
        Self: Sized,
    {
        let len = src.get_u16_le();

        Ok(Self(src.split_to(len as usize).to_vec()))
    }
}

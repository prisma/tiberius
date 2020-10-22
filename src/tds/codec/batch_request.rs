use super::{AllHeaderTy, Encode, ALL_HEADERS_LEN_TX};
use bytes::{BufMut, BytesMut};
use std::borrow::Cow;

pub struct BatchRequest<'a> {
    queries: Cow<'a, str>,
    transaction_id: u64,
}

impl<'a> BatchRequest<'a> {
    pub fn new(queries: impl Into<Cow<'a, str>>, transaction_id: u64) -> Self {
        Self {
            queries: queries.into(),
            transaction_id,
        }
    }
}

impl<'a> Encode<BytesMut> for BatchRequest<'a> {
    fn encode(self, dst: &mut BytesMut) -> crate::Result<()> {
        dst.put_u32_le(ALL_HEADERS_LEN_TX as u32);
        dst.put_u32_le(ALL_HEADERS_LEN_TX as u32 - 4);
        dst.put_u16_le(AllHeaderTy::TransactionDescriptor as u16);
        dst.put_u64_le(self.transaction_id);
        dst.put_u32_le(1);

        for c in self.queries.encode_utf16() {
            dst.put_u16_le(c);
        };

        Ok(())
    }
}

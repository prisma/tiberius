use super::{Packet, PacketCodec};
use bytes::{BufMut, BytesMut};
use futures_codec::Encoder;

pub(crate) trait Encode<B: BufMut> {
    fn encode(self, dst: &mut B) -> crate::Result<()>;
}

impl Encoder for PacketCodec {
    type Item = Packet;
    type Error = crate::Error;

    fn encode(&mut self, item: Self::Item, dst: &mut BytesMut) -> Result<(), Self::Error> {
        item.encode(dst)?;
        Ok(())
    }
}

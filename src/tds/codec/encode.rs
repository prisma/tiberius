use super::{Packet, PacketCodec};
use bytes::{BufMut, BytesMut};
use asynchronous_codec::Encoder;

pub(crate) trait Encode<B: BufMut> {
    fn encode(self, dst: &mut B) -> crate::Result<()>;
}

impl Encoder for PacketCodec {
    type Item = Packet;
    type Error = crate::Error;

    fn encode(&mut self, item: Packet, dst: &mut BytesMut) -> Result<(), Self::Error> {
        item.encode(dst)?;
        Ok(())
    }
}

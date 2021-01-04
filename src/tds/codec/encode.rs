use super::{Packet, PacketCodec};
use bytes::{BufMut, BytesMut};
use futures_codec2::Encoder;

pub(crate) trait Encode<B: BufMut> {
    fn encode(self, dst: &mut B) -> crate::Result<()>;
}

impl Encoder<Packet> for PacketCodec {
    type Error = crate::Error;

    fn encode(&mut self, item: Packet, dst: &mut BytesMut) -> Result<(), Self::Error> {
        item.encode(dst)?;
        Ok(())
    }
}

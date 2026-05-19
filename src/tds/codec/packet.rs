use super::{Decode, Encode, PacketHeader, PacketStatus, HEADER_BYTES};
use bytes::BytesMut;

#[derive(Debug)]
pub struct Packet {
    pub(crate) header: PacketHeader,
    pub(crate) payload: BytesMut,
}

impl Packet {
    pub(crate) fn new(header: PacketHeader, payload: BytesMut) -> Self {
        Self { header, payload }
    }

    pub(crate) fn is_last(&self) -> bool {
        self.header.status() == PacketStatus::EndOfMessage
    }

    pub(crate) fn into_parts(self) -> (PacketHeader, BytesMut) {
        (self.header, self.payload)
    }
}

impl Encode<BytesMut> for Packet {
    fn encode(self, dst: &mut BytesMut) -> crate::Result<()> {
        let size = (self.payload.len() as u16 + HEADER_BYTES as u16).to_be_bytes();

        // Remember offset where this packet starts. When multiple packets
        // are buffered before a flush (pipelining), dst may already contain
        // previously encoded packets.
        let offset = dst.len();

        self.header.encode(dst)?;
        dst.extend(self.payload);

        // Patch the length field (bytes 2-3 of the TDS header) at the
        // correct position within this packet, not at absolute buffer start.
        dst[offset + 2] = size[0];
        dst[offset + 3] = size[1];

        Ok(())
    }
}

impl Decode<BytesMut> for Packet {
    fn decode(src: &mut BytesMut) -> crate::Result<Self> {
        Ok(Self {
            header: PacketHeader::decode(src)?,
            payload: src.split(),
        })
    }
}

impl Extend<u8> for Packet {
    fn extend<T: IntoIterator<Item = u8>>(&mut self, iter: T) {
        self.payload.extend(iter)
    }
}

impl<'a> Extend<&'a u8> for Packet {
    fn extend<T: IntoIterator<Item = &'a u8>>(&mut self, iter: T) {
        self.payload.extend(iter)
    }
}

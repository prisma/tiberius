use std::borrow::Cow;

use bitflags::bitflags;
use byteorder::LittleEndian;
use tokio::io::AsyncWrite;

use crate::protocol::{
    self, types::ColumnData, PacketHeader, PacketStatus, PacketType, PacketWriter,
};
use crate::{Error, Result};

/// 2.2.6.6 RPC Request
#[allow(dead_code)]
#[repr(u8)]
#[derive(Clone, Copy, Debug)]
pub enum RpcProcId {
    SpCursorOpen = 2,
    SpCursorFetch = 7,
    SpCursorClose = 9,
    SpExecuteSQL = 10,
    SpPrepare = 11,
    SpExecute = 12,
    SpPrepExec = 13,
    SpUnprepare = 15,
}

#[derive(Debug)]
#[allow(dead_code)]
pub enum RpcProcIdValue<'a> {
    Name(Cow<'a, str>),
    Id(RpcProcId),
}

bitflags! {
    pub struct RpcStatusFlags: u8 {
        const PARAM_BY_REF_VALUE    = 0x01;
        const PARAM_DEFAULT_VALUE   = 0x02;
        // <- reserved
        const PARAM_ENCRYPTED       = 0x08;
        // <- 4 bits reserved
    }
}

bitflags! {
    pub struct RpcOptionFlags: u16 {
        const WITH_RECOMP   = 0x01;
        const NO_META       = 0x02;
        const REUSE_META    = 0x04;
        // <- 13 reserved bits
    }
}

#[derive(Debug)]
pub struct RpcParam<'a> {
    pub name: Cow<'a, str>,
    pub flags: RpcStatusFlags,
    pub value: ColumnData,
}

#[derive(Debug)]
pub struct TokenRpcRequest<'a> {
    pub proc_id: RpcProcIdValue<'a>,
    pub flags: RpcOptionFlags,
    pub params: Vec<RpcParam<'a>>,
}

impl<'a> TokenRpcRequest<'a> {
    pub async fn write_to<T>(&self, ctx: &protocol::Context, target: &mut T) -> Result<()>
    where
        T: AsyncWrite + Unpin,
    {
        // build the general header for the packet
        let header = PacketHeader {
            ty: PacketType::RPC,
            status: PacketStatus::NormalMessage,
            ..ctx.new_header(0)
        };

        let mut writer = PacketWriter::new(target, header);
        protocol::write_trans_descriptor(&mut writer, ctx, 0).await?;
        match self.proc_id {
            RpcProcIdValue::Id(ref id) => {
                let val = (0xffff as u32) | ((*id as u16) as u32) << 16;
                writer.write_bytes(&ctx, &val.to_le_bytes()).await?;
            }
            RpcProcIdValue::Name(ref _name) => {
                //let (left_bytes, _) = try!(write_varchar::<u16>(&mut cursor, name, 0));
                //assert_eq!(left_bytes, 0);
                unimplemented!()
            }
        }
        let bits: u16 = self.flags.bits();
        writer.write_bytes(&ctx, &bits.to_le_bytes()).await?;

        for (_, param) in self.params.iter().enumerate() {
            writer.write_bytes(&ctx, &[param.name.len() as u8]).await?;
            // name (varchar)
            for codepoint in param.name.encode_utf16() {
                writer.write_bytes(&ctx, &codepoint.to_le_bytes()).await?;
            }

            // status flag
            writer.write_bytes(&ctx, &[param.flags.bits]).await?;
            // recalculate the position for the value (offset)
            param.value.write_to(&ctx, &mut writer).await?;
        }

        // we're officially done with this token stream, flush a last time
        writer.finish(&ctx).await
    }
}

use super::{AllHeaderTy, Encode, ALL_HEADERS_LEN_TX};
use crate::{tds::codec::ColumnData, Result};
use bitflags::bitflags;
use bytes::{BufMut, BytesMut};
use std::borrow::Cow;

#[derive(Debug)]
pub struct TokenRpcRequest<'a> {
    pub proc_id: RpcProcIdValue<'a>,
    pub flags: RpcOptionFlags,
    pub params: Vec<RpcParam<'a>>,
}

#[derive(Debug)]
pub struct RpcParam<'a> {
    pub name: Cow<'a, str>,
    pub flags: RpcStatusFlags,
    pub value: ColumnData<'a>,
}

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

impl<'a> Encode<BytesMut> for TokenRpcRequest<'a> {
    fn encode(self, dst: &mut BytesMut) -> Result<()> {
        dst.put_u32_le(ALL_HEADERS_LEN_TX as u32);
        dst.put_u32_le(ALL_HEADERS_LEN_TX as u32 - 4);
        dst.put_u16_le(AllHeaderTy::TransactionDescriptor as u16);
        dst.put_u64_le(0u64);
        dst.put_u32_le(1);

        match self.proc_id {
            RpcProcIdValue::Id(ref id) => {
                let val = (0xffff as u32) | ((*id as u16) as u32) << 16;
                dst.put_u32_le(val);
            }
            RpcProcIdValue::Name(ref _name) => {
                //let (left_bytes, _) = try!(write_varchar::<u16>(&mut cursor, name, 0));
                //assert_eq!(left_bytes, 0);
                todo!()
            }
        }

        dst.put_u16_le(self.flags.bits() as u16);

        for param in self.params.into_iter() {
            param.encode(dst)?;
        }

        Ok(())
    }
}

impl<'a> Encode<BytesMut> for RpcParam<'a> {
    fn encode(self, dst: &mut BytesMut) -> Result<()> {
        let len = self.name.chars().fold(0, |acc, c| acc + c.len_utf8());
        dst.put_u8(len as u8);

        for codepoint in self.name.encode_utf16() {
            dst.put_u16_le(codepoint);
        }

        dst.put_u8(self.flags.bits());
        self.value.encode(dst)?;

        Ok(())
    }
}

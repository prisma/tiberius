mod bytes_data;
mod column_data;
mod decode;
mod encode;
mod header;
mod login;
mod packet;
mod pre_login;
mod rpc_request;
mod token_col_metadata;
mod token_done;
mod token_env_change;
mod token_error;
mod token_info;
mod token_login_ack;
mod token_order;
mod token_return_value;
mod token_row;
mod token_type;
mod type_info;

#[cfg(windows)]
mod token_sspi;

pub use bytes_data::*;
pub use column_data::*;
pub use decode::*;
pub use encode::*;
pub use header::*;
pub use login::*;
pub use packet::*;
pub use pre_login::*;
pub use rpc_request::*;
pub use token_col_metadata::*;
pub use token_done::*;
pub use token_env_change::*;
pub use token_error::*;
pub use token_info::*;
pub use token_login_ack::*;
pub use token_order::*;
pub use token_return_value::*;
pub use token_row::*;
pub use token_type::*;
pub use type_info::*;

#[cfg(windows)]
pub use token_sspi::*;

use bytes::{Buf, BytesMut};
use futures::{Stream, TryStreamExt};

const HEADER_BYTES: usize = 8;
const ALL_HEADERS_LEN_TX: usize = 22;

#[derive(Debug)]
#[repr(u16)]
#[allow(dead_code)]
enum AllHeaderTy {
    QueryDescriptor = 1,
    TransactionDescriptor = 2,
    TraceActivity = 3,
}

pub struct PacketCodec;

pub(crate) async fn collect_from<'a, S, T>(stream: &'a mut S) -> crate::Result<T>
where
    T: Decode<BytesMut> + Sized,
    S: Stream<Item = crate::Result<Packet>> + Unpin,
{
    let mut buf = BytesMut::new();

    while let Some(packet) = stream.try_next().await? {
        let is_last = packet.is_last();
        let (_, payload) = packet.into_parts();
        buf.extend(payload);

        if is_last {
            break;
        }
    }

    Ok(T::decode(&mut buf)?)
}

pub(crate) fn read_varchar<B: Buf>(src: &mut B, len: impl Into<usize>) -> crate::Result<String> {
    let len = len.into();
    let mut buf = vec![0u16; len];

    for i in 0..len {
        buf[i] = src.get_u16_le();
    }

    Ok(String::from_utf16(&buf[..])?)
}

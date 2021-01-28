mod batch_request;
mod column_data;
mod decode;
mod encode;
mod guid;
mod header;
mod login;
mod packet;
mod pre_login;
mod rpc_request;
mod token;
mod type_info;

pub use batch_request::*;
use bytes::BytesMut;
pub use column_data::*;
pub use decode::*;
pub(crate) use encode::*;
use futures::{Stream, TryStreamExt};
pub use header::*;
pub use login::*;
pub use packet::*;
pub use pre_login::*;
pub use rpc_request::*;
pub use token::*;
pub use type_info::*;

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

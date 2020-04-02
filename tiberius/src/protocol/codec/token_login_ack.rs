use super::Decode;
use crate::{Error, FeatureLevel};
use bytes::{Buf, BytesMut};
use std::convert::TryFrom;

#[derive(Debug)]
pub struct TokenLoginAck {
    /// The type of interface with which the server will accept client requests
    /// 0: SQL_DFLT (server confirms that whatever is sent by the client is acceptable. If the client
    ///    requested SQL_DFLT, SQL_TSQL will be used)
    /// 1: SQL_TSQL (TSQL is accepted)
    pub(crate) interface: u8,
    pub(crate) tds_version: FeatureLevel,
    pub(crate) prog_name: String,
    /// major.minor.buildhigh.buildlow
    pub(crate) version: u32,
}

impl Decode<BytesMut> for TokenLoginAck {
    fn decode(src: &mut BytesMut) -> crate::Result<Self>
    where
        Self: Sized,
    {
        let _length = src.get_u16_le();

        let interface = src.get_u8();

        let tds_version = FeatureLevel::try_from(src.get_u32())
            .map_err(|_| Error::Protocol("Login ACK: Invalid TDS version".into()))?;

        let prog_name = {
            let len = src.get_u8();
            super::read_varchar(src, len)?
        };

        let version = src.get_u32_le();

        Ok(TokenLoginAck {
            interface,
            tds_version,
            prog_name,
            version,
        })
    }
}

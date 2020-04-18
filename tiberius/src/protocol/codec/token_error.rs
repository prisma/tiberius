use super::{BytesData, Decode, FeatureLevel};
use crate::protocol::Context;
use bytes::Buf;
use std::fmt;

#[derive(Clone, Debug, thiserror::Error)]
pub struct TokenError {
    /// ErrorCode
    pub(crate) code: u32,
    /// ErrorState (describing code)
    pub(crate) state: u8,
    /// The class (severity) of the error
    pub(crate) class: u8,
    /// The error message
    pub(crate) message: String,
    pub(crate) server: String,
    pub(crate) procedure: String,
    pub(crate) line: u32,
}

impl fmt::Display for TokenError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "'{}' on server {} executing {} on line {} (code: {}, state: {}, class: {})",
            self.message, self.server, self.procedure, self.line, self.code, self.state, self.class
        )
    }
}

impl<'a> Decode<BytesData<'a, Context>> for TokenError {
    fn decode(src: &mut BytesData<'a, Context>) -> crate::Result<Self>
    where
        Self: Sized,
    {
        let _length = src.get_u16_le() as usize;
        let code = src.get_u32_le();
        let state = src.get_u8();
        let class = src.get_u8();

        let message_len = src.get_u16_le();
        let message = super::read_varchar(src, message_len)?;

        let server_len = src.get_u8();
        let server = super::read_varchar(src, server_len)?;

        let procedure_len = src.get_u8();
        let procedure = super::read_varchar(src, procedure_len)?;

        let line = if src.context().version > FeatureLevel::SqlServer2005 {
            src.get_u32_le()
        } else {
            src.get_u16_le() as u32
        };

        let token = TokenError {
            code,
            state,
            class,
            message,
            server,
            procedure,
            line,
        };

        Ok(token)
    }
}

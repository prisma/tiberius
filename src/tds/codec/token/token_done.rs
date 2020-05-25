use crate::{Error, SqlReadBytes};
use bitflags::bitflags;
use std::fmt;

#[derive(Debug)]
pub struct TokenDone {
    pub status: DoneStatus,
    pub cur_cmd: u16,
    pub done_rows: u64,
}

bitflags! {
    pub struct DoneStatus: u16 {
        const FINAL = 0x0;
        const MORE = 0x1;
        const ERROR = 0x2;
        const INEXACT = 0x4;
        const COUNT = 0x10;
        const ATTENTION = 0x20;
        const RPC_IN_BATCH  = 0x80;
        const SRVERROR = 0x100;
    }
}

impl TokenDone {
    pub(crate) async fn decode<R>(src: &mut R) -> crate::Result<Self>
    where
        R: SqlReadBytes + Unpin,
    {
        let status = DoneStatus::from_bits(src.read_u16_le().await?)
            .ok_or(Error::Protocol("done(variant): invalid status".into()))?;

        let cur_cmd = src.read_u16_le().await?;
        let done_row_count_bytes = src.context().version.done_row_count_bytes();

        let done_rows = match done_row_count_bytes {
            8 => src.read_u64_le().await?,
            4 => src.read_u32_le().await? as u64,
            _ => unreachable!(),
        };

        Ok(TokenDone {
            status,
            cur_cmd,
            done_rows,
        })
    }
}

impl fmt::Display for TokenDone {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.done_rows == 0 {
            write!(f, "Done with status {:?}", self.status)
        } else if self.done_rows == 1 {
            write!(f, "Done with status {:?} (1 row left)", self.status)
        } else {
            write!(
                f,
                "Done with status {:?} ({} rows left)",
                self.status, self.done_rows
            )
        }
    }
}

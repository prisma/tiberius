use crate::{Error, SqlReadBytes};
use enumflags2::BitFlags;
use std::fmt;

#[derive(Debug)]
pub struct TokenDone {
    status: BitFlags<DoneStatus>,
    cur_cmd: u16,
    done_rows: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, BitFlags)]
#[repr(u16)]
pub enum DoneStatus {
    More = 0x1,
    Error = 0x2,
    Inexact = 0x4,
    Count = 0x10,
    Attention = 0x20,
    RpcInBatch = 0x80,
    SrvError = 0x100,
}

impl TokenDone {
    pub(crate) async fn decode<R>(src: &mut R) -> crate::Result<Self>
    where
        R: SqlReadBytes + Unpin,
    {
        let status = BitFlags::from_bits(src.read_u16_le().await?)
            .map_err(|_| Error::Protocol("done(variant): invalid status".into()))?;

        let cur_cmd = src.read_u16_le().await?;
        let done_row_count_bytes = src.context().version().done_row_count_bytes();

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

    pub(crate) fn has_more(&self) -> bool {
        self.status.contains(DoneStatus::More)
    }

    pub(crate) fn is_final(&self) -> bool {
        self.status.is_empty()
    }

    pub(crate) fn rows(&self) -> u64 {
        self.done_rows
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

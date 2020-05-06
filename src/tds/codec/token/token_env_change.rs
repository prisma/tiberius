use crate::{
    tds::{codec::read_varchar, lcid_to_encoding, sortid_to_encoding},
    Error, SqlReadBytes,
};
use encoding::Encoding;
use fmt::Debug;
use std::{convert::TryFrom, fmt};
use tokio::io::AsyncReadExt;

uint_enum! {
    #[repr(u8)]
    pub enum EnvChangeTy {
        Database = 1,
        Language = 2,
        CharacterSet = 3,
        PacketSize = 4,
        UnicodeDataSortingLID = 5,
        UnicodeDataSortingCFL = 6,
        SqlCollation = 7,
        /// below here: >= TDSv7.2
        BeginTransaction = 8,
        CommitTransaction = 9,
        RollbackTransaction = 10,
        EnlistDTCTransaction = 11,
        DefectTransaction = 12,
        RTLS = 13,
        PromoteTransaction = 15,
        TransactionManagerAddress = 16,
        TransactionEnded = 17,
        ResetConnection = 18,
        UserName = 19,
        /// below here: TDS v7.4
        Routing = 20,
    }
}

pub struct CollationInfo {
    lcid_encoding: Option<&'static dyn Encoding>,
    sortid_encoding: Option<&'static dyn Encoding>,
}

impl CollationInfo {
    pub fn new(bytes: &[u8]) -> Self {
        let lcid_encoding = match (bytes.get(0), bytes.get(1)) {
            (Some(fst), Some(snd)) => lcid_to_encoding(u16::from_le_bytes([*fst, *snd])),
            _ => None,
        };

        let sortid_encoding = match bytes.get(4) {
            Some(byte) => sortid_to_encoding(*byte),
            _ => None,
        };

        Self {
            lcid_encoding,
            sortid_encoding,
        }
    }
}

impl Debug for CollationInfo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}

impl fmt::Display for CollationInfo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match (self.lcid_encoding, self.sortid_encoding) {
            (Some(lcid), Some(sortid)) => write!(f, "{}/{}", lcid.name(), sortid.name()),
            _ => write!(f, "None"),
        }
    }
}

#[derive(Debug)]
pub enum TokenEnvChange {
    Database(String, String),
    PacketSize(u32, u32),
    SqlCollation {
        old: CollationInfo,
        new: CollationInfo,
    },
}

impl fmt::Display for TokenEnvChange {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Database(ref old, ref new) => {
                write!(f, "Database change from '{}' to '{}'", old, new)
            }
            Self::PacketSize(old, new) => {
                write!(f, "Packet size change from '{}' to '{}'", old, new)
            }
            Self::SqlCollation { old, new } => {
                write!(f, "SQL collation change from {} to {}", old, new)
            }
        }
    }
}

impl TokenEnvChange {
    pub(crate) async fn decode<R>(src: &mut R) -> crate::Result<Self>
    where
        R: SqlReadBytes + Unpin,
    {
        let _len = src.read_u16_le().await?;
        let ty_byte = src.read_u8().await?;

        let ty = EnvChangeTy::try_from(ty_byte)
            .map_err(|_| Error::Protocol(format!("invalid envchange type {:x}", ty_byte).into()))?;

        let token = match ty {
            EnvChangeTy::Database => {
                let len = src.read_u8().await?;
                let new_value = read_varchar(src, len).await?;

                let len = src.read_u8().await?;
                let old_value = read_varchar(src, len).await?;

                TokenEnvChange::Database(new_value, old_value)
            }
            EnvChangeTy::PacketSize => {
                let len = src.read_u8().await?;
                let new_value = read_varchar(src, len).await?;

                let len = src.read_u8().await?;
                let old_value = read_varchar(src, len).await?;

                TokenEnvChange::PacketSize(new_value.parse()?, old_value.parse()?)
            }
            EnvChangeTy::SqlCollation => {
                let len = src.read_u8().await? as usize;
                let mut new_value = vec![0; len];
                src.read_exact(&mut new_value[0..len]).await?;

                let len = src.read_u8().await? as usize;
                let mut old_value = vec![0; len];
                src.read_exact(&mut old_value[0..len]).await?;

                TokenEnvChange::SqlCollation {
                    new: CollationInfo::new(new_value.as_slice()),
                    old: CollationInfo::new(old_value.as_slice()),
                }
            }
            ty => panic!("skipping env change type {:?}", ty),
        };

        Ok(token)
    }
}

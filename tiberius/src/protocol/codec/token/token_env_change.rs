use crate::{
    protocol::codec::{read_varchar, Decode},
    uint_enum, Error,
};
use bytes::{Buf, BytesMut};
use std::{convert::TryFrom, fmt};

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

#[derive(Debug)]
pub enum TokenEnvChange {
    Database(String, String),
    PacketSize(u32, u32),
    SqlCollation(Vec<u8>, Vec<u8>),
}

impl fmt::Display for TokenEnvChange {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Database(ref old, ref new) => {
                write!(f, "Database change from '{}' to '{}'", new, old)
            }
            Self::PacketSize(old, new) => {
                write!(f, "Packet size change from '{}' to '{}'", new, old)
            }
            Self::SqlCollation(ref old, ref new) => {
                write!(f, "SQL collation change from '{:?}' to '{:?}'", new, old)
            }
        }
    }
}

impl Decode<BytesMut> for TokenEnvChange {
    fn decode(src: &mut BytesMut) -> crate::Result<Self>
    where
        Self: Sized,
    {
        let _len = src.get_u16_le();
        let ty_byte = src.get_u8();

        let ty = EnvChangeTy::try_from(ty_byte)
            .map_err(|_| Error::Protocol(format!("invalid envchange type {:x}", ty_byte).into()))?;

        let token = match ty {
            EnvChangeTy::Database => {
                let len = src.get_u8();
                let new_value = read_varchar(src, len)?;

                let len = src.get_u8();
                let old_value = read_varchar(src, len)?;

                TokenEnvChange::Database(new_value, old_value)
            }
            EnvChangeTy::PacketSize => {
                let len = src.get_u8();
                let new_value = read_varchar(src, len)?;

                let len = src.get_u8();
                let old_value = read_varchar(src, len)?;

                TokenEnvChange::PacketSize(new_value.parse()?, old_value.parse()?)
            }
            EnvChangeTy::SqlCollation => {
                let len = src.get_u8() as usize;
                let new_value = src.split_to(len).to_vec();

                let len = src.get_u8() as usize;
                let old_value = src.split_to(len).to_vec();

                TokenEnvChange::SqlCollation(new_value, old_value)
            }
            ty => panic!("skipping env change type {:?}", ty),
        };

        Ok(token)
    }
}

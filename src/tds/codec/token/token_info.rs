use crate::{tds::codec::read_varchar, SqlReadBytes};

#[derive(Debug)]
pub struct TokenInfo {
    /// info number
    pub(crate) number: u32,
    /// error state
    pub(crate) state: u8,
    /// severity (<10: Info)
    pub(crate) class: u8,
    pub(crate) message: String,
    pub(crate) server: String,
    pub(crate) procedure: String,
    pub(crate) line: u32,
}

impl TokenInfo {
    pub(crate) async fn decode<R>(src: &mut R) -> crate::Result<Self>
    where
        R: SqlReadBytes + Unpin,
    {
        let _length = src.read_u16_le().await?;

        let token = TokenInfo {
            number: src.read_u32_le().await?,
            state: src.read_u8().await?,
            class: src.read_u8().await?,
            message: {
                let len = src.read_u16_le().await?;
                read_varchar(src, len).await?
            },
            server: {
                let len = src.read_u8().await?;
                read_varchar(src, len).await?
            },
            procedure: {
                let len = src.read_u8().await?;
                read_varchar(src, len).await?
            },
            line: src.read_u32_le().await?,
        };

        Ok(token)
    }
}

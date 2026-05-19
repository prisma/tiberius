use crate::{
    SqlReadBytes, FEA_EXT_AZURESQLSUPPORT, FEA_EXT_COLUMNENCRYPTION, FEA_EXT_FEDAUTH,
    FEA_EXT_TERMINATOR, FEA_EXT_UTF8_SUPPORT,
};
use futures_util::AsyncReadExt;

#[derive(Debug)]
pub struct TokenFeatureExtAck {
    pub features: Vec<FeatureAck>,
}

#[derive(Debug)]
#[allow(dead_code)]
pub enum FedAuthAck {
    SecurityToken { nonce: Option<[u8; 32]> },
}

#[derive(Debug)]
#[allow(dead_code)]
pub enum FeatureAck {
    FedAuth(FedAuthAck),
    /// Azure SQL Support acknowledgment from the server.
    AzureSqlSupport(Vec<u8>),
    /// Column Encryption acknowledgment.
    ColumnEncryption(Vec<u8>),
    /// UTF-8 Support acknowledgment.
    Utf8Support(Vec<u8>),
    /// Unknown feature — stored for forward-compatibility.
    Unknown {
        feature_id: u8,
        data: Vec<u8>,
    },
}

impl TokenFeatureExtAck {
    pub(crate) async fn decode<R>(src: &mut R) -> crate::Result<Self>
    where
        R: SqlReadBytes + Unpin,
    {
        let mut features = Vec::new();
        loop {
            let feature_id = src.read_u8().await?;

            if feature_id == FEA_EXT_TERMINATOR {
                break;
            } else if feature_id == FEA_EXT_FEDAUTH {
                let data_len = src.read_u32_le().await?;

                let nonce = if data_len == 32 {
                    let mut n = [0u8; 32];
                    src.read_exact(&mut n).await?;

                    Some(n)
                } else if data_len == 0 {
                    None
                } else {
                    panic!("invalid Feature_Ext_Ack token");
                };

                features.push(FeatureAck::FedAuth(FedAuthAck::SecurityToken { nonce }))
            } else if feature_id == FEA_EXT_AZURESQLSUPPORT {
                let data_len = src.read_u32_le().await? as usize;
                let mut data = vec![0u8; data_len];
                if data_len > 0 {
                    src.read_exact(&mut data).await?;
                }
                features.push(FeatureAck::AzureSqlSupport(data));
            } else if feature_id == FEA_EXT_COLUMNENCRYPTION {
                let data_len = src.read_u32_le().await? as usize;
                let mut data = vec![0u8; data_len];
                if data_len > 0 {
                    src.read_exact(&mut data).await?;
                }
                features.push(FeatureAck::ColumnEncryption(data));
            } else if feature_id == FEA_EXT_UTF8_SUPPORT {
                let data_len = src.read_u32_le().await? as usize;
                let mut data = vec![0u8; data_len];
                if data_len > 0 {
                    src.read_exact(&mut data).await?;
                }
                features.push(FeatureAck::Utf8Support(data));
            } else {
                // Unknown feature — skip gracefully by reading data_len bytes
                let data_len = src.read_u32_le().await? as usize;
                let mut data = vec![0u8; data_len];
                if data_len > 0 {
                    src.read_exact(&mut data).await?;
                }
                features.push(FeatureAck::Unknown { feature_id, data });
            }
        }

        Ok(TokenFeatureExtAck { features })
    }
}

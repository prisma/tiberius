use super::BaseMetaDataColumn;
use crate::{
    tds::codec::{read_varchar, ColumnData},
    Error, SqlReadBytes,
};

#[derive(Debug)]
pub struct TokenReturnValue {
    pub param_ordinal: u16,
    pub param_name: String,
    /// return value of user defined function
    pub udf: bool,
    pub meta: BaseMetaDataColumn,
    pub value: ColumnData<'static>,
}

impl TokenReturnValue {
    pub(crate) async fn decode<R>(src: &mut R) -> crate::Result<Self>
    where
        R: SqlReadBytes + Unpin,
    {
        let param_ordinal = src.read_u16_le().await?;
        let param_name_len = src.read_u8().await? as usize;

        let param_name = read_varchar(src, param_name_len).await?;

        let udf = match src.read_u8().await? {
            0x01 => false,
            0x02 => true,
            _ => return Err(Error::Protocol("ReturnValue: invalid status".into())),
        };

        let meta = BaseMetaDataColumn::decode(src).await?;
        let value = ColumnData::decode(src, &meta.ty).await?;

        let token = TokenReturnValue {
            param_ordinal,
            param_name,
            udf,
            value,
            meta,
        };

        Ok(token)
    }
}

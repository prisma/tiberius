use super::BaseMetaDataColumn;
use crate::{
    protocol::codec::{read_varchar, BytesData, ColumnData, Decode},
    Error,
};
use bytes::{Buf, BytesMut};

#[derive(Debug)]
pub struct TokenReturnValue {
    pub param_ordinal: u16,
    pub param_name: String,
    /// return value of user defined function
    pub udf: bool,
    pub meta: BaseMetaDataColumn,
    pub value: ColumnData<'static>,
}

impl Decode<BytesMut> for TokenReturnValue {
    fn decode(src: &mut BytesMut) -> crate::Result<Self>
    where
        Self: Sized,
    {
        let param_ordinal = src.get_u16_le();
        let param_name_len = src.get_u8() as usize;

        let param_name = read_varchar(src, param_name_len)?;

        let udf = match src.get_u8() {
            0x01 => false,
            0x02 => true,
            _ => return Err(Error::Protocol("ReturnValue: invalid status".into())),
        };

        let meta = BaseMetaDataColumn::decode(src)?;
        let mut src = BytesData::new(src, &meta);
        let value = ColumnData::decode(&mut src)?;

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

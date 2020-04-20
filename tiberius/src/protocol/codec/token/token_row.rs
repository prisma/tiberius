use crate::{
    async_read_le_ext::AsyncReadLeExt,
    protocol::{
        codec::{ColumnData, TokenColMetaData},
        Context,
    },
};
use std::sync::Arc;

#[derive(Debug)]
pub struct TokenRow {
    pub meta: Arc<TokenColMetaData>,
    pub columns: Vec<ColumnData<'static>>,
}

impl TokenRow {
    pub(crate) async fn decode<R>(src: &mut R, ctx: &Context) -> crate::Result<Self>
    where
        R: AsyncReadLeExt + Unpin,
    {
        let col_meta = ctx.last_meta.lock().clone().unwrap();

        let mut row = TokenRow {
            meta: col_meta.clone(),
            columns: Vec::with_capacity(col_meta.columns.len()),
        };

        for column in col_meta.columns.iter() {
            let data = ColumnData::decode(src, &column.base.ty).await?;

            row.columns.push(data);
        }

        Ok(row)
    }
}

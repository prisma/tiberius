use crate::{
    async_read_le_ext::AsyncReadLeExt,
    protocol::{
        codec::{ColumnData, TokenColMetaData},
        Context,
    },
};
use std::sync::Arc;
use tokio::io::AsyncReadExt;

#[derive(Debug)]
pub struct TokenRow {
    pub meta: Arc<TokenColMetaData>,
    pub columns: Vec<ColumnData<'static>>,
}

struct RowBitmap {
    data: Vec<u8>,
}

impl RowBitmap {
    fn is_null(&self, i: usize) -> bool {
        let index = i / 8;
        let bit = i % 8;

        self.data[index] & (1 << bit) > 0
    }

    async fn decode<R>(src: &mut R, columns: usize) -> crate::Result<Self>
    where
        R: AsyncReadLeExt + Unpin,
    {
        let size = (columns + 8 - 1) / 8;
        let mut data = vec![0; size];
        src.read_exact(&mut data[0..size]).await?;

        Ok(Self { data })
    }
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

    pub(crate) async fn decode_nbc<R>(src: &mut R, ctx: &Context) -> crate::Result<Self>
    where
        R: AsyncReadLeExt + Unpin,
    {
        let col_meta = ctx.last_meta.lock().clone().unwrap();
        let row_bitmap = RowBitmap::decode(src, col_meta.columns.len()).await?;

        let mut row = TokenRow {
            meta: col_meta.clone(),
            columns: Vec::with_capacity(col_meta.columns.len()),
        };

        for (i, column) in col_meta.columns.iter().enumerate() {
            let data = if row_bitmap.is_null(i) {
                ColumnData::None
            } else {
                ColumnData::decode(src, &column.base.ty).await?
            };

            row.columns.push(data);
        }

        Ok(row)
    }
}

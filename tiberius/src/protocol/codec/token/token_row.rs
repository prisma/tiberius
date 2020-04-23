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

/// A bitmap of null values in the row. Sometimes SQL Server decides to pack the
/// null values in the row, calling it the NBCROW. In this kind of tokens the row
/// itself skips the null columns completely, but they can be found from the bitmap
/// stored in the beginning of the token.
///
/// One byte can store eight bits of information. Bits with value of one being null.
///
/// If our row has eight columns, and our byte in bits is:
///
/// ```ignore
/// 1 0 0 1 0 1 0 0
/// ```
///
/// This would mean columns 0, 3 and 5 are null and should not be parsed at all.
/// For more than eight columns, more bits need to be reserved for the bitmap
/// (see the size calculation).
struct RowBitmap {
    data: Vec<u8>,
}

impl RowBitmap {
    /// Is the given column index null or not.
    #[inline]
    fn is_null(&self, i: usize) -> bool {
        let index = i / 8;
        let bit = i % 8;

        self.data[index] & (1 << bit) > 0
    }

    /// Decode the bitmap data from the beginning of the row. Only doable if the
    /// type is `NbcRowToken`.
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
    /// Normal row. We'll read the metadata what we've cached and parse columns
    /// based on that.
    pub(crate) async fn decode<R>(src: &mut R, ctx: &Context) -> crate::Result<Self>
    where
        R: AsyncReadLeExt + Unpin,
    {
        let col_meta = ctx.last_meta.lock().await.clone().unwrap();

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

    /// SQL Server has packed nulls on this row type. We'll read what columns
    /// are null from the bitmap.
    pub(crate) async fn decode_nbc<R>(src: &mut R, ctx: &Context) -> crate::Result<Self>
    where
        R: AsyncReadLeExt + Unpin,
    {
        let col_meta = ctx.last_meta.lock().await.clone().unwrap();
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

use crate::protocol::codec::{ColumnData, TokenColMetaData};
use std::sync::Arc;

#[derive(Debug)]
pub struct TokenRow {
    pub meta: Arc<TokenColMetaData>,
    pub columns: Vec<ColumnData<'static>>,
}

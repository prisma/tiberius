use parking_lot::Mutex;
use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicI32, Ordering},
        Arc,
    },
};

pub struct Statement {
    pub(crate) query: String,
    // Map from statement SQL type signature to prepared statement ID/handle
    pub(crate) handles: Mutex<HashMap<String, Arc<AtomicI32>>>,
    close_handle_queue: Arc<Mutex<Vec<i32>>>,
}

impl Drop for Statement {
    fn drop(&mut self) {
        // Mark all handles for cleanup (unprepare)
        let handles = self.handles.get_mut();

        let cleanable_handles = handles
            .values()
            .map(|x| x.load(Ordering::SeqCst))
            .filter(|x| *x > 0);

        let mut close_queue = self.close_handle_queue.lock();
        close_queue.extend(cleanable_handles);
    }
}

pub(crate) mod private {
    use super::Statement;

    pub enum StatementRepr<'a> {
        Statement(&'a Statement),
        QueryString(&'a str),
    }
}

pub trait ToStatement {
    fn to_stmt(&self) -> private::StatementRepr;
}

impl ToStatement for str {
    fn to_stmt(&self) -> private::StatementRepr {
        private::StatementRepr::QueryString(&self)
    }
}

impl ToStatement for Statement {
    fn to_stmt(&self) -> private::StatementRepr {
        private::StatementRepr::Statement(&self)
    }
}

use std::{
    borrow::{Borrow, BorrowMut},
    ops::{Deref, DerefMut},
};

use asynchronous_codec::BytesMut;
use bytes::BufMut;

pub(crate) struct BufColumnData<'a> {
    buf: &'a mut BytesMut,
    pub(crate) write_headers: bool,
}

impl<'a> BufColumnData<'a> {
    pub(crate) fn with_headers(buf: &'a mut BytesMut) -> Self {
        Self {
            buf,
            write_headers: true,
        }
    }

    #[allow(dead_code)]
    pub(crate) fn without_headers(buf: &'a mut BytesMut) -> Self {
        Self {
            buf,
            write_headers: false,
        }
    }

    pub(crate) fn extend(&mut self, other: Vec<u8>) {
        self.buf.extend(other)
    }

    pub(crate) fn extend_from_slice(&mut self, other: &[u8]) {
        self.buf.extend_from_slice(other);
    }

    pub(crate) fn len(&mut self) -> usize {
        self.buf.len()
    }
}

unsafe impl<'a> BufMut for BufColumnData<'a> {
    fn remaining_mut(&self) -> usize {
        self.buf.remaining_mut()
    }

    unsafe fn advance_mut(&mut self, cnt: usize) {
        self.buf.advance_mut(cnt)
    }

    fn chunk_mut(&mut self) -> &mut bytes::buf::UninitSlice {
        self.buf.chunk_mut()
    }
}

impl<'a> Borrow<[u8]> for BufColumnData<'a> {
    fn borrow(&self) -> &[u8] {
        self.buf.deref()
    }
}

impl<'a> BorrowMut<[u8]> for BufColumnData<'a> {
    fn borrow_mut(&mut self) -> &mut [u8] {
        self.buf.borrow_mut()
    }
}

impl<'a> Deref for BufColumnData<'a> {
    type Target = BytesMut;

    fn deref(&self) -> &Self::Target {
        self.buf
    }
}

impl<'a> DerefMut for BufColumnData<'a> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.buf
    }
}

use bytes::{Buf, BufMut, BytesMut};
use std::{
    mem::MaybeUninit,
    ops::{Deref, DerefMut},
};

pub struct BytesData<'a, C> {
    src: &'a mut BytesMut,
    context: &'a C,
}

impl<'a, C> BytesData<'a, C> {
    pub fn new(src: &'a mut BytesMut, context: &'a C) -> Self {
        Self { src, context }
    }

    pub fn inner(&mut self) -> &mut BytesMut {
        self.src
    }

    pub fn context(&self) -> &'a C {
        self.context
    }
}

impl<'a, C> Buf for BytesData<'a, C> {
    fn remaining(&self) -> usize {
        self.src.remaining()
    }

    fn bytes(&self) -> &[u8] {
        self.src.bytes()
    }

    fn advance(&mut self, cnt: usize) {
        self.src.advance(cnt)
    }
}

impl<'a, C> BufMut for BytesData<'a, C> {
    fn remaining_mut(&self) -> usize {
        self.src.remaining_mut()
    }

    unsafe fn advance_mut(&mut self, cnt: usize) {
        self.src.advance_mut(cnt)
    }

    fn bytes_mut(&mut self) -> &mut [MaybeUninit<u8>] {
        self.src.bytes_mut()
    }
}

impl<'a, C> AsMut<[u8]> for BytesData<'a, C> {
    fn as_mut(&mut self) -> &mut [u8] {
        self.inner().as_mut()
    }
}

impl<'a, C> AsRef<[u8]> for BytesData<'a, C> {
    fn as_ref(&self) -> &[u8] {
        self.src.as_ref()
    }
}

impl<'a, C> Deref for BytesData<'a, C> {
    type Target = [u8];

    #[inline]
    fn deref(&self) -> &[u8] {
        self.as_ref()
    }
}

impl<'a, C> DerefMut for BytesData<'a, C> {
    #[inline]
    fn deref_mut(&mut self) -> &mut [u8] {
        self.src.as_mut()
    }
}

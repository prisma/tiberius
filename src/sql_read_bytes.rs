use crate::tds::Context;
use bytes::Buf;
use futures::io::AsyncRead;
use pin_project_lite::pin_project;
use std::io::ErrorKind::UnexpectedEof;
use std::{future::Future, io, mem::size_of, pin::Pin, task};
use task::Poll;

macro_rules! bytes_reader {
    ($name:ident, $ty:ty, $reader:ident) => {
        bytes_reader!($name, $ty, $reader, size_of::<$ty>());
    };
    ($name:ident, $ty:ty, $reader:ident, $bytes:expr) => {
        pin_project! {
            #[doc(hidden)]
            pub struct $name<R> {
                #[pin]
                src: R,
                buf: [u8; $bytes],
                read: u8,
            }
        }

        #[allow(dead_code)]
        impl<R> $name<R> {
            pub(crate) fn new(src: R) -> Self {
                $name {
                    src,
                    buf: [0; $bytes],
                    read: 0,
                }
            }
        }

        impl<R> Future for $name<R>
        where
            R: AsyncRead,
        {
            type Output = io::Result<$ty>;

            fn poll(self: Pin<&mut Self>, cx: &mut task::Context<'_>) -> Poll<Self::Output> {
                let mut me = self.project();

                if *me.read == $bytes as u8 {
                    return Poll::Ready(Ok(Buf::$reader(&mut &me.buf[..])));
                }

                while *me.read < $bytes as u8 {
                    *me.read += match me
                        .src
                        .as_mut()
                        .poll_read(cx, &mut me.buf[*me.read as usize..])
                    {
                        Poll::Pending => return Poll::Pending,
                        Poll::Ready(Err(e)) => return Poll::Ready(Err(e.into())),
                        Poll::Ready(Ok(0)) => {
                            return Poll::Ready(Err(UnexpectedEof.into()));
                        }
                        Poll::Ready(Ok(n)) => n as u8,
                    };
                }

                let num = Buf::$reader(&mut &me.buf[..]);

                Poll::Ready(Ok(num))
            }
        }
    };
}

pub(crate) trait SqlReadBytes: AsyncRead + Unpin {
    fn debug_buffer(&self);

    fn context(&self) -> &Context;

    fn context_mut(&mut self) -> &mut Context;

    fn read_i8<'a>(&'a mut self) -> ReadI8<&'a mut Self>
    where
        Self: Unpin,
    {
        ReadI8::new(self)
    }

    fn read_u8<'a>(&'a mut self) -> ReadU8<&'a mut Self>
    where
        Self: Unpin,
    {
        ReadU8::new(self)
    }

    fn read_u32<'a>(&'a mut self) -> ReadU32Be<&'a mut Self>
    where
        Self: Unpin,
    {
        ReadU32Be::new(self)
    }

    fn read_f32<'a>(&'a mut self) -> ReadF32<&'a mut Self>
    where
        Self: Unpin,
    {
        ReadF32::new(self)
    }

    fn read_f64<'a>(&'a mut self) -> ReadF64<&'a mut Self>
    where
        Self: Unpin,
    {
        ReadF64::new(self)
    }

    fn read_f32_le<'a>(&'a mut self) -> ReadF32Le<&'a mut Self>
    where
        Self: Unpin,
    {
        ReadF32Le::new(self)
    }

    fn read_f64_le<'a>(&'a mut self) -> ReadF64Le<&'a mut Self>
    where
        Self: Unpin,
    {
        ReadF64Le::new(self)
    }

    fn read_u16_le<'a>(&'a mut self) -> ReadU16Le<&'a mut Self>
    where
        Self: Unpin,
    {
        ReadU16Le::new(self)
    }

    fn read_u16<'a>(&'a mut self) -> ReadU16<&'a mut Self>
    where
        Self: Unpin,
    {
        ReadU16::new(self)
    }

    fn read_u32_le<'a>(&'a mut self) -> ReadU32Le<&'a mut Self>
    where
        Self: Unpin,
    {
        ReadU32Le::new(self)
    }

    fn read_u64_le<'a>(&'a mut self) -> ReadU64Le<&'a mut Self>
    where
        Self: Unpin,
    {
        ReadU64Le::new(self)
    }

    fn read_u128_le<'a>(&'a mut self) -> ReadU128Le<&'a mut Self>
    where
        Self: Unpin,
    {
        ReadU128Le::new(self)
    }

    fn read_i16_le<'a>(&'a mut self) -> ReadI16Le<&'a mut Self>
    where
        Self: Unpin,
    {
        ReadI16Le::new(self)
    }

    fn read_i32_le<'a>(&'a mut self) -> ReadI32Le<&'a mut Self>
    where
        Self: Unpin,
    {
        ReadI32Le::new(self)
    }

    fn read_i64_le<'a>(&'a mut self) -> ReadI64Le<&'a mut Self>
    where
        Self: Unpin,
    {
        ReadI64Le::new(self)
    }

    fn read_i128_le<'a>(&'a mut self) -> ReadI128Le<&'a mut Self>
    where
        Self: Unpin,
    {
        ReadI128Le::new(self)
    }
}

bytes_reader!(ReadI8, i8, get_i8);
bytes_reader!(ReadU8, u8, get_u8);
bytes_reader!(ReadU32Be, u32, get_u32);

bytes_reader!(ReadU16, u16, get_u16);
bytes_reader!(ReadU16Le, u16, get_u16_le);
bytes_reader!(ReadU32Le, u32, get_u32_le);
bytes_reader!(ReadU64Le, u64, get_u64_le);
bytes_reader!(ReadU128Le, u128, get_u128_le);

bytes_reader!(ReadI16Le, i16, get_i16_le);
bytes_reader!(ReadI32Le, i32, get_i32_le);
bytes_reader!(ReadI64Le, i64, get_i64_le);
bytes_reader!(ReadI128Le, i128, get_i128_le);

bytes_reader!(ReadF32, f32, get_f32);
bytes_reader!(ReadF64, f64, get_f64);

bytes_reader!(ReadF32Le, f32, get_f32_le);
bytes_reader!(ReadF64Le, f64, get_f64_le);

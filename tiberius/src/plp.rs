//! Partially Length-Prefixed types handling

use crate::{protocol::PacketReader, Error};
use byteorder::{LittleEndian, WriteBytesExt};
use std::{
    cmp,
    io::{self, Write},
};
use tokio::io::AsyncRead;

/// Mode for type reader.
#[derive(Debug, Clone, Copy)]
pub enum ReadTyMode {
    /// Fixed-size type with given size
    FixedSize(usize),
    /// Partially length-prefixed type
    Plp,
}

impl ReadTyMode {
    /// Determine the mode automatically from size
    pub fn auto(size: usize) -> Self {
        if size < 0xffff {
            ReadTyMode::FixedSize(size)
        } else {
            ReadTyMode::Plp
        }
    }
}

/// A partially read type
#[derive(Debug)]
pub struct ReadTyState {
    pub(crate) mode: ReadTyMode,
    pub(crate) data: Option<Vec<u8>>,
    pub(crate) chunk_data_left: usize,
}

impl ReadTyState {
    /// Initialize a type reader
    pub fn new(mode: ReadTyMode) -> Self {
        ReadTyState {
            mode,
            data: None,
            chunk_data_left: 0,
        }
    }
}

pub struct PLPChunkWriter<W: Write> {
    pub target: W,
    pub buf: Vec<u8>,
}

impl<W: Write> io::Write for PLPChunkWriter<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        // fast path for small writes
        if self.buf.capacity() - self.buf.len() > buf.len() {
            self.buf.extend_from_slice(buf);
            return Ok(buf.len());
        }

        let mut pending = buf;
        while !pending.is_empty() {
            let free_bytes = self.buf.capacity() - self.buf.len();
            let boundary = cmp::min(pending.len(), free_bytes);
            let (fitting, next) = pending.split_at(boundary);

            // we can produce a whole chunk => write to the underlying buf
            if fitting.len() == free_bytes {
                self.target
                    .write_u32::<LittleEndian>(self.buf.capacity() as u32)?;
                self.target.write_all(&self.buf)?;
                self.target.write_all(fitting)?;
                self.buf.truncate(0);
            } else {
                self.buf.extend_from_slice(fitting);
            }
            pending = next;
        }
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        if !self.buf.is_empty() {
            self.target
                .write_u32::<LittleEndian>(self.buf.len() as u32)?;
            self.target.write_all(&self.buf)?;
            self.buf.truncate(0);
        }
        Ok(())
    }
}

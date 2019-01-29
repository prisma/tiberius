//! Partially Length-Prefixed types handling

use std::cmp;
use std::io::{self, Cursor, Write};

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use futures::{Async, Poll};

use Error;

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
    mode: ReadTyMode,
    data: Option<Vec<u8>>,
    chunk_data_left: usize,
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

    /// Read data stream as Plain or PLP
    ///
    /// Returns bytes read or `None` if the value turned out to be NULL
    pub fn read(&mut self, input: &mut impl ReadBytesExt) -> Poll<Option<Vec<u8>>, Error> {
        // If we did not read anything yet, initialize the reader.
        if self.data.is_none() {
            let size = match self.mode {
                ReadTyMode::FixedSize(_) => input.read_u16::<LittleEndian>()? as u64,
                ReadTyMode::Plp => input.read_u64::<LittleEndian>()?,
            };

            self.data = match (size, self.mode) {
                (0xffff, ReadTyMode::FixedSize(_)) => None, // NULL value
                (0xffffffffffffffff, ReadTyMode::Plp) => None, // NULL value
                (0xfffffffffffffffe, ReadTyMode::Plp) => Some(Vec::new()), // unknown size
                (len, _) => Some(Vec::with_capacity(len as usize)), // given size
            };

            // If this is not PLP, treat everything as a single chunk.
            if let ReadTyMode::FixedSize(_) = self.mode {
                self.chunk_data_left = size as usize;
            }
        }

        // If there is a buffer, we have something to read.
        if let Some(ref mut buf) = self.data {
            loop {
                if self.chunk_data_left == 0 {
                    // We have no chunk. Start a new one.
                    let chunk_size = match self.mode {
                        ReadTyMode::FixedSize(_) => 0,
                        ReadTyMode::Plp => input.read_u32::<LittleEndian>()? as usize,
                    };
                    if chunk_size == 0 {
                        break // found a sentinel, we're done
                    } else {
                        self.chunk_data_left = chunk_size
                    }
                } else {
                    // Just read a byte
                    let byte = input.read_u8()?;
                    self.chunk_data_left -= 1;
                    buf.push(byte);
                }
            }
        }

        // If we're here, we're done reading.
        Ok(Async::Ready(self.data.take()))
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
            self.target.write_u32::<LittleEndian>(self.buf.len() as u32)?;
            self.target.write_all(&self.buf)?;
            self.buf.truncate(0);
        }
        Ok(())
    }
}

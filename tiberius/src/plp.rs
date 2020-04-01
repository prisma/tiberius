//! Partially Length-Prefixed types handling

use crate::{
    protocol::{Context, PacketReader, PacketWriter},
    Error,
};
use byteorder::{LittleEndian, WriteBytesExt};
use std::{
    cmp,
    io::{self, Write},
    marker::Unpin,
    pin::Pin,
    task::Poll,
};
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};

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

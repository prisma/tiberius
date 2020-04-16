pub mod codec;

pub mod stream;
mod types;

use crate::uint_enum;
use codec::*;
use std::{
    sync::atomic::{AtomicU32, AtomicU8, Ordering},
    sync::Arc,
};

uint_enum! {
    /// The configured encryption level specifying if encryption is required
    #[repr(u8)]
    pub enum EncryptionLevel {
        /// Only use encryption for the login procedure
        Off = 0,
        /// Encrypt everything if possible
        On = 1,
        /// Do not encrypt anything
        NotSupported = 2,
        /// Encrypt everything and fail if not possible
        Required = 3,
    }
}

/// Context, that might be required to make sure we understand and are understood by the server
pub struct Context {
    pub version: FeatureLevel,
    pub packet_size: AtomicU32,
    pub packet_id: AtomicU8,
    pub last_meta: parking_lot::Mutex<Option<Arc<TokenColMetaData>>>,
}

impl Context {
    pub fn new() -> Context {
        Context {
            version: FeatureLevel::SqlServerN,
            packet_size: AtomicU32::new(4096),
            packet_id: AtomicU8::new(0),
            last_meta: parking_lot::Mutex::new(None),
        }
    }

    pub fn new_header(&self, length: usize) -> PacketHeader {
        PacketHeader::new(length, self.packet_id.fetch_add(1, Ordering::SeqCst))
    }

    pub fn set_last_meta(&self, meta: Arc<TokenColMetaData>) {
        *self.last_meta.lock() = Some(meta);
    }
}

/// The amount of bytes a packet header consists of
pub const HEADER_BYTES: usize = 8;

pub mod codec;
mod collation;
pub mod numeric;
pub mod stream;
pub mod time;

pub(crate) use collation::*;
pub(crate) use numeric::*;
pub(crate) use time::*;

use codec::*;
use std::{
    sync::atomic::{AtomicU32, AtomicU8, Ordering},
    sync::Arc,
};
use tokio::sync::Mutex;

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
pub(crate) struct Context {
    pub(crate) version: FeatureLevel,
    pub(crate) packet_size: AtomicU32,
    pub(crate) packet_id: AtomicU8,
    pub(crate) last_meta: Mutex<Option<Arc<TokenColMetaData>>>,
    #[cfg(windows)]
    pub(crate) spn: Option<String>,
}

impl Context {
    pub(crate) fn new() -> Context {
        Context {
            version: FeatureLevel::SqlServerN,
            packet_size: AtomicU32::new(4096),
            packet_id: AtomicU8::new(0),
            last_meta: Mutex::new(None),
            #[cfg(windows)]
            spn: None,
        }
    }

    pub(crate) fn new_header(&self, length: usize) -> PacketHeader {
        PacketHeader::new(length, self.packet_id.fetch_add(1, Ordering::SeqCst))
    }

    pub(crate) async fn set_last_meta(&self, meta: Arc<TokenColMetaData>) {
        *self.last_meta.lock().await = Some(meta);
    }

    #[cfg(windows)]
    pub(crate) fn set_spn(&mut self, host: impl AsRef<str>, port: u16) {
        self.spn = Some(format!("MSSQLSvc/{}:{}", host.as_ref(), port));
    }

    #[cfg(windows)]
    pub(crate) fn spn(&self) -> &str {
        self.spn.as_ref().map(|s| s.as_str()).unwrap_or("")
    }
}

/// The amount of bytes a packet header consists of
pub const HEADER_BYTES: usize = 8;

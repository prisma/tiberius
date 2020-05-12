pub mod codec;
mod collation;
pub mod numeric;
pub mod stream;
pub mod time;
pub mod xml;

pub(crate) use collation::*;
pub(crate) use numeric::*;
pub(crate) use time::*;

use codec::*;
use std::{
    sync::atomic::{AtomicU32, AtomicU8, Ordering},
    sync::Arc,
};
use tokio::sync::Mutex;

/// The amount of bytes a packet header consists of
pub(crate) const HEADER_BYTES: usize = 8;

#[cfg(feature = "tls")]
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

#[cfg(not(feature = "tls"))]
uint_enum! {
    pub enum EncryptionLevel {
        /// Do not encrypt anything
        NotSupported = 2,
    }
}

/// Context, that might be required to make sure we understand and are understood by the server
#[derive(Debug)]
pub(crate) struct Context {
    pub version: FeatureLevel,
    pub packet_size: AtomicU32,
    pub packet_id: AtomicU8,
    pub last_meta: Mutex<Option<Arc<TokenColMetaData>>>,
    #[cfg(windows)]
    pub spn: Option<String>,
}

impl Context {
    pub fn new() -> Context {
        Context {
            version: FeatureLevel::SqlServerN,
            packet_size: AtomicU32::new(4096),
            packet_id: AtomicU8::new(0),
            last_meta: Mutex::new(None),
            #[cfg(windows)]
            spn: None,
        }
    }

    pub fn new_header(&self, length: usize) -> PacketHeader {
        PacketHeader::new(length, self.packet_id.fetch_add(1, Ordering::SeqCst))
    }

    pub async fn set_last_meta(&self, meta: Arc<TokenColMetaData>) {
        *self.last_meta.lock().await = Some(meta);
    }

    #[cfg(windows)]
    pub fn set_spn(&mut self, host: impl AsRef<str>, port: u16) {
        self.spn = Some(format!("MSSQLSvc/{}:{}", host.as_ref(), port));
    }

    #[cfg(windows)]
    pub fn spn(&self) -> &str {
        self.spn.as_ref().map(|s| s.as_str()).unwrap_or("")
    }
}

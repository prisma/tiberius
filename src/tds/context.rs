use super::codec::*;
use std::sync::Arc;

/// Context, that might be required to make sure we understand and are understood by the server
#[derive(Debug)]
pub(crate) struct Context {
    version: FeatureLevel,
    packet_size: u32,
    packet_id: u8,
    transaction_id: u64,
    last_meta: Option<Arc<TokenColMetaData>>,
    #[cfg(windows)]
    spn: Option<String>,
}

impl Context {
    pub fn new() -> Context {
        Context {
            version: FeatureLevel::SqlServerN,
            packet_size: 4096,
            packet_id: 0,
            transaction_id: 0,
            last_meta: None,
            #[cfg(windows)]
            spn: None,
        }
    }

    pub fn next_packet_id(&mut self) -> u8 {
        let id = self.packet_id;
        self.packet_id += 1;
        id
    }

    pub fn set_last_meta(&mut self, meta: Arc<TokenColMetaData>) {
        self.last_meta.replace(meta);
    }

    pub fn last_meta(&self) -> Option<Arc<TokenColMetaData>> {
        self.last_meta.as_ref().map(Arc::clone)
    }

    pub fn packet_size(&self) -> u32 {
        self.packet_size
    }

    pub fn set_packet_size(&mut self, new_size: u32) {
        self.packet_size = new_size;
    }

    pub fn transaction_id(&self) -> u64 {
        self.transaction_id
    }

    pub fn set_transaction_id(&mut self, id: u64) {
        self.transaction_id = id;
    }

    pub fn version(&self) -> FeatureLevel {
        self.version
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

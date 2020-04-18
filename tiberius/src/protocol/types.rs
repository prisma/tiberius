mod numeric;

pub use numeric::Numeric;

use crate::collation;
use encoding::Encoding;

#[derive(Debug, Clone, Copy)]
pub struct Collation {
    /// LCID ColFlags Version
    info: u32,
    /// Sortid
    sort_id: u8,
}

impl Collation {
    pub fn new(info: u32, sort_id: u8) -> Self {
        Self { info, sort_id }
    }

    /// return the locale id part of the LCID (the specification here uses ambiguous terms)
    pub fn lcid(&self) -> u16 {
        (self.info & 0xffff) as u16
    }

    /// return an encoding for a given collation
    pub fn encoding(&self) -> Option<&'static dyn Encoding> {
        if self.sort_id == 0 {
            collation::lcid_to_encoding(self.lcid())
        } else {
            collation::sortid_to_encoding(self.sort_id)
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Guid(pub(crate) [u8; 16]);

impl Guid {
    pub fn from_bytes(input_bytes: &[u8]) -> Guid {
        assert_eq!(input_bytes.len(), 16);
        let mut bytes = [0u8; 16];
        bytes.clone_from_slice(input_bytes);
        Guid(bytes)
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

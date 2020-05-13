use super::{Decode, Encode};
use crate::{tds, Error, Result};
use byteorder::{BigEndian, ReadBytesExt};
use bytes::{BufMut, BytesMut};
use std::convert::TryFrom;
use std::io::{self, Cursor};
use tds::EncryptionLevel;

/// The prelogin packet used to initialize a connection
#[derive(Debug)]
pub struct PreloginMessage {
    /// [BE] token=0x00
    /// Either the driver version or the version of the SQL server
    pub version: u32,
    pub sub_build: u16,
    /// token=0x01
    pub encryption: EncryptionLevel,
    /// [client] threadid for debugging purposes, token=0x03
    pub thread_id: u32,
    /// token=0x04
    pub mars: bool,
}

impl PreloginMessage {
    pub fn new() -> PreloginMessage {
        let driver_version = crate::get_driver_version();
        PreloginMessage {
            version: driver_version as u32,
            sub_build: (driver_version >> 32) as u16,
            encryption: EncryptionLevel::NotSupported,
            thread_id: 0,
            mars: false,
        }
    }

    pub fn negotiated_encryption(&self, expected: EncryptionLevel) -> EncryptionLevel {
        match (expected, self.encryption) {
            (EncryptionLevel::NotSupported, EncryptionLevel::NotSupported) => {
                EncryptionLevel::NotSupported
            }
            #[cfg(feature = "tls")]
            (EncryptionLevel::Off, EncryptionLevel::Off) => EncryptionLevel::Off,
            #[cfg(feature = "tls")]
            (EncryptionLevel::On, EncryptionLevel::Off)
            | (EncryptionLevel::On, EncryptionLevel::NotSupported) => {
                panic!("Server does not allow the requested encryption level.")
            }
            #[cfg(feature = "tls")]
            (_, _) => EncryptionLevel::On,
        }
    }
}

impl Encode<BytesMut> for PreloginMessage {
    fn encode(self, dst: &mut BytesMut) -> Result<()> {
        // build the packet-body
        // offset = PL_OPTION_TOKEN + PL_OFFSET + PL_OPTION_LENGTH = 5 bytes + the terminator (0xFF)
        let mut data_offset = 4 * 5 + 1;

        // write the offsets
        {
            let mut write_option = |token: u8, length: u16| -> io::Result<()> {
                dst.put_u8(token);
                dst.put_u16(data_offset);
                dst.put_u16(length);

                data_offset += length;

                Ok(())
            };

            write_option(0x00, 0x04 + 0x02)?; // version + subbuild
            write_option(0x01, 0x01)?; // encryption
            write_option(0x03, 0x04)?; // threadid
            write_option(0x04, 0x01)?; // MARS
        }

        dst.put_u8(255);

        // write the data (body of the options)
        dst.put_u32(self.version as u32);
        dst.put_u16(self.sub_build as u16);
        dst.put_u8(self.encryption as u8);
        dst.put_u32(self.thread_id);
        dst.put_u8(self.mars as u8);

        Ok(())
    }
}

impl Decode<BytesMut> for PreloginMessage {
    fn decode(src: &mut BytesMut) -> Result<Self>
    where
        Self: Sized,
    {
        let mut cursor = Cursor::new(src);
        let mut ret = PreloginMessage::new();

        // read all options
        loop {
            let token = cursor.read_u8()?;

            // read until terminator
            if token == 0xff {
                break;
            }

            let offset = cursor.read_u16::<BigEndian>()?;
            let length = cursor.read_u16::<BigEndian>()?;
            let old_pos = cursor.position();

            cursor.set_position(offset as u64);

            // verify whether the server acts in accordance to what we requested
            // and if we can handle on what we seemingly agreed to
            // TODO: support parsing more
            match token {
                // version
                0 => {
                    ret.version = cursor.read_u32::<BigEndian>()?;
                    ret.sub_build = cursor.read_u16::<BigEndian>()?;
                }
                // encryption
                1 => {
                    let encrypt = cursor.read_u8()?;
                    ret.encryption = tds::EncryptionLevel::try_from(encrypt).map_err(|_| {
                        Error::Protocol(format!("invalid encryption value: {}", encrypt).into())
                    })?;
                }
                3 => debug_assert_eq!(length, 0), // threadid
                4 => debug_assert_eq!(length, 1), // mars
                _ => panic!("unsupported prelogin token: {}", token),
            }

            cursor.set_position(old_pos);
        }

        Ok(ret)
    }
}

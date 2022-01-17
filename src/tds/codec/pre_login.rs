use super::guid::reorder_bytes;
use super::{Decode, Encode};
use crate::{tds, Error, Result};
use byteorder::{BigEndian, LittleEndian, ReadBytesExt};
use bytes::{BufMut, BytesMut};
use std::convert::TryFrom;
use std::io::{self, Cursor, Read};
use tds::EncryptionLevel;
use uuid::Uuid;

/// Client application activity id token used for debugging purposes introduced
/// in TDS 7.4.
#[allow(unused)]
#[derive(Debug)]
pub struct ActivityId {
    id: Uuid,
    sequence: u32,
}

/// The prelogin packet used to initialize a connection
#[derive(Debug)]
pub struct PreloginMessage {
    /// [BE] token=0x00
    /// Either the driver version or the version of the SQL server
    pub version: u32,
    pub sub_build: u16,
    /// token=0x01
    pub encryption: EncryptionLevel,
    /// token=0x02
    pub instance_name: Option<String>,
    /// [client] threadid for debugging purposes, token=0x03
    pub thread_id: u32,
    /// token=0x04
    pub mars: bool,
    /// token=0x05
    pub activity_id: Option<ActivityId>,
    /// token=0x06
    pub fed_auth_required: bool,
    pub nonce: Option<[u8; 32]>,
}

impl PreloginMessage {
    pub fn new() -> PreloginMessage {
        let driver_version = crate::get_driver_version();
        PreloginMessage {
            version: driver_version as u32,
            sub_build: (driver_version >> 32) as u16,
            encryption: EncryptionLevel::NotSupported,
            instance_name: None,
            thread_id: 0,
            mars: false,
            activity_id: None,
            fed_auth_required: false,
            nonce: None,
        }
    }

    pub fn negotiated_encryption(&self, expected: EncryptionLevel) -> EncryptionLevel {
        match (expected, self.encryption) {
            (EncryptionLevel::NotSupported, EncryptionLevel::NotSupported) => {
                EncryptionLevel::NotSupported
            }
            (EncryptionLevel::Off, EncryptionLevel::Off) => EncryptionLevel::Off,
            (EncryptionLevel::On, EncryptionLevel::Off)
            | (EncryptionLevel::On, EncryptionLevel::NotSupported) => {
                panic!("Server does not allow the requested encryption level.")
            }
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
                0x00 => {
                    ret.version = cursor.read_u32::<BigEndian>()?;
                    ret.sub_build = cursor.read_u16::<BigEndian>()?;
                }
                // encryption
                0x01 => {
                    let encrypt = cursor.read_u8()?;
                    ret.encryption = tds::EncryptionLevel::try_from(encrypt).map_err(|_| {
                        Error::Protocol(format!("invalid encryption value: {}", encrypt).into())
                    })?;
                }
                // instance name
                0x02 => {
                    let mut bytes = Vec::new();
                    let mut next_byte = cursor.read_u8()?;

                    while next_byte != 0x00 {
                        bytes.push(next_byte);
                        next_byte = cursor.read_u8()?;
                    }

                    if !bytes.is_empty() {
                        ret.instance_name = Some(String::from_utf8_lossy(&bytes).into_owned());
                    }
                }
                // threadid (should be empty when sent from server to client)
                0x03 => debug_assert_eq!(length, 0),
                // mars
                0x04 => {
                    ret.mars = cursor.read_u8()? != 0;
                }
                // activity id
                0x05 => {
                    // Data is a Guid, 16 bytes and ordered the wrong way around
                    // than Uuid.
                    let mut data = [0u8; 16];

                    cursor.read_exact(&mut data)?;
                    reorder_bytes(&mut data);

                    ret.activity_id = Some(ActivityId {
                        id: Uuid::from_bytes(data),
                        sequence: cursor.read_u32::<LittleEndian>()?,
                    });
                }
                // fed auth
                0x06 => {
                    ret.fed_auth_required = cursor.read_u8()? != 0;
                }
                // nonce
                0x07 => {
                    let mut data = [0u8; 32];

                    for item in data.iter_mut() {
                        *item = cursor.read_u8()?;
                    }

                    ret.nonce = Some(data);
                }
                _ => panic!("unsupported prelogin token: {}", token),
            }

            cursor.set_position(old_pos);
        }

        Ok(ret)
    }
}

use super::{read_varchar, Decode, FixedLenType, TypeInfo, VarLenType};
use bitflags::bitflags;
use bytes::{Buf, BytesMut};
use pretty_hex::*;

#[derive(Debug)]
pub struct TokenColMetaData {
    pub columns: Vec<MetaDataColumn>,
}

impl TokenColMetaData {
    pub fn row_size(&self) -> usize {
        self.columns.iter().fold(0, |acc, col| acc + col.size())
    }
}

#[derive(Debug)]
pub struct MetaDataColumn {
    pub base: BaseMetaDataColumn,
    pub col_name: String,
}

impl MetaDataColumn {
    pub fn size(&self) -> usize {
        match self.base.ty {
            TypeInfo::FixedLen(fixed_len_type) => match fixed_len_type {
                FixedLenType::Null => 0,
                FixedLenType::Int1 => 1,
                FixedLenType::Bit => 1,
                FixedLenType::Int2 => 2,
                FixedLenType::Int4 => 4,
                FixedLenType::Datetime4 => 4,
                FixedLenType::Float4 => 4,
                FixedLenType::Money => 8,
                FixedLenType::Datetime => 8,
                FixedLenType::Float8 => 8,
                FixedLenType::Money4 => 4,
                FixedLenType::Int8 => 8,
            },
            TypeInfo::VarLenSized(_, size, _) => size,
            TypeInfo::VarLenSizedPrecision {
                ty: _,
                size,
                precision: _,
                scale: _,
            } => size,
        }
    }
}

#[derive(Debug)]
pub struct BaseMetaDataColumn {
    pub flags: ColmetaDataFlags,
    pub ty: TypeInfo,
}

bitflags! {
    pub struct ColmetaDataFlags: u16 {
        const CDF_NULLABLE            = 1<<0;
        const CDF_CASE_SENSITIVE      = 1<<1;
        const CDF_UPDATEABLE          = 1<<3;
        const CDF_UPDATEABLE_UNKNOWN  = 1<<4;
        const CDF_IDENTITY            = 1<<5;
        const CDF_COMPUTED            = 1<<7;
        // 2 bits reserved for ODBC gateway
        const CDF_FIXED_LEN_CLR_TYPE  = 1<<10;
        const CDF_SPARSE_COLUMN_SET   = 1<<11;
        const CDF_ENCRYPTED           = 1<<12;
        const CDF_HIDDEN              = 1<<13;
        const CDF_KEY                 = 1<<14;
        const CDF_NULLABLE_UNKNOWN    = 1<<15;
    }
}

impl Decode<BytesMut> for TokenColMetaData {
    fn decode(src: &mut BytesMut) -> crate::Result<Self>
    where
        Self: Sized,
    {
        let column_count = src.get_u16_le();
        let mut columns = Vec::with_capacity(column_count as usize);

        if column_count > 0 && column_count < 0xffff {
            /*// CekTable (Column Encryption Keys)
            let cek_count = try!(self.read_u16::<LittleEndian>());
            // TODO: Cek/encryption stuff not implemented yet
            assert_eq!(cek_count, 0);*/

            // read all metadata for each column
            for _ in 0..column_count {
                let base = BaseMetaDataColumn::decode(src)?;
                let col_name_len = src.get_u8();

                let meta = MetaDataColumn {
                    base,
                    col_name: read_varchar(src, col_name_len)?,
                };
                columns.push(meta);
            }
        }

        Ok(TokenColMetaData { columns })
    }
}

impl Decode<BytesMut> for BaseMetaDataColumn {
    fn decode(src: &mut BytesMut) -> crate::Result<Self>
    where
        Self: Sized,
    {
        let _user_ty = src.get_u32_le();
        let raw_flags = src.get_u16_le();
        let flags = ColmetaDataFlags::from_bits(raw_flags).unwrap();
        let ty = TypeInfo::decode(src)?;

        match ty {
            TypeInfo::VarLenSized(VarLenType::Text, _, _) => {
                src.get_u16_le();
                src.get_u16_le();
                src.get_u16_le();

                // table name (wtf)
                let len = src.get_u16_le();
                read_varchar(src, len)?;
            }
            _ => (),
        }

        // TODO: for type={text, ntext, and image} TABLENAME

        /*// CryptoMetaData
        let cmd_ordinal = try!(self.read_u16::<LittleEndian>());
        let cmd_user_ty = try!(self.read_u32::<LittleEndian>());
        let cmd_ty_info: TypeInfo = try!(self.unserialize(ctx));
        let cmd_encryption_algo = try!(self.read_u8());
        // TODO:
        assert_eq!(cmd_encryption_algo, 0);
        let cmd_algo_name = try!(self.read_varchar::<u8>());
        let cmd_algo_type = try!(self.read_u8());
        let cmd_norm_version = try!(self.read_u8());*/

        Ok(BaseMetaDataColumn { flags, ty })
    }
}

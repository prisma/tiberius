use crate::{
    tds::codec::{read_varchar, FixedLenType, TypeInfo, VarLenType},
    ColumnData, SqlReadBytes,
};
use bitflags::bitflags;

#[derive(Debug)]
pub struct TokenColMetaData {
    pub columns: Vec<MetaDataColumn>,
}

#[derive(Debug)]
pub struct MetaDataColumn {
    pub base: BaseMetaDataColumn,
    pub col_name: String,
}

#[derive(Debug)]
pub struct BaseMetaDataColumn {
    pub flags: ColmetaDataFlags,
    pub ty: TypeInfo,
}

impl BaseMetaDataColumn {
    pub(crate) fn null_value(&self) -> ColumnData<'static> {
        match self.ty {
            TypeInfo::FixedLen(ty) => match ty {
                FixedLenType::Null => ColumnData::I32(None),
                FixedLenType::Int1 => ColumnData::I8(None),
                FixedLenType::Bit => ColumnData::Bit(None),
                FixedLenType::Int2 => ColumnData::I16(None),
                FixedLenType::Int4 => ColumnData::I32(None),
                FixedLenType::Datetime4 => ColumnData::SmallDateTime(None),
                FixedLenType::Float4 => ColumnData::F32(None),
                FixedLenType::Money => ColumnData::F64(None),
                FixedLenType::Datetime => ColumnData::DateTime(None),
                FixedLenType::Float8 => ColumnData::F64(None),
                FixedLenType::Money4 => ColumnData::F32(None),
                FixedLenType::Int8 => ColumnData::I64(None),
            },
            TypeInfo::VarLenSized(ty, _, _) => match ty {
                VarLenType::Guid => ColumnData::Guid(None),
                VarLenType::Intn => ColumnData::I32(None),
                VarLenType::Bitn => ColumnData::Bit(None),
                VarLenType::Decimaln => ColumnData::Numeric(None),
                VarLenType::Numericn => ColumnData::Numeric(None),
                VarLenType::Floatn => ColumnData::F32(None),
                VarLenType::Money => ColumnData::F64(None),
                VarLenType::Datetimen => ColumnData::DateTime(None),
                #[cfg(feature = "tds73")]
                VarLenType::Daten => ColumnData::Date(None),
                #[cfg(feature = "tds73")]
                VarLenType::Timen => ColumnData::Time(None),
                #[cfg(feature = "tds73")]
                VarLenType::Datetime2 => ColumnData::DateTime2(None),
                #[cfg(feature = "tds73")]
                VarLenType::DatetimeOffsetn => ColumnData::DateTimeOffset(None),
                VarLenType::BigVarBin => ColumnData::Binary(None),
                VarLenType::BigVarChar => ColumnData::String(None),
                VarLenType::BigBinary => ColumnData::Binary(None),
                VarLenType::BigChar => ColumnData::String(None),
                VarLenType::NVarchar => ColumnData::String(None),
                VarLenType::NChar => ColumnData::String(None),
                VarLenType::Xml => ColumnData::Xml(None),
                VarLenType::Udt => todo!("User-defined types not supported"),
                VarLenType::Text => ColumnData::String(None),
                VarLenType::Image => ColumnData::Binary(None),
                VarLenType::NText => ColumnData::String(None),
                VarLenType::SSVariant => todo!(),
            },
            TypeInfo::VarLenSizedPrecision { ty, .. } => match ty {
                VarLenType::Guid => ColumnData::Guid(None),
                VarLenType::Intn => ColumnData::I32(None),
                VarLenType::Bitn => ColumnData::Bit(None),
                VarLenType::Decimaln => ColumnData::Numeric(None),
                VarLenType::Numericn => ColumnData::Numeric(None),
                VarLenType::Floatn => ColumnData::F32(None),
                VarLenType::Money => ColumnData::F64(None),
                VarLenType::Datetimen => ColumnData::DateTime(None),
                #[cfg(feature = "tds73")]
                VarLenType::Daten => ColumnData::Date(None),
                #[cfg(feature = "tds73")]
                VarLenType::Timen => ColumnData::Time(None),
                #[cfg(feature = "tds73")]
                VarLenType::Datetime2 => ColumnData::DateTime2(None),
                #[cfg(feature = "tds73")]
                VarLenType::DatetimeOffsetn => ColumnData::DateTimeOffset(None),
                VarLenType::BigVarBin => ColumnData::Binary(None),
                VarLenType::BigVarChar => ColumnData::String(None),
                VarLenType::BigBinary => ColumnData::Binary(None),
                VarLenType::BigChar => ColumnData::String(None),
                VarLenType::NVarchar => ColumnData::String(None),
                VarLenType::NChar => ColumnData::String(None),
                VarLenType::Xml => ColumnData::Xml(None),
                VarLenType::Udt => todo!("User-defined types not supported"),
                VarLenType::Text => ColumnData::String(None),
                VarLenType::Image => ColumnData::Binary(None),
                VarLenType::NText => ColumnData::String(None),
                VarLenType::SSVariant => todo!(),
            },
            TypeInfo::Xml { .. } => ColumnData::Xml(None),
        }
    }
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

#[allow(dead_code)]
impl TokenColMetaData {
    pub(crate) async fn decode<R>(src: &mut R) -> crate::Result<Self>
    where
        R: SqlReadBytes + Unpin,
    {
        let column_count = src.read_u16_le().await?;
        let mut columns = Vec::with_capacity(column_count as usize);

        if column_count > 0 && column_count < 0xffff {
            /*// CekTable (Column Encryption Keys)
            let cek_count = try!(self.read_u16::<LittleEndian>());
            // TODO: Cek/encryption stuff not implemented yet
            assert_eq!(cek_count, 0);*/

            // read all metadata for each column
            for _ in 0..column_count {
                let base = BaseMetaDataColumn::decode(src).await?;
                let col_name_len = src.read_u8().await?;

                let meta = MetaDataColumn {
                    base,
                    col_name: read_varchar(src, col_name_len).await?,
                };

                columns.push(meta);
            }
        }

        Ok(TokenColMetaData { columns })
    }
}

impl BaseMetaDataColumn {
    pub(crate) async fn decode<R>(src: &mut R) -> crate::Result<Self>
    where
        R: SqlReadBytes + Unpin,
    {
        let _user_ty = src.read_u32_le().await?;
        let raw_flags = src.read_u16_le().await?;
        let flags = ColmetaDataFlags::from_bits(raw_flags).unwrap();
        let ty = TypeInfo::decode(src).await?;

        match ty {
            TypeInfo::VarLenSized(VarLenType::Text, _, _) => {
                src.read_u16_le().await?;
                src.read_u16_le().await?;
                src.read_u16_le().await?;

                // table name
                let len = src.read_u16_le().await?;
                read_varchar(src, len).await?;
            }
            TypeInfo::VarLenSized(VarLenType::NText, _, _) => {
                src.read_u8().await?;
                // table name
                let len = src.read_u16_le().await?;
                read_varchar(src, len as usize).await?;
            }
            TypeInfo::VarLenSized(VarLenType::Image, _, _) => {
                src.read_u8().await?;

                // table name
                let len = src.read_u16_le().await?;
                read_varchar(src, len).await?;
            }
            _ => (),
        }

        Ok(BaseMetaDataColumn { flags, ty })
    }
}

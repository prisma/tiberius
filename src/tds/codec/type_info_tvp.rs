use std::borrow::BorrowMut;

use asynchronous_codec::BytesMut;
use bytes::BufMut;

use crate::ColumnData;

use super::{BytesMutWithTypeInfo, Encode, FixedLenType, MetaDataColumn, TypeInfo, VarLenContext};

const TVPTYPE: u8 = 0xF3;

#[derive(Debug)]
pub struct TypeInfoTvp<'a> {
    scheema_name: &'a str,
    db_type_name: &'a str,
    columns: Option<Vec<MetaDataColumn<'a>>>,
    data: Vec<Vec<ColumnData<'a>>>,
}

impl<'a> Encode<BytesMut> for TypeInfoTvp<'a> {
    fn encode(self, dst: &mut BytesMut) -> crate::Result<()> {
        // TVPTYPE        =   %xF3
        // TVP_TYPE_INFO  =   TVPTYPE
        //                    TVP_TYPENAME
        //                    TVP_COLMETADATA
        //                    [TVP_ORDER_UNIQUE]
        //                    [TVP_COLUMN_ORDERING]
        //                    TVP_END_TOKEN

        dst.put_u8(TVPTYPE);
        put_b_varchar("", dst); // DB name
        put_b_varchar(self.scheema_name, dst);
        put_b_varchar(self.db_type_name, dst);

        if let Some(ref columns_metadata) = self.columns {
            dst.put_u16_le(columns_metadata.len() as u16);
            for col in columns_metadata {
                // TvpColumnMetaData = UserType
                //                     Flags
                //                     TYPE_INFO
                //                     ColName ; Column metadata instance
                dst.put_u32_le(0_u32);
                col.base.clone().encode(dst)?; // Arc would look better than this clone, but might be actually slower
                put_b_varchar("", dst); // 2.2.5.5.5.1: ColName MUST be a zero-length string in the TVP.
                                        // put_b_varchar(col.col_name, dst);
            }
        } else {
            dst.put_u16_le(0xFFFF_u16); // TVP_NULL_TOKEN, server knows the type (never worked in practice)
        }

        dst.put_u8(0_u8); // TVP_END_TOKEN

        for row in self.data.into_iter() {
            dst.put_u8(0x01u8); // TVP_ROW_TOKEN = %x01
            for (i, col) in row.into_iter().enumerate() {
                let mut dst_ti = BytesMutWithTypeInfo::new(dst);
                if let Some(ref metadata) = self.columns {
                    dst_ti = dst_ti.with_type_info(&metadata[i].base.ty);
                }
                col.encode(&mut dst_ti)?;
            }
        }
        // TVP_ROW_TOKEN = %x01 ; A row as defined by TVP_COLMETADATA follows
        // TvpColumnData = TYPE_VARBYTE ; Actual value must match metadata for the column
        // AllColumnData = *TvpColumnData ; Chunks of data, one per non-default column defined
        //                                ; in TVP_COLMETADATA
        // TVP_ROW       = TVP_ROW_TOKEN
        //                 AllColumnData

        dst.put_u8(0_u8); // TVP_END_TOKEN

        Ok(())
    }
}

fn put_b_varchar<T: AsRef<str>>(s: T, dst: &mut BytesMut) {
    let len_pos = dst.len();
    dst.put_u8(0u8);
    let mut length = 0_u8;

    for chr in s.as_ref().encode_utf16() {
        dst.put_u16_le(chr);
        length += 1;
    }
    let dst: &mut [u8] = dst.borrow_mut();
    dst[len_pos] = length;
}

impl<'a> TypeInfoTvp<'a> {
    pub fn new(type_name: &'a str, rows: Vec<Vec<ColumnData<'a>>>) -> TypeInfoTvp<'a> {
        let (scheema_name, db_type_name) = if let Some((s, t)) = type_name.split_once(".") {
            (s, t)
        } else {
            ("", type_name.as_ref())
        };
        TypeInfoTvp {
            scheema_name,
            db_type_name,
            columns: None,
            data: rows,
        }
    }

    pub fn with_metadata(self, metadata: Vec<MetaDataColumn<'a>>) -> TypeInfoTvp<'_> {
        let mut metadata = metadata;
        // 2.2.5.5.5.3
        for mdc in metadata.iter_mut() {
            let ty_replace =
                match mdc.base.ty {
                    TypeInfo::FixedLen(ref ty) => {
                        match ty {
                            FixedLenType::Int1 => Some(TypeInfo::VarLenSized(VarLenContext::new(
                                super::VarLenType::Intn,
                                1,
                                None,
                            ))),
                            FixedLenType::Bit => Some(TypeInfo::VarLenSized(VarLenContext::new(
                                super::VarLenType::Bitn,
                                1,
                                None,
                            ))),
                            FixedLenType::Int2 => Some(TypeInfo::VarLenSized(VarLenContext::new(
                                super::VarLenType::Intn,
                                2,
                                None,
                            ))),
                            FixedLenType::Int4 => Some(TypeInfo::VarLenSized(VarLenContext::new(
                                super::VarLenType::Intn,
                                4,
                                None,
                            ))),
                            FixedLenType::Datetime4 => Some(TypeInfo::VarLenSized(
                                VarLenContext::new(super::VarLenType::Datetimen, 4, None),
                            )),
                            FixedLenType::Float4 => Some(TypeInfo::VarLenSized(
                                VarLenContext::new(super::VarLenType::Floatn, 4, None),
                            )),
                            FixedLenType::Money => Some(TypeInfo::VarLenSized(VarLenContext::new(
                                super::VarLenType::Money,
                                8,
                                None,
                            ))),
                            FixedLenType::Datetime => Some(TypeInfo::VarLenSized(
                                VarLenContext::new(super::VarLenType::Datetimen, 8, None),
                            )),
                            FixedLenType::Float8 => Some(TypeInfo::VarLenSized(
                                VarLenContext::new(super::VarLenType::Floatn, 8, None),
                            )),
                            FixedLenType::Money4 => Some(TypeInfo::VarLenSized(
                                VarLenContext::new(super::VarLenType::Money, 4, None),
                            )),
                            FixedLenType::Int8 => Some(TypeInfo::VarLenSized(VarLenContext::new(
                                super::VarLenType::Intn,
                                8,
                                None,
                            ))),
                            _ => None,
                        }
                    }
                    // TypeInfo::VarLenSized(ref ctx) => match ctx.r#type() {
                    //     super::VarLenType::Guid => todo!(),
                    //     super::VarLenType::Intn => todo!(),
                    //     super::VarLenType::Bitn => todo!(),
                    //     super::VarLenType::Decimaln => todo!(),
                    //     super::VarLenType::Numericn => todo!(),
                    //     super::VarLenType::Floatn => todo!(),
                    //     super::VarLenType::Money => todo!(),
                    //     super::VarLenType::Datetimen => todo!(),
                    //     super::VarLenType::Daten => todo!(),
                    //     super::VarLenType::Timen => todo!(),
                    //     super::VarLenType::Datetime2 => todo!(),
                    //     super::VarLenType::DatetimeOffsetn => todo!(),
                    //     super::VarLenType::BigVarBin => todo!(),
                    //     super::VarLenType::BigVarChar => todo!(),
                    //     super::VarLenType::BigBinary => todo!(),
                    //     super::VarLenType::BigChar => todo!(),
                    //     super::VarLenType::NVarchar => todo!(),
                    //     super::VarLenType::NChar => todo!(),
                    //     super::VarLenType::Xml => todo!(),
                    //     super::VarLenType::Udt => todo!(),
                    //     super::VarLenType::Text => todo!(),
                    //     super::VarLenType::Image => todo!(),
                    //     super::VarLenType::NText => todo!(),
                    //     super::VarLenType::SSVariant => todo!(),
                    // },
                    // TypeInfo::VarLenSizedPrecision {
                    //     ty,
                    //     size,
                    //     precision,
                    //     scale,
                    // } => match ty {
                    //     super::VarLenType::Guid => todo!(),
                    //     super::VarLenType::Intn => todo!(),
                    //     super::VarLenType::Bitn => todo!(),
                    //     super::VarLenType::Decimaln => todo!(),
                    //     super::VarLenType::Numericn => todo!(),
                    //     super::VarLenType::Floatn => todo!(),
                    //     super::VarLenType::Money => todo!(),
                    //     super::VarLenType::Datetimen => todo!(),
                    //     super::VarLenType::Daten => todo!(),
                    //     super::VarLenType::Timen => todo!(),
                    //     super::VarLenType::Datetime2 => todo!(),
                    //     super::VarLenType::DatetimeOffsetn => todo!(),
                    //     super::VarLenType::BigVarBin => todo!(),
                    //     super::VarLenType::BigVarChar => todo!(),
                    //     super::VarLenType::BigBinary => todo!(),
                    //     super::VarLenType::BigChar => todo!(),
                    //     super::VarLenType::NVarchar => todo!(),
                    //     super::VarLenType::NChar => todo!(),
                    //     super::VarLenType::Xml => todo!(),
                    //     super::VarLenType::Udt => todo!(),
                    //     super::VarLenType::Text => todo!(),
                    //     super::VarLenType::Image => todo!(),
                    //     super::VarLenType::NText => todo!(),
                    //     super::VarLenType::SSVariant => todo!(),
                    // },
                    _ => None,
                };
            if let Some(ty) = ty_replace {
                mdc.base.ty = ty;
            }
        }
        TypeInfoTvp {
            columns: Some(metadata),
            ..self
        }
    }
}

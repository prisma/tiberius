macro_rules! uint_enum {
    ($( #[$gattr:meta] )* pub enum $ty:ident { $( $( #[$attr:meta] )* $variant:ident = $val:expr,)* }) => {
        #[allow(missing_docs)]
        uint_enum!($( #[$gattr ])* (pub) enum $ty { $( $( #[$attr] )* $variant = $val, )* });
    };
    ($( #[$gattr:meta] )* enum $ty:ident { $( $( #[$attr:meta] )* $variant:ident = $val:expr,)* }) => {
        #[allow(missing_docs)]
        uint_enum!($( #[$gattr ])* () enum $ty { $( $( #[$attr] )* $variant = $val, )* });
    };

    ($( #[$gattr:meta] )* ( $($vis:tt)* ) enum $ty:ident { $( $( #[$attr:meta] )* $variant:ident = $val:expr,)* }) => {
        #[derive(Debug, Copy, Clone, PartialEq)]
        $( #[$gattr] )*
        #[allow(missing_docs)]
        $( $vis )* enum $ty {
            $( $( #[$attr ])* $variant = $val, )*
        }

        impl ::std::convert::TryFrom<u8> for $ty {
            type Error = ();
            fn try_from(n: u8) -> ::std::result::Result<$ty, ()> {
                match n {
                    $( x if x == $ty::$variant as u8 => Ok($ty::$variant), )*
                    _ => Err(()),
                }
            }
        }

        impl ::std::convert::TryFrom<u32> for $ty {
            type Error = ();
            fn try_from(n: u32) -> ::std::result::Result<$ty, ()> {
                match n {
                    $( x if x == $ty::$variant as u32 => Ok($ty::$variant), )*
                    _ => Err(()),
                }
            }
        }
    }
}

macro_rules! to_sql {
    ($target:ident, $( $ty:ty: ($name:expr, $val:expr) ;)* ) => {
        $(
            impl crate::ToSql for $ty {
                fn to_sql(&self) -> (std::borrow::Cow<'static, str>, crate::tds::codec::ColumnData<'_>) {
                    let $target = self;
                    (std::borrow::Cow::Borrowed($name), $val)
                }
            }

            impl crate::ToSql for Option<$ty> {
                fn to_sql(&self) -> (std::borrow::Cow<'static, str>, crate::tds::codec::ColumnData<'_>) {
                    let val = match self {
                        None => crate::tds::codec::ColumnData::None,
                        Some(item) => {
                            let $target = item;
                            $val
                        }
                    };

                    (std::borrow::Cow::Borrowed($name), val)
                }
            }
        )*
    };
}

macro_rules! from_column_data {
    ($( $ty:ty: $($pat:pat => $val:expr),* );* ) => {
        $(
            impl<'a> std::convert::TryFrom<&'a ColumnData<'a>> for $ty {
                type Error = crate::Error;

                fn try_from(data: &ColumnData<'a>) -> crate::Result<Self> {
                    match data {
                        $( $pat => Ok($val), )*
                        _ => Err(crate::Error::Conversion(format!("cannot interpret {:?} as an {} value", data, stringify!($ty)).into()))
                    }
                }
            }
        )*
    };
}

#[macro_export]
macro_rules! uint_enum {
    ($( #[$gattr:meta] )* pub enum $ty:ident { $( $( #[$attr:meta] )* $variant:ident = $val:expr,)* }) => {
        uint_enum!($( #[$gattr ])* (pub) enum $ty { $( $( #[$attr] )* $variant = $val, )* });
    };
    ($( #[$gattr:meta] )* enum $ty:ident { $( $( #[$attr:meta] )* $variant:ident = $val:expr,)* }) => {
        uint_enum!($( #[$gattr ])* () enum $ty { $( $( #[$attr] )* $variant = $val, )* });
    };

    ($( #[$gattr:meta] )* ( $($vis:tt)* ) enum $ty:ident { $( $( #[$attr:meta] )* $variant:ident = $val:expr,)* }) => {
        #[derive(Debug, Copy, Clone, PartialEq)]
        $( #[$gattr] )*
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

#[macro_export]
macro_rules! to_sql {
    ($target:ident, $( $ty:ty: ($name:expr, $val:expr) ;)* ) => {
        $(
            impl crate::prepared::ToSql for $ty {
                fn to_sql(&self) -> (&'static str, crate::protocol::codec::ColumnData) {
                    let $target = self;
                    ($name, $val)
                }
            }

            impl crate::prepared::ToSql for Option<$ty> {
                fn to_sql(&self) -> (&'static str, crate::protocol::codec::ColumnData) {
                    let val = match self {
                        None => crate::protocol::codec::ColumnData::None,
                        Some(item) => {
                            let $target = item;
                            $val
                        }
                    };

                    ($name, val)
                }
            }
        )*
    };
}

#[macro_export]
macro_rules! from_column_data {
    ($( $ty:ty: $($pat:pat => $val:expr),* );* ) => {
        $(
            impl<'a> std::convert::TryFrom<&'a ColumnData<'a>> for $ty {
                type Error = crate::Error;

                fn try_from(data: &ColumnData) -> crate::Result<Self> {
                    match data {
                        $( $pat => Ok($val), )*
                        _ => Err(crate::Error::Conversion(format!("cannot interpret {:?} as an {} value", data, stringify!($ty)).into()))
                    }
                }
            }
        )*
    };
}

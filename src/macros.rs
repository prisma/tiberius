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
    ($target:ident, $( $ty:ty: ($variant:expr, $val:expr) ;)* ) => {
        $(
            impl crate::ToSql for $ty {
                fn to_sql(&self) -> crate::tds::codec::ColumnData<'_> {
                    let $target = self;
                    $variant(Some($val))
                }
            }

            impl crate::ToSql for Option<$ty> {
                fn to_sql(&self) -> crate::tds::codec::ColumnData<'_> {
                    match self {
                        Some(val) => {
                            let $target = val;
                            $variant(Some($val))
                        },
                        None => $variant(None)
                    }
                }
            }

            impl crate::ToSql for &Option<$ty> {
                fn to_sql(&self) -> crate::tds::codec::ColumnData<'_> {
                    match self {
                        Some(val) => {
                            let $target = val;
                            $variant(Some($val))
                        },
                        None => $variant(None)
                    }
                }
            }
        )*
    };
}

macro_rules! into_sql {
    ($target:ident, $( $ty:ty: ($variant:expr, $val:expr) ;)* ) => {
        $(
            impl IntoSql for $ty {
                fn into_sql(self) -> ColumnData<'static> {
                    let $target = self;
                    $variant(Some($val))
                }
            }

            impl IntoSql for Option<$ty> {
                fn into_sql(self) -> ColumnData<'static> {
                    match self {
                        Some(val) => {
                            let $target = val;
                            $variant(Some($val))
                        },
                        None => $variant(None)
                    }
                }
            }
        )*
    }
}

macro_rules! from_sql {
    ($( $ty:ty: $($pat:pat => ($borrowed_val:expr, $owned_val:expr)),* );* ) => {
        $(
            impl<'a> crate::FromSql<'a> for $ty {
                fn from_sql(data: &'a crate::tds::codec::ColumnData<'static>) -> crate::Result<Option<Self>> {
                    match data {
                        $( $pat => Ok($borrowed_val), )*
                        _ => Err(crate::Error::Conversion(format!("cannot interpret {:?} as an {} value", data, stringify!($ty)).into()))
                    }
                }
            }

            impl crate::FromSqlOwned for $ty {
                fn from_sql_owned(data: crate::tds::codec::ColumnData<'static>) -> crate::Result<Option<Self>> {
                    match data {
                        $( $pat => Ok($owned_val), )*
                        _ => Err(crate::Error::Conversion(format!("cannot interpret {:?} as an {} value", data, stringify!($ty)).into()))
                    }
                }
            }
        )*
    };
    ($( $ty:ty: $($pat:pat => $borrowed_val:expr),* );* ) => {
        $(
            impl<'a> crate::FromSql<'a> for $ty {
                fn from_sql(data: &'a crate::tds::codec::ColumnData<'static>) -> crate::Result<Option<Self>> {
                    match data {
                        $( $pat => Ok($borrowed_val), )*
                        _ => Err(crate::Error::Conversion(format!("cannot interpret {:?} as an {} value", data, stringify!($ty)).into()))
                    }
                }
            }

            impl crate::FromSqlOwned for $ty {
                fn from_sql_owned(data: crate::tds::codec::ColumnData<'static>) -> crate::Result<Option<Self>> {
                    match data {
                        $( $pat => Ok($borrowed_val), )*
                        _ => Err(crate::Error::Conversion(format!("cannot interpret {:?} as an {} value", data, stringify!($ty)).into()))
                    }
                }
            }
        )*
    };
}

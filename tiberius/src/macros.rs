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

/// prepares a statement which selects a passed value
/// this tests serialization of a parameter and deserialization
/// atlast it checks if the received value is the same as the sent value
/// it also checks if the time formatted is correct
#[cfg(test)]
#[macro_export]
macro_rules! test_timedatatype {
    ( $($name:ident: $ty:ty = $val:expr => $str_val:expr),* ) => {
        $(
            #[test]
            fn $name() {
                let future = SqlConnection::connect(connection_string().as_ref())
                    .and_then(|conn| {
                        conn.query("SELECT @P1, convert(varchar, @P1, 121)", &[&$val]).for_each(|row| {
                            assert_eq!(row.get::<_, $ty>(0), $val);
                            assert_eq!(row.get::<_, &str>(1), $str_val);
                            Ok(())
                        })
                    });
                current_thread::block_on_all(future).unwrap();
            }
        )*
    }
}

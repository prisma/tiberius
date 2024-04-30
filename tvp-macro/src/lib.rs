//! This is a utility macro crate used to generate trivial trait implementations used in rust-to-SQL data exchange
extern crate proc_macro;

#[macro_use]
extern crate quote;
#[macro_use]
extern crate syn;

use proc_macro::TokenStream;

macro_rules! sp_quote {
    ($($t:tt)*) => (quote_spanned!(proc_macro2::Span::call_site() => $($t)*))
}

mod attr;
mod table_value_param;

/// This macro generates a trivial implementation of the `TableValueRow` trait.
/// # Applications
/// Could be applied to structures that represent rows of a Table Value params.
/// # Example
/// ```rust,ignore
/// # use tiberius::*;
/// #[derive(TableValueRow)]
/// pub struct SomeGeoList {
///   #[colname = "SomeID"]
///   pub id: i32,
///   #[colname = "LastSyncIPGeoLat"]
///   pub lat: Numeric,
///   #[colname = "LastSyncIPGeoLong"]
///   pub lon: Numeric,
/// }
/// ```
#[proc_macro_derive(TableValueRow, attributes(colname))]
pub fn table_value_param(input: TokenStream) -> TokenStream {
    //println!("intput: {}", input.to_string());
    let ast: syn::DeriveInput = syn::parse(input).expect("Couldn't parse item");
    let result = match ast.data {
        syn::Data::Enum(_) => panic!("n/a for enums, makes sense for structs only"),
        syn::Data::Struct(ref s) => table_value_param::for_struct(&ast, &s.fields),
        syn::Data::Union(_) => panic!("doesn't work with unions"),
    };
    result.into()
}

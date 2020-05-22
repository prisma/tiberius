extern crate proc_macro;

#[proc_macro_attribute]
pub fn test_on_runtimes(_attr: proc_macro::TokenStream, input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    // let attributes = parse_macro_input!(attr as syn::Attribute);
    // todo: get altered connstr from attributes

    let func = syn::parse_macro_input!(input as syn::ItemFn);
    // todo: ensure func is async

    let func_name = func.sig.ident.clone();
    let async_std_test = quote::format_ident!("{}_{}", func_name, "async_std");
    let tokio_test = quote::format_ident!("{}_{}", func_name, "tokio");

    let tokens = quote::quote!{
        #func

        #[test]
        fn #async_std_test()-> Result<()> {
            LOGGER_SETUP.call_once(|| {
                env_logger::init();
            });
            async_std::task::block_on(async {
                let builder = tiberius_async_std::ClientBuilder::from_ado_string(&*CONN_STR)?;
                let conn = builder.build().await?.into();
                #func_name(conn).await?;
                Ok(())
            })
        }

        #[test]
        fn #tokio_test()-> Result<()> {
            LOGGER_SETUP.call_once(|| {
                env_logger::init();
            });
            let mut rt = tokio::runtime::Runtime::new()?;
            rt.block_on(async {
                let builder = tiberius_tokio::ClientBuilder::from_ado_string(&*CONN_STR)?;
                let conn = builder.build().await?.into();
                #func_name(conn).await?;
                Ok(())
            })
        }
    };

    proc_macro::TokenStream::from(tokens)
}

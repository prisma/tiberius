extern crate proc_macro;
use darling::FromMeta;

#[derive(Debug, FromMeta)]
struct MacroArgs {
    #[darling(default)]
    connection_string: Option<String>,
}

#[proc_macro_attribute]
pub fn test_on_runtimes(args: proc_macro::TokenStream, input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    // let attributes = parse_macro_input!(attr as syn::Attribute);
    // todo: get altered connstr from attributes

    let attr_args = syn::parse_macro_input!(args as syn::AttributeArgs);

    let args = match MacroArgs::from_list(&attr_args) {
        Ok(v) => v,
        Err(e) => { return proc_macro::TokenStream::from(e.write_errors()); }
    };

    let func = syn::parse_macro_input!(input as syn::ItemFn);

    let conn_str_ident_str = args.connection_string.unwrap_or_else(|| "CONN_STR".into());


    let conn_str_ident = proc_macro2::Ident::new(&conn_str_ident_str, proc_macro2::Span::call_site());

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
                let config = tiberius::ClientBuilder::from_ado_string(&#conn_str_ident)?;
                let tcp = async_std::net::TcpStream::connect(config.get_addr()).await?;
                tcp.set_nodelay(true)?;
                let mut client = tiberius::Client::connect(config, tcp).await?;

                #func_name(client).await?;
                Ok(())
            })
        }

        #[test]
        fn #tokio_test()-> Result<()> {
            LOGGER_SETUP.call_once(|| {
                env_logger::init();
            });
            use tokio_util::compat::Tokio02AsyncWriteCompatExt;
            let mut rt = tokio::runtime::Runtime::new()?;
            rt.block_on(async {
                let config = tiberius::ClientBuilder::from_ado_string(&#conn_str_ident)?;
                let tcp = tokio::net::TcpStream::connect(config.get_addr()).await?;
                tcp.set_nodelay(true)?;
                let mut client = tiberius::Client::connect(config, tcp.compat_write()).await?;

                #func_name(client).await?;
                Ok(())
            })
        }
    };

    proc_macro::TokenStream::from(tokens)
}

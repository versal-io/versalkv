use proc_macro::TokenStream;
use quote::quote;
use syn::parse::{Parse, ParseStream};
use syn::{parse_macro_input, ExprPath, ItemFn, LitStr, Token};

#[proc_macro]
pub fn portable_command_support(input: TokenStream) -> TokenStream {
    if input.is_empty() {
        TokenStream::new()
    } else {
        syn::Error::new(
            proc_macro2::Span::call_site(),
            "portable_command_support does not accept arguments",
        )
        .to_compile_error()
        .into()
    }
}

/// ```ignore
/// #[command("REPLICAOF")]
/// pub fn handle(conn, args, ctx) -> Value { ... }
/// #[command("CLIENT")]
/// pub fn handle(conn, args, ctx) -> Value { ... }
/// #[command("CLIENT::LIST")]
/// pub fn handle(conn, args, ctx) -> Value { ... }
/// #[command("SET", get_keys = set_get_keys)]
/// pub fn set(conn, args, ctx) -> Value { ... }
/// // Add behavior owned by the local implementation when the pinned metadata
/// // predates the corresponding upstream command flag.
/// #[command("REPLICAOF", extra_flags = crate::framework::CMD_LOADING)]
/// pub fn replicaof(conn, args, ctx) -> Value { ... }
/// ```
#[proc_macro_attribute]
pub fn command(attr: TokenStream, item: TokenStream) -> TokenStream {
    let CommandAttr {
        cmd_name,
        get_keys,
        hosted,
        runtime,
        extra_flags,
    } = parse_macro_input!(attr as CommandAttr);
    let safe_name = cmd_name.replace("::", "_").replace('-', "_");
    let meta_ident = syn::Ident::new(
        &format!("{}_META", safe_name),
        proc_macro2::Span::call_site(),
    );
    let spec_ident = syn::Ident::new(
        &format!("{}_SPEC", safe_name),
        proc_macro2::Span::call_site(),
    );
    let runtime_spec_ident = syn::Ident::new(
        &format!("{}_RUNTIME_SPEC", safe_name),
        proc_macro2::Span::call_site(),
    );
    let input_fn = parse_macro_input!(item as ItemFn);
    let attrs = &input_fn.attrs;
    let fn_name = &input_fn.sig.ident;
    let vis = &input_fn.vis;
    let sig = &input_fn.sig;
    let block = &input_fn.block;

    let get_keys_expr = match get_keys {
        Some(path) => quote! { Some(#path as crate::framework::GetKeysFn) },
        None => quote! { META.get_keys },
    };
    let executor_expr = match (hosted, runtime) {
        (Some(_), true) => {
            return syn::Error::new_spanned(
                &input_fn.sig.ident,
                "runtime and hosted command executors are mutually exclusive",
            )
            .to_compile_error()
            .into();
        }
        (Some(path), false) => quote! {
            crate::framework::CommandExecutor::Hosted {
                fallback: #fn_name,
                prepare: #path,
            }
        },
        (None, true) => quote! { crate::framework::CommandExecutor::Runtime(#fn_name) },
        (None, false) => quote! { crate::framework::CommandExecutor::Immediate(#fn_name) },
    };
    let flags_expr = match extra_flags {
        Some(path) => quote! { META.flags | #path },
        None => quote! { META.flags },
    };

    let expanded = quote! {
        #[cfg(not(feature = "wasm"))]
        #[allow(non_upper_case_globals)]
        #vis const #spec_ident: crate::framework::CommandSpec = {
            use crate::framework::generated::commands::#meta_ident as META;
            crate::framework::CommandSpec {
                name:             crate::engine::command::CommandName::Static(META.name),
                arity:            META.arity,
                flags:            #flags_expr,
                dispatch_group:   META.dispatch_group,
                first_key:        META.first_key,
                last_key:         META.last_key,
                key_step:         META.key_step,
                container:        META.container,
                key_flags:        META.key_flags,
                acl_categories:   META.acl_categories,
                executor:         #executor_expr,
                get_keys:         #get_keys_expr,
            }
        };

        #(#attrs)*
        #vis #sig #block
    };

    let expanded = if runtime {
        quote! {
            #expanded

            #[cfg(any(feature = "wasm", test))]
            #[allow(non_upper_case_globals)]
            pub(crate) const #runtime_spec_ident:
                crate::framework::commands::command_runtime::RuntimeCommandSpec = {
                use crate::framework::generated::commands::#meta_ident as META;
                crate::framework::commands::command_runtime::RuntimeCommandSpec {
                    meta: &META,
                    handler: #fn_name,
                    get_keys: #get_keys_expr,
                }
            };
        }
    } else {
        expanded
    };

    TokenStream::from(expanded)
}

struct CommandAttr {
    cmd_name: String,
    get_keys: Option<ExprPath>,
    hosted: Option<ExprPath>,
    runtime: bool,
    extra_flags: Option<ExprPath>,
}

impl Parse for CommandAttr {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let name: LitStr = input.parse()?;
        let mut get_keys = None;
        let mut hosted = None;
        let mut runtime = false;
        let mut extra_flags = None;
        while input.parse::<Token![,]>().is_ok() {
            if input.is_empty() {
                break;
            }
            let key: syn::Ident = input.parse()?;
            match key.to_string().as_str() {
                "runtime" => runtime = true,
                "get_keys" => {
                    input.parse::<Token![=]>()?;
                    get_keys = Some(input.parse()?);
                }
                "hosted" => {
                    input.parse::<Token![=]>()?;
                    hosted = Some(input.parse()?);
                }
                "extra_flags" => {
                    input.parse::<Token![=]>()?;
                    extra_flags = Some(input.parse()?);
                }
                other => {
                    return Err(syn::Error::new(
                        key.span(),
                        format!("unknown #[command] argument `{}`", other),
                    ));
                }
            }
        }
        Ok(CommandAttr {
            cmd_name: name.value(),
            get_keys,
            hosted,
            runtime,
            extra_flags,
        })
    }
}

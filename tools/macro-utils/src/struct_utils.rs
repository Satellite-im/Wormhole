use proc_macro::TokenStream;
use quote::{format_ident, quote};
use syn::{
    parse::{Parse, Parser},
    parse_quote,
    punctuated::Punctuated,
    spanned::Spanned,
    Error, ImplItem, ItemImpl, ItemTrait, Meta, Result, TraitItem, Visibility,
};

pub fn expand(attr: TokenStream, input: TokenStream) -> Result<TokenStream> {
    let attributes = Punctuated::<Meta, syn::Token![,]>::parse_terminated.parse(attr)?;
    let mut name = "functions".into();
    let expanded;
    for attr in attributes {
        if attr.path().is_ident("name") {
            if let syn::Meta::NameValue(val) = &attr {
                if let syn::Expr::Lit(v) = &val.value {
                    if let syn::Lit::Str(s) = &v.lit {
                        name = s.value();
                    }
                }
            }
        } else {
            return Err(Error::new(
                attr.span(),
                format!(
                    "Unrecognized attribute {:?}",
                    attr.path()
                        .get_ident()
                        .map(|i| i.to_string())
                        .unwrap_or_default()
                ),
            ));
        }
    }

    if let Ok(mut impls) = ItemImpl::parse.parse(input.clone()) {
        // Parse impl block
        let mut functions = vec![];
        for item in impls.items.iter() {
            if let ImplItem::Fn(function) = item {
                if !function
                    .attrs
                    .iter()
                    .any(|attr| attr.path().is_ident("skip"))
                {
                    let is_async = function.sig.asyncness.is_some();
                    if matches!(function.vis, Visibility::Public(_) | Visibility::Inherited) {
                        let id = function.sig.ident.to_string();
                        functions.push(quote! {
                            (#id, #is_async)
                        });
                    }
                }
            }
        }
        let func = format_ident!("{name}");
        impls.items.push(parse_quote! {
            fn #func() -> &'static[(&'static str, bool)] {
                &[ #(#functions),* ]
            }
        });
        expanded = quote! {
            #impls
        };
    } else {
        // Parse trait block
        let mut trait_impl: ItemTrait = ItemTrait::parse.parse(input)?;
        let mut functions = vec![];
        for item in &trait_impl.items {
            if let TraitItem::Fn(function) = item {
                if !function
                    .attrs
                    .iter()
                    .any(|attr| attr.path().is_ident("skip"))
                {
                    let is_async = function.sig.asyncness.is_some();
                    let id = function.sig.ident.to_string();
                    functions.push(quote! {
                        (#id, #is_async)
                    });
                }
            }
        }
        let func = format_ident!("{name}");
        trait_impl.items.push(parse_quote! {
            fn #func() -> &'static[(&'static str, bool)] {
                &[ #(#functions),* ]
            }
        });
        expanded = quote! {
            #trait_impl
        };
    }
    Ok(TokenStream::from(expanded))
}

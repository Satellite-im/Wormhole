extern crate proc_macro2;

use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, Data, DeriveInput};

/// Implements #values() for enum values that returns all defined values in the enum
/// E.g.
/// ```
/// use enum_macro::EnumValues;
/// #[derive(EnumValues, PartialEq, Debug)]
/// pub enum SomeEnum {
///     SomeValue1,
///     SomeValue2
/// }
///
/// assert_eq!([SomeEnum::SomeValue1, SomeEnum::SomeValue2], SomeEnum::values());
/// ```
#[proc_macro_derive(EnumValues)]
pub fn enum_values(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let name = &input.ident;
    if let Data::Enum(enum_data) = input.data {
        let variants: Vec<_> = enum_data.variants.into_iter().map(|v| v.ident).collect();
        let expanded = quote! {
            impl #name {
                pub fn values() -> &'static[#name] {
                    &[ #(#name::#variants),* ]
                }
            }
        };
        TokenStream::from(expanded)
    } else {
        panic!("Only Enums are supported");
    }
}

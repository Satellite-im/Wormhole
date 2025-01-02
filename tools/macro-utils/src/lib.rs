extern crate proc_macro2;

use proc_macro::TokenStream;

mod struct_utils;

/// Implements a way to fetch all public implemented method for a given struct or trait.
/// It returns a tuple where the first element is the functions name and the second if its async
/// E.g.
/// ```
/// use macro_utils::impl_funcs;
///
/// pub struct A;
///
/// /// For structs
/// #[impl_funcs(name="get_all_impls")] /// Optional name parameter. If not present will be called functions
/// impl A {
///     fn some_method_a() {}
///
///     fn some_method_b() {}
///
///     async fn some_async_method_c() {}
/// }
/// assert_eq!([("some_method_a", false), ("some_method_b", false), ("some_async_method_c", true)], A::get_all_impls());
///
///
/// /// For traits
/// #[impl_funcs(name="get_all_impls")] /// Optional name parameter. If not present will be called functions
/// trait SomeTrait {
///     fn some_method_a();
///
///     fn some_method_b();
///
///     async fn some_async_method_c();
/// }
///
/// pub struct B;
/// impl SomeTrait for B {
///     fn some_method_a() {}
///
///     fn some_method_b() {}
///
///     async fn some_async_method_c() {}
/// }
///
/// assert_eq!([("some_method_a", false), ("some_method_b", false), ("some_async_method_c", true)], B::get_all_impls());
/// ```
#[proc_macro_attribute]
pub fn impl_funcs(attr: TokenStream, input: TokenStream) -> TokenStream {
    struct_utils::expand(attr, input).unwrap_or_else(|e| e.into_compile_error().into())
}

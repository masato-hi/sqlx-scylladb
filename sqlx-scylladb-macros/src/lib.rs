use proc_macro::TokenStream;
use sqlx_scylladb_macros_core::expand_user_defined_type;
use syn::{DeriveInput, parse_macro_input};

#[proc_macro_derive(UserDefinedType, attributes(user_defined_type))]
pub fn user_defined_type(input: TokenStream) -> TokenStream {
    let item = parse_macro_input!(input as DeriveInput);

    match expand_user_defined_type(item) {
        Ok(ts) => ts.into(),
        Err(e) => e.to_compile_error().into(),
    }
}

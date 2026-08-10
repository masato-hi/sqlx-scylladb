#![warn(missing_docs)]
#![doc = include_str!("lib.md")]

use proc_macro::TokenStream;
use sqlx_scylladb_macros_core::derives::{expand_from_row, expand_user_defined_type};
use syn::{DeriveInput, parse_macro_input};

/// Implement `sqlx::FromRow`, with `#[sqlx(default_when_null)]` support.
#[cfg(feature = "derive")]
#[proc_macro_derive(FromRow, attributes(sqlx))]
pub fn from_row(input: TokenStream) -> TokenStream {
    let item = parse_macro_input!(input as DeriveInput);

    match expand_from_row(item) {
        Ok(ts) => ts.into(),
        Err(e) => e.to_compile_error().into(),
    }
}

/// Implement a user-defined type in sqlx-scylladb that supports binding and fetching.
///
/// # Examples
///
/// ```no_run,ignore
/// use sqlx_scylladb::macros::UserDefinedType;
///
/// // By default, the names of user-defined types use snake-case for struct names.
/// #[derive(UserDefinedType)]
/// struct MyUserDefinedType{
///     my_id: i64,
///     my_name: String,
/// }
///
/// // If using a different name, declare the name.
/// #[derive(UserDefinedType)]
/// #[user_defined_type(name = "my_user_defined_type")]
/// struct OtherNamedMyUserDefinedType{
///     my_id: i64,
///     my_name: String,
/// }
/// ```
#[cfg(feature = "derive")]
#[proc_macro_derive(UserDefinedType, attributes(user_defined_type))]
pub fn user_defined_type(input: TokenStream) -> TokenStream {
    let item = parse_macro_input!(input as DeriveInput);

    match expand_user_defined_type(item) {
        Ok(ts) => ts.into(),
        Err(e) => e.to_compile_error().into(),
    }
}

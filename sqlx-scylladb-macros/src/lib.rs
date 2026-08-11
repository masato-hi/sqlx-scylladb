#![warn(missing_docs)]
#![doc = include_str!("lib.md")]

use proc_macro::TokenStream;
use sqlx_scylladb_macros_core::derives::{expand_from_row, expand_user_defined_type};
use syn::{DeriveInput, parse_macro_input};

/// Derive `sqlx::FromRow` for a struct, with support for the
/// `#[sqlx(default_when_null)]` field attribute.
#[cfg(feature = "derive")]
#[proc_macro_derive(FromRow, attributes(sqlx))]
pub fn from_row(input: TokenStream) -> TokenStream {
    let item = parse_macro_input!(input as DeriveInput);

    match expand_from_row(item) {
        Ok(ts) => ts.into(),
        Err(e) => e.to_compile_error().into(),
    }
}

/// Derive support for binding and fetching a ScyllaDB user-defined type.
///
/// By default, the ScyllaDB type name is derived from the Rust struct name by
/// converting it to snake case. Use `#[user_defined_type(name = "...")]` to
/// specify a different type name.
///
/// # Examples
///
/// ```no_run,ignore
/// use sqlx_scylladb::macros::UserDefinedType;
///
/// // The type name is derived from the struct name as `my_user_defined_type`.
/// #[derive(UserDefinedType)]
/// struct MyUserDefinedType{
///     my_id: i64,
///     my_name: String,
/// }
///
/// // Specify a different ScyllaDB type name explicitly.
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

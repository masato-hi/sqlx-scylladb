use convert_case::{Case, Casing};
use darling::FromDeriveInput;
use proc_macro2::TokenStream;
use quote::quote;
use syn::DeriveInput;

#[derive(Debug, FromDeriveInput)]
#[darling(attributes(user_defined_type))]
struct UserDefinedTypeOption {
    name: Option<String>,
}

#[doc(hidden)]
pub fn expand_user_defined_type(item: DeriveInput) -> syn::Result<TokenStream> {
    let option = UserDefinedTypeOption::from_derive_input(&item).unwrap();

    let struct_ident = item.ident;
    let type_name = if let Some(name) = &option.name {
        name.clone()
    } else {
        struct_ident.to_string().to_case(Case::Snake)
    };
    let array_type_name = format!("{}[]", type_name);

    let tokens = quote! {
        #[automatically_derived]
        impl<'r> ::sqlx_scylladb::UserDefinedType<'r> for #struct_ident {}

        impl<'r> ::sqlx_scylladb::ScyllaDBHasArrayType for #struct_ident
        where
            Self: ::sqlx_scylladb::UserDefinedType<'r>,
        {
            fn array_type_info() -> ::sqlx_scylladb::ScyllaDBTypeInfo {
                let ty = ::sqlx_scylladb::ext::ustr::UStr::new(#array_type_name);
                ::sqlx_scylladb::ScyllaDBTypeInfo::UserDefinedTypeArray(ty)
            }
        }

        #[automatically_derived]
        impl ::sqlx_scylladb::ext::sqlx::types::Type<::sqlx_scylladb::ScyllaDB> for #struct_ident {
            fn type_info() -> ::sqlx_scylladb::ScyllaDBTypeInfo {
                let ty = ::sqlx_scylladb::ext::ustr::UStr::new(#type_name);
                ::sqlx_scylladb::ScyllaDBTypeInfo::UserDefinedType(ty)
            }
        }

        #[automatically_derived]
        impl<'r> ::sqlx_scylladb::ext::sqlx::encode::Encode<'_, ::sqlx_scylladb::ScyllaDB> for #struct_ident
        where Self: ::sqlx_scylladb::UserDefinedType<'r> + ::std::clone::Clone {
            fn encode(self, buf: &mut ::sqlx_scylladb::ScyllaDBArgumentBuffer) -> Result<::sqlx_scylladb::ext::sqlx::encode::IsNull, ::sqlx_scylladb::ext::sqlx::error::BoxDynError> {
                let argument = ::sqlx_scylladb::ScyllaDBArgument::UserDefinedType(::std::boxed::Box::new(self));
                buf.push(argument);

                Ok(::sqlx_scylladb::ext::sqlx::encode::IsNull::No)
            }

            fn encode_by_ref(&self, buf: &mut ::sqlx_scylladb::ScyllaDBArgumentBuffer) -> Result<::sqlx_scylladb::ext::sqlx::encode::IsNull, ::sqlx_scylladb::ext::sqlx::error::BoxDynError> {
                use ::sqlx_scylladb::UserDefinedType as _;

                let argument = ::sqlx_scylladb::ScyllaDBArgument::UserDefinedType(::std::boxed::Box::new(self.clone()));
                buf.push(argument);

                Ok(::sqlx_scylladb::ext::sqlx::encode::IsNull::No)
            }
        }

        #[automatically_derived]
        impl ::sqlx_scylladb::ext::sqlx::decode::Decode<'_, ::sqlx_scylladb::ScyllaDB> for #struct_ident {
            fn decode(value: ::sqlx_scylladb::ScyllaDBValueRef<'_>) -> Result<Self, ::sqlx_scylladb::ext::sqlx::error::BoxDynError> {
                let value: Self = value.deserialize()?;

                Ok(value)
            }
        }
    };

    Ok(tokens.into())
}

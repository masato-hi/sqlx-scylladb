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

    let tokens = quote! {
        #[automatically_derived]
        impl<'r> ::sqlx_scylladb::UserDefinedType<'r> for #struct_ident {
            fn type_name() -> ::sqlx_scylladb::ext::ustr::UStr {
                ::sqlx_scylladb::ext::ustr::UStr::new(#type_name)
            }
        }

        #[automatically_derived]
        impl ::sqlx_scylladb::ext::sqlx::Type<::sqlx_scylladb::ScyllaDB> for #struct_ident {
            fn type_info() -> ::sqlx_scylladb::ScyllaDBTypeInfo {
                use ::sqlx_scylladb::UserDefinedType as _;

                let type_name = #struct_ident::type_name();
                ::sqlx_scylladb::ScyllaDBTypeInfo::UserDefinedType(type_name)
            }
        }

        #[automatically_derived]
        impl<'r> ::sqlx_scylladb::ext::sqlx::Encode<'_, ::sqlx_scylladb::ScyllaDB> for #struct_ident
        where Self: ::sqlx_scylladb::UserDefinedType<'r> {
            fn encode_by_ref(&self, buf: &mut ::sqlx_scylladb::ScyllaDBArgumentBuffer) -> Result<::sqlx_scylladb::ext::sqlx::IsNull, ::sqlx_scylladb::ext::sqlx::BoxDynError> {
                use ::sqlx_scylladb::UserDefinedType as _;

                let argument = ::sqlx_scylladb::ScyllaDBArgument::UserDefinedType(self.box_cloned());
                buf.push(argument);

                Ok(::sqlx_scylladb::ext::sqlx::IsNull::No)
            }
        }

        #[automatically_derived]
        impl ::sqlx_scylladb::ext::sqlx::Decode<'_, ::sqlx_scylladb::ScyllaDB> for #struct_ident {
            fn decode(value: ::sqlx_scylladb::ScyllaDBValueRef<'_>) -> Result<Self, ::sqlx_scylladb::ext::sqlx::BoxDynError> {
                let value: Self = value.deserialize()?;

                Ok(value)
            }
        }
    };

    Ok(tokens.into())
}

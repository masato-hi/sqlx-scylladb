use convert_case::{Case, Casing};
use darling::FromDeriveInput;
use proc_macro2::TokenStream;
use quote::quote;
use syn::{DeriveInput, Ident};

#[derive(Debug, FromDeriveInput)]
#[darling(attributes(user_defined_type))]
struct UserDefinedTypeOption {
    name: Option<String>,
}

#[doc(hidden)]
pub fn expand_user_defined_type(item: DeriveInput) -> syn::Result<TokenStream> {
    let option = UserDefinedTypeOption::from_derive_input(&item).unwrap();

    let struct_ident = item.ident;
    let vec_ident = syn::parse_str::<Ident>(format!("{struct_ident}Vec").as_str())?;
    let type_name = if let Some(name) = &option.name {
        name.clone()
    } else {
        struct_ident.to_string().to_case(Case::Snake)
    };
    let vec_type_name = format!("{type_name}[]");

    let tokens = quote! {
        #[automatically_derived]
        impl ::sqlx_scylladb::ext::sqlx::Type<::sqlx_scylladb::ScyllaDB> for #struct_ident {
            fn type_info() -> ::sqlx_scylladb::ScyllaDBTypeInfo {
                let type_name = ::sqlx_scylladb::ext::ustr::UStr::new(#type_name);
                ::sqlx_scylladb::ScyllaDBTypeInfo::UserDefinedType(type_name)
            }
        }

        #[automatically_derived]
        impl ::sqlx_scylladb::ext::sqlx::Encode<'_, ::sqlx_scylladb::ScyllaDB> for #struct_ident {
            fn encode(self, buf: &mut ::sqlx_scylladb::ScyllaDBArgumentBuffer) -> Result<::sqlx_scylladb::ext::sqlx::IsNull, ::sqlx_scylladb::ext::sqlx::BoxDynError> {
                let argument = ::sqlx_scylladb::ScyllaDBArgument::UserDefinedType(::std::sync::Arc::new(self));
                buf.push(argument);

                Ok(::sqlx_scylladb::ext::sqlx::IsNull::No)
            }

            fn encode_by_ref(&self, buf: &mut ::sqlx_scylladb::ScyllaDBArgumentBuffer) -> Result<::sqlx_scylladb::ext::sqlx::IsNull, ::sqlx_scylladb::ext::sqlx::BoxDynError> {
                let argument = ::sqlx_scylladb::ScyllaDBArgument::UserDefinedType(::std::sync::Arc::new(self.clone()));
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

        #[derive(Debug, Clone, Default)]
        pub(crate) struct #vec_ident(Vec<#struct_ident>);

        impl #vec_ident {
            pub fn new() -> Self{
                Self(Vec::new())
            }

            pub fn with_capacity(cap: usize) -> Self {
                Self(Vec::with_capacity(cap))
            }
        }

        impl From<Vec<#struct_ident>> for #vec_ident {
            fn from(value: Vec<#struct_ident>) -> #vec_ident {
                Self(value)
            }
        }

        impl Into<Vec<#struct_ident>> for #vec_ident {
            fn into(self) -> Vec<#struct_ident> {
                self.0
            }
        }

        impl ::std::ops::Deref for #vec_ident {
            type Target = Vec<#struct_ident>;

            fn deref(&self) -> &Self::Target {
                &self.0
            }
        }

        impl ::std::ops::DerefMut for #vec_ident {
            fn deref_mut(&mut self) -> &mut Self::Target {
                &mut self.0
            }
        }

        #[automatically_derived]
        impl ::sqlx_scylladb::ext::scylla::serialize::value::SerializeValue for #vec_ident {
            fn serialize<'b>(
                &self,
                typ: &::sqlx_scylladb::ext::scylla::cluster::metadata::ColumnType,
                writer: ::sqlx_scylladb::ext::scylla::serialize::writers::CellWriter<'b>,
            ) -> Result<::sqlx_scylladb::ext::scylla::serialize::writers::WrittenCellProof<'b>, ::sqlx_scylladb::ext::scylla::errors::SerializationError> {
                <_ as ::sqlx_scylladb::ext::scylla::serialize::value::SerializeValue>::serialize(&self.0, typ, writer)
            }
        }

        #[automatically_derived]
        impl ::sqlx_scylladb::ext::sqlx::Type<::sqlx_scylladb::ScyllaDB> for #vec_ident {
            fn type_info() -> ::sqlx_scylladb::ScyllaDBTypeInfo {
                let type_name = ::sqlx_scylladb::ext::ustr::UStr::new(#vec_type_name);
                ::sqlx_scylladb::ScyllaDBTypeInfo::UserDefinedTypeArray(type_name)
            }
        }

        #[automatically_derived]
        impl ::sqlx_scylladb::ext::sqlx::Encode<'_, ::sqlx_scylladb::ScyllaDB> for #vec_ident {
            fn encode(self, buf: &mut ::sqlx_scylladb::ScyllaDBArgumentBuffer) -> Result<::sqlx_scylladb::ext::sqlx::IsNull, ::sqlx_scylladb::ext::sqlx::BoxDynError> {
                let argument = ::sqlx_scylladb::ScyllaDBArgument::UserDefinedTypeArray(::std::sync::Arc::new(self));
                buf.push(argument);

                Ok(::sqlx_scylladb::ext::sqlx::IsNull::No)
            }

            fn encode_by_ref(
                &self,
                buf: &mut ::sqlx_scylladb::ScyllaDBArgumentBuffer,
            ) -> Result<::sqlx_scylladb::ext::sqlx::IsNull, ::sqlx_scylladb::ext::sqlx::BoxDynError> {
                let argument = ::sqlx_scylladb::ScyllaDBArgument::UserDefinedTypeArray(::std::sync::Arc::new(self.clone()));
                buf.push(argument);

                Ok(::sqlx_scylladb::ext::sqlx::IsNull::No)
            }
        }

        #[automatically_derived]
        impl ::sqlx_scylladb::ext::sqlx::Decode<'_, ::sqlx_scylladb::ScyllaDB> for #vec_ident {
            fn decode(
                value: ::sqlx_scylladb::ScyllaDBValueRef<'_>,
            ) -> Result<Self, ::sqlx_scylladb::ext::sqlx::BoxDynError> {
                let val: Vec<#struct_ident> = value.deserialize()?;
                Ok(Self(val))
            }
        }
    };

    Ok(tokens.into())
}

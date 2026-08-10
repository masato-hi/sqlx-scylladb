use proc_macro2::{Span, TokenStream};
use quote::quote;
use syn::{
    Attribute, Data, DataStruct, DeriveInput, Expr, Field, Fields, FieldsNamed, FieldsUnnamed,
    Lifetime, Type, parse_quote, punctuated::Punctuated, token::Comma,
};

#[derive(Default)]
struct ContainerAttributes {
    default: bool,
    rename_all: Option<String>,
}

#[derive(Default)]
struct FieldAttributes {
    rename: Option<String>,
    default: bool,
    default_when_null: bool,
    flatten: bool,
    try_from: Option<Type>,
    skip: bool,
    json: Option<JsonAttribute>,
}

#[derive(Clone, Copy)]
enum JsonAttribute {
    NonNullable,
    Nullable,
}

fn parse_container_attributes(attrs: &[Attribute]) -> syn::Result<ContainerAttributes> {
    let mut result = ContainerAttributes::default();
    for attr in attrs.iter().filter(|attr| attr.path().is_ident("sqlx")) {
        attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("default") {
                result.default = true;
            } else if meta.path.is_ident("rename_all") {
                meta.input.parse::<syn::Token![=]>()?;
                result.rename_all = Some(meta.input.parse::<syn::LitStr>()?.value());
            }
            Ok(())
        })?;
    }
    Ok(result)
}

fn parse_field_attributes(attrs: &[Attribute]) -> syn::Result<FieldAttributes> {
    let mut result = FieldAttributes::default();
    for attr in attrs.iter().filter(|attr| attr.path().is_ident("sqlx")) {
        attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("rename") {
                meta.input.parse::<syn::Token![=]>()?;
                result.rename = Some(meta.input.parse::<syn::LitStr>()?.value());
            } else if meta.path.is_ident("default") {
                result.default = true;
            } else if meta.path.is_ident("default_when_null") {
                result.default_when_null = true;
            } else if meta.path.is_ident("flatten") {
                result.flatten = true;
            } else if meta.path.is_ident("skip") {
                result.skip = true;
            } else if meta.path.is_ident("try_from") {
                meta.input.parse::<syn::Token![=]>()?;
                result.try_from = Some(meta.input.parse::<syn::LitStr>()?.parse()?);
            } else if meta.path.is_ident("json") {
                result.json = Some(if meta.input.peek(syn::token::Paren) {
                    let content;
                    syn::parenthesized!(content in meta.input);
                    let value: syn::Ident = content.parse()?;
                    if value != "nullable" {
                        return Err(syn::Error::new_spanned(value, "expected `nullable`"));
                    }
                    JsonAttribute::Nullable
                } else {
                    JsonAttribute::NonNullable
                });
            }
            Ok(())
        })?;
        if result.flatten && result.json.is_some() {
            return Err(syn::Error::new_spanned(
                attr,
                "Cannot use `json` and `flatten` together on the same field",
            ));
        }
    }
    Ok(result)
}

fn rename_all(name: &str, pattern: &str) -> String {
    use convert_case::{Case, Casing};
    match pattern {
        "lowercase" => name.to_lowercase(),
        "UPPERCASE" => name.to_uppercase(),
        "SCREAMING_SNAKE_CASE" => name.to_case(Case::Constant),
        "kebab-case" => name.to_case(Case::Kebab),
        "camelCase" => name.to_case(Case::Camel),
        "PascalCase" => name.to_case(Case::Pascal),
        _ => name.to_case(Case::Snake),
    }
}

fn field_expr(
    field_type: &Type,
    column: &str,
    attrs: &FieldAttributes,
    predicates: &mut Punctuated<syn::WherePredicate, Comma>,
    lifetime: &Lifetime,
) -> syn::Result<Expr> {
    if attrs.flatten && attrs.default_when_null {
        return Err(syn::Error::new(
            Span::call_site(),
            "`default_when_null` cannot be used with `flatten`",
        ));
    }
    let expr = match (attrs.flatten, attrs.try_from.as_ref(), attrs.json) {
        (true, None, None) => {
            predicates.push(parse_quote!(#field_type: ::sqlx::FromRow<#lifetime, R>));
            parse_quote!(<#field_type as ::sqlx::FromRow<#lifetime, R>>::from_row(__row))
        }
        (true, Some(source), None) => {
            predicates.push(parse_quote!(#source: ::sqlx::FromRow<#lifetime, R>));
            parse_quote!(<#source as ::sqlx::FromRow<#lifetime, R>>::from_row(__row).and_then(|value| {
                <#field_type as ::std::convert::TryFrom<#source>>::try_from(value).map_err(|error| ::sqlx::Error::ColumnDecode {
                    index: #column.to_string(), source: ::sqlx::__spec_error!(error),
                })
            }))
        }
        (true, _, Some(_)) => {
            return Err(syn::Error::new(
                Span::call_site(),
                "Cannot use both `flatten` and `json`",
            ));
        }
        (false, Some(_), Some(_)) => {
            return Err(syn::Error::new(
                Span::call_site(),
                "Cannot use both `try_from` and `json`",
            ));
        }
        (false, Some(source), None) => {
            predicates.push(parse_quote!(#source: ::sqlx::decode::Decode<#lifetime, R::Database>));
            predicates.push(parse_quote!(#source: ::sqlx::types::Type<R::Database>));
            if attrs.default_when_null {
                predicates.push(parse_quote!(::core::option::Option<#source>: ::sqlx::decode::Decode<#lifetime, R::Database>));
                predicates.push(
                    parse_quote!(::core::option::Option<#source>: ::sqlx::types::Type<R::Database>),
                );
                parse_quote!(__row.try_get::<::core::option::Option<#source>, _>(#column).and_then(|value| {
                    value.map(|value| <#field_type as ::std::convert::TryFrom<#source>>::try_from(value)
                        .map_err(|error| ::sqlx::Error::ColumnDecode { index: #column.to_string(), source: ::sqlx::__spec_error!(error) }))
                        .transpose()
                }))
            } else {
                parse_quote!(__row.try_get::<#source, _>(#column).and_then(|value| {
                    <#field_type as ::std::convert::TryFrom<#source>>::try_from(value).map_err(|error| ::sqlx::Error::ColumnDecode { index: #column.to_string(), source: ::sqlx::__spec_error!(error) })
                }))
            }
        }
        (false, None, Some(JsonAttribute::NonNullable)) => {
            predicates.push(parse_quote!(::sqlx::types::Json<#field_type>: ::sqlx::decode::Decode<#lifetime, R::Database>));
            predicates.push(
                parse_quote!(::sqlx::types::Json<#field_type>: ::sqlx::types::Type<R::Database>),
            );
            if attrs.default_when_null {
                parse_quote!(__row.try_get::<::core::option::Option<::sqlx::types::Json<#field_type>>, _>(#column).map(|value| value.map(|value| value.0)))
            } else {
                parse_quote!(__row.try_get::<::sqlx::types::Json<#field_type>, _>(#column).map(|value| value.0))
            }
        }
        (false, None, Some(JsonAttribute::Nullable)) => {
            let json_type: Type =
                parse_quote!(::core::option::Option<::sqlx::types::Json<#field_type>>);
            predicates
                .push(parse_quote!(#json_type: ::sqlx::decode::Decode<#lifetime, R::Database>));
            predicates.push(parse_quote!(#json_type: ::sqlx::types::Type<R::Database>));
            parse_quote!(__row.try_get::<#json_type, _>(#column).map(|value| value.and_then(|value| value.0)))
        }
        (false, None, None) => {
            if attrs.default_when_null {
                predicates.push(parse_quote!(::core::option::Option<#field_type>: ::sqlx::decode::Decode<#lifetime, R::Database>));
                predicates.push(parse_quote!(::core::option::Option<#field_type>: ::sqlx::types::Type<R::Database>));
                parse_quote!(__row.try_get::<::core::option::Option<#field_type>, _>(#column))
            } else {
                predicates.push(
                    parse_quote!(#field_type: ::sqlx::decode::Decode<#lifetime, R::Database>),
                );
                predicates.push(parse_quote!(#field_type: ::sqlx::types::Type<R::Database>));
                parse_quote!(__row.try_get(#column))
            }
        }
    };
    Ok(expr)
}

fn expand_named(
    input: &DeriveInput,
    fields: &Punctuated<Field, Comma>,
) -> syn::Result<TokenStream> {
    let ident = &input.ident;
    let (lifetime, provided) = input
        .generics
        .lifetimes()
        .next()
        .map(|def| (def.lifetime.clone(), false))
        .unwrap_or_else(|| (Lifetime::new("'a", Span::call_site()), true));
    let (_, ty_generics, _) = input.generics.split_for_impl();
    let mut generics = input.generics.clone();
    generics.params.insert(0, parse_quote!(R: ::sqlx::Row));
    if provided {
        generics.params.insert(0, parse_quote!(#lifetime));
    }
    let predicates = &mut generics.make_where_clause().predicates;
    predicates.push(parse_quote!(&#lifetime ::std::primitive::str: ::sqlx::ColumnIndex<R>));
    let container = parse_container_attributes(&input.attrs)?;
    let default_instance = if container.default {
        predicates.push(parse_quote!(#ident: ::std::default::Default));
        Some(quote!(let __default = #ident::default();))
    } else {
        None
    };
    let mut reads = Vec::new();
    for field in fields {
        let id = field.ident.as_ref().unwrap();
        let ty = &field.ty;
        let attrs = parse_field_attributes(&field.attrs)?;
        if attrs.skip {
            reads.push(quote!(let #id: #ty = ::std::default::Default::default();));
            continue;
        }
        let name = attrs.rename.clone().unwrap_or_else(|| {
            let name = id.to_string().trim_start_matches("r#").to_owned();
            container
                .rename_all
                .as_deref()
                .map(|p| rename_all(&name, p))
                .unwrap_or(name)
        });
        let mut expr = field_expr(ty, &name, &attrs, predicates, &lifetime)?;
        if attrs.default_when_null {
            expr = parse_quote!(#expr.map(|value| value.unwrap_or_default()));
            predicates.push(parse_quote!(#ty: ::std::default::Default));
        }
        if attrs.default {
            expr = parse_quote!(#expr.or_else(|error| match error { ::sqlx::Error::ColumnNotFound(_) => ::std::result::Result::Ok(::std::default::Default::default()), error => ::std::result::Result::Err(error) }));
            predicates.push(parse_quote!(#ty: ::std::default::Default));
        } else if container.default {
            expr = parse_quote!(#expr.or_else(|error| match error { ::sqlx::Error::ColumnNotFound(_) => ::std::result::Result::Ok(__default.#id), error => ::std::result::Result::Err(error) }));
        }
        reads.push(quote!(let #id: #ty = #expr?;));
    }
    let (impl_generics, _, where_clause) = generics.split_for_impl();
    let names = fields.iter().map(|field| field.ident.as_ref().unwrap());
    Ok(
        quote! { #[automatically_derived] impl #impl_generics ::sqlx::FromRow<#lifetime, R> for #ident #ty_generics #where_clause { fn from_row(__row: &#lifetime R) -> ::sqlx::Result<Self> { #default_instance #(#reads)* ::std::result::Result::Ok(#ident { #(#names),* }) } } },
    )
}

fn expand_unnamed(
    input: &DeriveInput,
    fields: &Punctuated<Field, Comma>,
) -> syn::Result<TokenStream> {
    let ident = &input.ident;
    let (lifetime, provided) = input
        .generics
        .lifetimes()
        .next()
        .map(|def| (def.lifetime.clone(), false))
        .unwrap_or_else(|| (Lifetime::new("'a", Span::call_site()), true));
    let (_, ty_generics, _) = input.generics.split_for_impl();
    let mut generics = input.generics.clone();
    generics.params.insert(0, parse_quote!(R: ::sqlx::Row));
    if provided {
        generics.params.insert(0, parse_quote!(#lifetime));
    }
    let predicates = &mut generics.make_where_clause().predicates;
    predicates.push(parse_quote!(::std::primitive::usize: ::sqlx::ColumnIndex<R>));
    for field in fields {
        let ty = &field.ty;
        predicates.push(parse_quote!(#ty: ::sqlx::decode::Decode<#lifetime, R::Database>));
        predicates.push(parse_quote!(#ty: ::sqlx::types::Type<R::Database>));
    }
    let (impl_generics, _, where_clause) = generics.split_for_impl();
    let gets = fields
        .iter()
        .enumerate()
        .map(|(idx, _)| quote!(__row.try_get(#idx)?));
    Ok(
        quote! { #[automatically_derived] impl #impl_generics ::sqlx::FromRow<#lifetime, R> for #ident #ty_generics #where_clause { fn from_row(__row: &#lifetime R) -> ::sqlx::Result<Self> { ::std::result::Result::Ok(#ident(#(#gets),*)) } } },
    )
}

pub fn expand_from_row(input: DeriveInput) -> syn::Result<TokenStream> {
    match &input.data {
        Data::Struct(DataStruct {
            fields: Fields::Named(FieldsNamed { named, .. }),
            ..
        }) => expand_named(&input, named),
        Data::Struct(DataStruct {
            fields: Fields::Unnamed(FieldsUnnamed { unnamed, .. }),
            ..
        }) => expand_unnamed(&input, unnamed),
        Data::Struct(DataStruct {
            fields: Fields::Unit,
            ..
        }) => Err(syn::Error::new_spanned(
            input,
            "unit structs are not supported",
        )),
        Data::Enum(_) => Err(syn::Error::new_spanned(input, "enums are not supported")),
        Data::Union(_) => Err(syn::Error::new_spanned(input, "unions are not supported")),
    }
}

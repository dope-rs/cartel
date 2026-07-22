#![allow(clippy::too_many_arguments)]

extern crate proc_macro;

use backend::Backend;
use proc_macro::TokenStream;
use syn::Token;
use syn::punctuated::Punctuated;
use syn::{Data, DeriveInput, Fields, ItemImpl, parse_macro_input, parse_quote};

mod backend;
mod build;
mod derive_table;
mod emit;
mod parse;
mod pg;
mod row_meta;
mod shape;
mod sqlite;
mod util;
mod where_clause;

#[proc_macro_attribute]
pub fn dispatcher(attr: TokenStream, item: TokenStream) -> TokenStream {
    let public_constructor = if attr.is_empty() {
        false
    } else {
        let option = parse_macro_input!(attr as syn::Ident);
        if option != "new" {
            return syn::Error::new_spanned(option, "expected `new`")
                .to_compile_error()
                .into();
        }
        true
    };

    let mut input = parse_macro_input!(item as DeriveInput);
    if let Err(error) = reject_packed(&input.attrs) {
        return error.to_compile_error().into();
    }
    let name = input.ident.clone();
    let vis = input.vis.clone();
    let generics = input.generics.clone();
    let fields = match &mut input.data {
        Data::Struct(data) => match &mut data.fields {
            Fields::Named(fields) => &mut fields.named,
            _ => {
                return syn::Error::new_spanned(
                    &input.ident,
                    "#[dispatcher] requires named fields",
                )
                .to_compile_error()
                .into();
            }
        },
        _ => {
            return syn::Error::new_spanned(&input.ident, "#[dispatcher] requires a struct")
                .to_compile_error()
                .into();
        }
    };
    let constructor_fields = fields
        .iter()
        .map(|field| {
            (
                field.ident.clone().expect("named field"),
                field.ty.clone(),
                field
                    .attrs
                    .iter()
                    .filter(|attr| attr.path().is_ident("cfg"))
                    .cloned()
                    .collect::<Vec<_>>(),
            )
        })
        .collect::<Vec<_>>();
    let has_manifold_fields = fields.iter().any(|field| {
        field
            .attrs
            .iter()
            .any(|attr| attr.path().is_ident("manifold"))
    });
    for field in fields.iter_mut() {
        if !has_manifold_fields {
            field.attrs.push(parse_quote!(#[manifold]));
        }
        if field
            .attrs
            .iter()
            .any(|attr| attr.path().is_ident("manifold"))
        {
            field.attrs.push(parse_quote!(#[pin]));
        }
    }
    let brand = generics.lifetimes().next().map(|param| {
        let lifetime = &param.lifetime;
        let mut field_name = "__cartel_dispatcher_brand".to_owned();
        while fields.iter().any(|field| {
            field
                .ident
                .as_ref()
                .is_some_and(|ident| ident == field_name.as_str())
        }) {
            field_name.push('_');
        }
        let field_name = syn::Ident::new(&field_name, proc_macro2::Span::call_site());
        fields.push(parse_quote! {
            #field_name: ::core::marker::PhantomData<&#lifetime ()>
        });
        quote::quote! { #field_name: ::core::marker::PhantomData, }
    });
    let constructor_args = constructor_fields
        .iter()
        .map(|(name, ty, attrs)| quote::quote!(#(#attrs)* #name: #ty))
        .collect::<Vec<_>>();
    let field_initializers = constructor_fields
        .iter()
        .map(|(name, _, attrs)| quote::quote!(#(#attrs)* #name,))
        .collect::<Vec<_>>();
    let constructor_names = constructor_fields
        .iter()
        .map(|(name, _, attrs)| quote::quote!(#(#attrs)* #name))
        .collect::<Vec<_>>();
    let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();
    let new = public_constructor.then(|| {
        quote::quote! {
            #[inline(always)]
            #vis fn new(#(#constructor_args),*) -> Self {
                Self::__cartel_dispatcher_new(#(#constructor_names),*)
            }
        }
    });

    quote::quote! {
        #[::cartel_core::__private::pin_project]
        #[derive(::cartel_core::__private::Dispatcher)]
        #input

        impl #impl_generics #name #ty_generics #where_clause {
            #[doc(hidden)]
            #[inline(always)]
            fn __cartel_dispatcher_new(#(#constructor_args),*) -> Self {
                Self {
                    #(#field_initializers)*
                    #brand
                }
            }

            #new
        }
    }
    .into()
}

fn reject_packed(attrs: &[syn::Attribute]) -> syn::Result<()> {
    for attr in attrs {
        if !attr.path().is_ident("repr") {
            continue;
        }
        let reprs = attr.parse_args_with(Punctuated::<syn::Meta, Token![,]>::parse_terminated)?;
        if let Some(repr) = reprs.iter().find(|repr| match repr {
            syn::Meta::Path(path) => path.is_ident("packed"),
            syn::Meta::List(list) => list.path.is_ident("packed"),
            syn::Meta::NameValue(value) => value.path.is_ident("packed"),
        }) {
            return Err(syn::Error::new_spanned(
                repr,
                "pinned projection does not support repr(packed)",
            ));
        }
    }
    Ok(())
}

#[proc_macro_derive(PgTable, attributes(pk, table_name))]
pub fn pg_derive_table(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    match pg::PgBackend::derive_table(&input) {
        Ok(ts) => ts.into(),
        Err(e) => e.to_compile_error().into(),
    }
}

#[proc_macro_attribute]
pub fn query_group(_attr: TokenStream, item: TokenStream) -> TokenStream {
    let block = parse_macro_input!(item as ItemImpl);
    match pg::PgBackend::expand_query_group(block) {
        Ok(ts) => ts.into(),
        Err(e) => e.to_compile_error().into(),
    }
}

#[proc_macro]
pub fn pg_instance(input: TokenStream) -> TokenStream {
    match pg::PgBackend::expand_instance(input) {
        Ok(ts) => ts.into(),
        Err(e) => e.to_compile_error().into(),
    }
}

#[proc_macro_derive(SqliteTable, attributes(pk, table_name))]
pub fn sqlite_derive_table(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    match sqlite::SqliteBackend::derive_table(&input) {
        Ok(ts) => ts.into(),
        Err(e) => e.to_compile_error().into(),
    }
}

#[proc_macro_attribute]
pub fn sqlite_query(attr: TokenStream, item: TokenStream) -> TokenStream {
    let f = parse_macro_input!(item as syn::ItemFn);
    let no_probe = attr.to_string().contains("no_probe");
    match sqlite::SqliteBackend::query_free(f, no_probe) {
        Ok(ts) => ts.into(),
        Err(e) => e.to_compile_error().into(),
    }
}

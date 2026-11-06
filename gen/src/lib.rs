extern crate proc_macro;

use backend::TableBackend as _;
use dialect::pg::{group, row};
use dialect::{pg, sqlite};

mod backend;
mod dialect;
mod emit;
mod ext;
mod parse;
mod plan;
mod pred;
mod shape;

#[proc_macro_derive(PgTable, attributes(table_name, table))]
pub fn pg_derive_table(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let input = syn::parse_macro_input!(input as syn::DeriveInput);
    match pg::PgBackend::derive_table(&input) {
        Ok(ts) => ts.into(),
        Err(e) => e.to_compile_error().into(),
    }
}

#[proc_macro_derive(PgRow, attributes(row))]
pub fn pg_derive_row(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let input = syn::parse_macro_input!(input as syn::DeriveInput);
    match row::Row(&input).emit() {
        Ok(tokens) => tokens.into(),
        Err(error) => error.to_compile_error().into(),
    }
}

#[proc_macro_attribute]
pub fn query_group(
    attr: proc_macro::TokenStream,
    item: proc_macro::TokenStream,
) -> proc_macro::TokenStream {
    if !attr.is_empty() {
        return syn::Error::new_spanned(
            proc_macro2::TokenStream::from(attr),
            "#[query_group] takes no arguments",
        )
        .to_compile_error()
        .into();
    }
    let block = syn::parse_macro_input!(item as syn::ItemImpl);
    match group::Group::expand(block) {
        Ok(ts) => ts.into(),
        Err(e) => e.to_compile_error().into(),
    }
}

#[proc_macro]
pub fn pg_instance(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    match pg::PgBackend::expand(input) {
        Ok(ts) => ts.into(),
        Err(e) => e.to_compile_error().into(),
    }
}

#[proc_macro_derive(Table, attributes(table_name))]
pub fn sqlite_table(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let input = syn::parse_macro_input!(input as syn::DeriveInput);
    match sqlite::Backend::derive_table(&input) {
        Ok(ts) => ts.into(),
        Err(e) => e.to_compile_error().into(),
    }
}

#[proc_macro_attribute]
pub fn query(
    attr: proc_macro::TokenStream,
    item: proc_macro::TokenStream,
) -> proc_macro::TokenStream {
    if !attr.is_empty() {
        return syn::Error::new_spanned(
            proc_macro2::TokenStream::from(attr),
            "#[query] takes no arguments",
        )
        .to_compile_error()
        .into();
    }
    let f = syn::parse_macro_input!(item as syn::ItemFn);
    match sqlite::Query::expand(&f) {
        Ok((_, ts)) => ts.into(),
        Err(e) => e.to_compile_error().into(),
    }
}

#[proc_macro]
pub fn catalog(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    match sqlite::Catalog::from_tokens(input) {
        Ok(tokens) => tokens.into(),
        Err(error) => error.to_compile_error().into(),
    }
}

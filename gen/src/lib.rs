#![allow(clippy::too_many_arguments)]

extern crate proc_macro;

use backend::Backend;
use proc_macro::TokenStream;
use syn::{DeriveInput, ItemImpl, parse_macro_input};

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

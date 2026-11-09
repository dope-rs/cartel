use std::borrow::Cow;

pub(crate) enum Kind<'a> {
    One(&'a syn::Type),
    First(&'a syn::Type),
    Aggregate(&'a syn::Type),
    All(&'a syn::Type),
    Stream(&'a syn::Type),
    NoRows,
}

pub(crate) struct Plan<'a> {
    pub(crate) output: Cow<'a, syn::ReturnType>,
    pub(crate) sql_parts: Vec<proc_macro2::TokenStream>,
    pub(crate) n_result_cols: proc_macro2::TokenStream,
    pub(crate) dispatch: Kind<'a>,
    pub(crate) probe_override: Option<syn::Block>,
}

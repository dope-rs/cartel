use std::borrow::Cow;

use crate::shape;

pub(crate) enum Kind<'a> {
    One(&'a syn::Type),
    First(&'a syn::Type),
    Aggregate(&'a syn::Type),
    All(&'a syn::Type),
    Stream(&'a syn::Type),
    NoRows,
}

impl<'a> Kind<'a> {
    pub(crate) fn rows(kind: shape::Terminator, row: &'a syn::Type) -> Self {
        match kind {
            shape::Terminator::One => Self::One(row),
            shape::Terminator::First => Self::First(row),
            shape::Terminator::All => Self::All(row),
            shape::Terminator::Stream => Self::Stream(row),
        }
    }
}

pub(crate) struct Plan<'a> {
    pub(crate) output: Cow<'a, syn::ReturnType>,
    pub(crate) sql_parts: Vec<proc_macro2::TokenStream>,
    pub(crate) n_result_cols: proc_macro2::TokenStream,
    pub(crate) dispatch: Kind<'a>,
    pub(crate) probe_override: Option<syn::Block>,
}

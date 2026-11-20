use crate::shape;

pub(crate) enum Kind<'a> {
    One(&'a syn::Type),
    First(&'a syn::Type),
    Aggregate(&'a syn::Type),
    All(&'a syn::Type),
    Stream(&'a syn::Type),
    NoRows(bool),
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
    pub(crate) output: &'a syn::ReturnType,
    pub(crate) sql_parts: Vec<proc_macro2::TokenStream>,
    pub(crate) n_result_cols: proc_macro2::TokenStream,
    pub(crate) dispatch: Kind<'a>,
    pub(crate) result: proc_macro2::TokenStream,
}

pub(crate) mod token {
    pub(crate) const SELECT: &str = "SELECT ";
    pub(crate) const FROM: &str = " FROM ";
    pub(crate) const WHERE: &str = " WHERE ";
    pub(crate) const ORDER_BY: &str = " ORDER BY ";
    pub(crate) const DESC: &str = " DESC";
    pub(crate) const EQ: &str = " = ";
    pub(crate) const COUNT_STAR: &str = "COUNT(*)";
    pub(crate) const INSERT_INTO: &str = "INSERT INTO ";
    pub(crate) const UPDATE: &str = "UPDATE ";
    pub(crate) const SET: &str = " SET ";
    pub(crate) const SUBQUERY_OPEN: &str = "(SELECT ";
    pub(crate) const COMMA: &str = ", ";
    pub(crate) const COMMA_TIGHT: &str = ",";
    pub(crate) const SPACE: &str = " ";
    pub(crate) const PAREN_OPEN: &str = "(";
    pub(crate) const PAREN_CLOSE: &str = ")";
    pub(crate) const PAREN_OPEN_LEADING_SPACE: &str = " (";
    pub(crate) const PAREN_CLOSE_SPACE: &str = ") ";

    pub(crate) fn fragment(text: &str) -> proc_macro2::TokenStream {
        quote::quote! { #text }
    }
}

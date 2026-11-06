pub(crate) enum Kind {
    One(syn::Type),
    First(syn::Type),
    Aggregate(syn::Type),
    All(syn::Type),
    Stream(syn::Type),
    NoRows,
}

pub(crate) struct Plan {
    pub(crate) output: syn::ReturnType,
    pub(crate) sql_parts: Vec<proc_macro2::TokenStream>,
    pub(crate) n_result_cols: proc_macro2::TokenStream,
    pub(crate) dispatch: Kind,
    pub(crate) probe_override: Option<syn::Block>,
}

impl Plan {
    pub(crate) fn no_rows(output: syn::ReturnType, sql: Vec<proc_macro2::TokenStream>) -> Self {
        Self {
            output,
            sql_parts: sql,
            n_result_cols: quote::quote! { 0u16 },
            dispatch: Kind::NoRows,
            probe_override: None,
        }
    }
}

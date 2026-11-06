pub(crate) struct Var {
    pub(crate) ident: syn::Ident,
    pub(crate) table_const: Option<proc_macro2::TokenStream>,
}

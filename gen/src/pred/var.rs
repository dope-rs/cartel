pub(crate) struct Var {
    pub(crate) referenced: std::cell::Cell<bool>,
    pub(crate) ty: proc_macro2::TokenStream,
    pub(crate) ident: syn::Ident,
    pub(crate) table_const: Option<proc_macro2::TokenStream>,
}

use syn::Type;

pub(super) struct QueryPlan {
    pub(super) row_ty: Type,
    pub(super) sql_parts: Vec<proc_macro2::TokenStream>,
    pub(super) n_result_cols: proc_macro2::TokenStream,
    pub(super) decode: DecodeKind,
    pub(super) dispatch: DispatchKind,
    pub(super) probe_override: Option<proc_macro2::TokenStream>,
}

pub(super) enum DecodeKind {
    Row(Box<Type>),
    Unit,
}

pub(super) enum DispatchKind {
    One(Type),
    First(Type),
    All(Type),
    Stream(Type),
    NoRows,
}

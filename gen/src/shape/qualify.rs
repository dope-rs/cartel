use crate::shape::{bound, lock};

pub(crate) struct Order {
    pub(crate) closure: syn::Expr,
    pub(crate) dir: Dir,
}

#[derive(Clone, Copy)]
pub(crate) enum Dir {
    Asc,
    Desc,
}

#[derive(Default)]
pub(crate) struct State {
    pub(crate) order_by: Vec<Order>,
    pub(crate) limit: bound::Bound,
    pub(crate) offset: bound::Bound,
    pub(crate) distinct: Distinct,
    pub(crate) lock: lock::Lock,
}

#[derive(Default)]
pub(crate) enum Distinct {
    #[default]
    None,
    All,
    On(syn::Ident, syn::Expr),
}

use crate::shape::lock;

pub(crate) struct Order<'a> {
    pub(crate) closure: &'a syn::Expr,
    pub(crate) dir: Dir,
}

#[derive(Clone, Copy)]
pub(crate) enum Dir {
    Asc,
    Desc,
}

#[derive(Default)]
pub(crate) struct State<'a> {
    pub(crate) order_by: Vec<Order<'a>>,
    pub(crate) limit: Option<&'a syn::Expr>,
    pub(crate) offset: Option<&'a syn::Expr>,
    pub(crate) distinct: Distinct<'a>,
    pub(crate) lock: lock::Lock,
}

impl State<'_> {
    pub(crate) fn ordered(&self, span: proc_macro2::Span) -> syn::Result<()> {
        if self.limit.is_some()
            || self.offset.is_some()
            || !matches!(self.distinct, Distinct::None)
            || !matches!(self.lock, lock::Lock::None)
            || self
                .order_by
                .iter()
                .any(|order| matches!(order.dir, Dir::Desc))
        {
            return Err(syn::Error::new(
                span,
                "joined and grouped rows support only order_by",
            ));
        }
        Ok(())
    }
}

#[derive(Default)]
pub(crate) enum Distinct<'a> {
    #[default]
    None,
    All,
    On(&'a syn::Ident, &'a syn::Expr),
}

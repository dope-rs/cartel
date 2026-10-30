use syn::spanned::Spanned as _;

#[derive(Default)]
pub(crate) enum Bound {
    #[default]
    None,
    Expr(syn::Expr),
}

impl Bound {
    pub(crate) fn add(
        self,
        parent: &Self,
        expr: syn::Expr,
        span: proc_macro2::Span,
        name: &str,
    ) -> syn::Result<Self> {
        match (parent, &self) {
            (Self::None, Self::None) => Ok(Self::Expr(expr)),
            (Self::Expr(prior), Self::None) | (Self::None, Self::Expr(prior)) => {
                let mut error = syn::Error::new(span, format!("duplicate `.{name}(...)`"));
                error.combine(syn::Error::new(prior.span(), "first qualifier is here"));
                Err(error)
            }
            (Self::Expr(parent), Self::Expr(prior)) => {
                let mut error = syn::Error::new(span, format!("duplicate `.{name}(...)`"));
                error.combine(syn::Error::new(parent.span(), "outer qualifier is here"));
                error.combine(syn::Error::new(prior.span(), "first qualifier is here"));
                Err(error)
            }
        }
    }

    pub(crate) fn merge(self, parent: &mut Self) {
        match self {
            Self::None => {}
            Self::Expr(expr) => *parent = Self::Expr(expr),
        }
    }
}

use crate::backend;
use syn::ext::IdentExt as _;

enum Part {
    Token(proc_macro2::TokenStream),
    Capture(syn::Ident),
}

pub(crate) struct Sql(Vec<Part>);

impl Sql {
    pub(crate) const fn new() -> Self {
        Self(Vec::new())
    }

    pub(crate) fn tokens(tokens: Vec<proc_macro2::TokenStream>) -> Self {
        Self(tokens.into_iter().map(Part::Token).collect())
    }

    pub(crate) fn captured(ident: syn::Ident) -> Self {
        Self(vec![Part::Capture(ident)])
    }

    pub(crate) fn infix(
        left: Self,
        operator: proc_macro2::TokenStream,
        right: Self,
        dialect: &backend::Dialect,
    ) -> Self {
        Self::tokens(vec![dialect.kw("PAREN_OPEN")])
            .append(left)
            .push(operator)
            .append(right)
            .push(dialect.kw("PAREN_CLOSE"))
    }

    pub(crate) fn call<const N: usize>(
        prefix: proc_macro2::TokenStream,
        args: [Self; N],
        dialect: &backend::Dialect,
    ) -> Self {
        let mut sql = Self::tokens(vec![prefix]);
        for (index, arg) in args.into_iter().enumerate() {
            if index > 0 {
                sql = sql.push(dialect.kw("COMMA"));
            }
            sql = sql.append(arg);
        }
        sql.push(dialect.kw("PAREN_CLOSE"))
    }

    pub(crate) fn push(mut self, token: proc_macro2::TokenStream) -> Self {
        self.0.push(Part::Token(token));
        self
    }

    pub(crate) fn append(mut self, mut other: Self) -> Self {
        self.0.append(&mut other.0);
        self
    }

    pub(crate) fn render(
        self,
        captures: &mut Vec<syn::Ident>,
        ids: &[syn::Ident],
        tys: &[syn::Type],
        dialect: &backend::Dialect,
    ) -> Vec<proc_macro2::TokenStream> {
        let mut tokens = Vec::with_capacity(self.0.len());
        for part in self.0 {
            match part {
                Part::Token(token) => tokens.push(token),
                Part::Capture(ident) => {
                    let index = match captures
                        .iter()
                        .position(|capture| capture.unraw() == ident.unraw())
                    {
                        Some(index) => index,
                        None => {
                            let index = captures.len();
                            captures.push(ident);
                            index
                        }
                    };
                    let context = backend::ParamCtx {
                        captures,
                        param_ids: ids,
                        param_tys: tys,
                    };
                    tokens.push(dialect.placeholder(index, &context));
                }
            }
        }
        tokens
    }

    pub(crate) fn join_tokens(self) -> syn::Result<Vec<proc_macro2::TokenStream>> {
        let mut tokens = Vec::with_capacity(self.0.len());
        for part in self.0 {
            match part {
                Part::Token(token) => tokens.push(token),
                Part::Capture(ident) => {
                    return Err(syn::Error::new(
                        ident.span(),
                        "JOIN ON clause must compare columns only; bound parameters belong in `.filter(...)`",
                    ));
                }
            }
        }
        Ok(tokens)
    }
}

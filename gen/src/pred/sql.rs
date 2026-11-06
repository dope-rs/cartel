use crate::backend;
use syn::ext::IdentExt as _;

enum Part {
    Token(proc_macro2::TokenStream),
    Capture(syn::Ident),
}

pub(crate) struct Sql(Vec<Part>);

pub(crate) fn intern(captures: &mut Vec<syn::Ident>, ident: syn::Ident) -> usize {
    if let Some(index) = captures
        .iter()
        .position(|current| current.unraw() == ident.unraw())
    {
        return index;
    }
    let index = captures.len();
    captures.push(ident);
    index
}

impl Sql {
    pub(crate) const fn new() -> Self {
        Self(Vec::new())
    }

    pub(super) fn len(&self) -> usize {
        self.0.len()
    }

    pub(super) fn truncate(&mut self, len: usize) {
        self.0.truncate(len);
    }

    pub(super) fn capture(&mut self, ident: syn::Ident) {
        self.0.push(Part::Capture(ident));
    }

    pub(super) fn push(&mut self, token: proc_macro2::TokenStream) {
        self.0.push(Part::Token(token));
    }

    pub(super) fn extend(&mut self, tokens: impl IntoIterator<Item = proc_macro2::TokenStream>) {
        self.0.extend(tokens.into_iter().map(Part::Token));
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
                    let index = intern(captures, ident);
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

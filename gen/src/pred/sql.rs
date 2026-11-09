use crate::backend;

enum Part {
    Token(proc_macro2::TokenStream),
    Capture(backend::Capture),
}

pub(crate) struct Sql(Vec<Part>);

pub(crate) fn intern(captures: &mut Vec<backend::Capture>, capture: backend::Capture) -> usize {
    if let Some(index) = captures.iter().position(|current| current.0 == capture.0) {
        return index;
    }
    let index = captures.len();
    captures.push(capture);
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

    pub(super) fn capture(&mut self, capture: backend::Capture) {
        self.0.push(Part::Capture(capture));
    }

    pub(super) fn push(&mut self, token: proc_macro2::TokenStream) {
        self.0.push(Part::Token(token));
    }

    pub(super) fn extend(&mut self, tokens: impl IntoIterator<Item = proc_macro2::TokenStream>) {
        self.0.extend(tokens.into_iter().map(Part::Token));
    }

    pub(crate) fn render(
        self,
        captures: &mut Vec<backend::Capture>,
        params: &[backend::Param],
        dialect: &backend::Dialect,
    ) -> Vec<proc_macro2::TokenStream> {
        let mut tokens = Vec::with_capacity(self.0.len());
        for part in self.0 {
            match part {
                Part::Token(token) => tokens.push(token),
                Part::Capture(capture) => {
                    let index = intern(captures, capture);
                    tokens.push(dialect.placeholder(index, &params[capture.0].1));
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
                Part::Capture((_, span)) => {
                    return Err(syn::Error::new(
                        span,
                        "JOIN ON clause must compare columns only; bound parameters belong in `.filter(...)`",
                    ));
                }
            }
        }
        Ok(tokens)
    }
}

use crate::backend;

enum Part {
    Token(proc_macro2::TokenStream),
    Capture(backend::Capture),
}

pub(crate) struct Sql(Vec<Part>);

pub(crate) struct Operand {
    pub(crate) sql: Sql,
    pub(crate) rust: proc_macro2::TokenStream,
}

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

    pub(crate) fn len(&self) -> usize {
        self.0.len()
    }

    pub(super) fn truncate(&mut self, len: usize) {
        self.0.truncate(len);
    }

    pub(super) fn capture(&mut self, capture: backend::Capture) {
        self.0.push(Part::Capture(capture));
    }

    pub(crate) fn push(&mut self, token: proc_macro2::TokenStream) {
        self.0.push(Part::Token(token));
    }

    pub(super) fn extend(&mut self, tokens: impl IntoIterator<Item = proc_macro2::TokenStream>) {
        self.0.extend(tokens.into_iter().map(Part::Token));
    }

    pub(crate) fn render_into(
        self,
        captures: &mut Vec<backend::Capture>,
        params: &[backend::Param],
        dialect: &backend::Dialect,
        tokens: &mut Vec<proc_macro2::TokenStream>,
    ) {
        tokens.extend(self.0.into_iter().map(|part| match part {
            Part::Token(token) => token,
            Part::Capture(capture) => {
                let index = intern(captures, capture);
                dialect.placeholder(index, &params[capture.0].1)
            }
        }));
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

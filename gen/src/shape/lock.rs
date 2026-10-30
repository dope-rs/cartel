use crate::backend;

#[derive(Default, Clone, Copy)]
pub(crate) enum Lock {
    #[default]
    None,
    ForUpdate(proc_macro2::Span),
    ForShare(proc_macro2::Span),
}

impl Lock {
    pub(crate) fn reject(self, message: &str) -> syn::Result<()> {
        match self {
            Self::None => Ok(()),
            Self::ForUpdate(span) | Self::ForShare(span) => Err(syn::Error::new(span, message)),
        }
    }

    pub(crate) fn try_render(
        self,
        dialect: &backend::Dialect,
    ) -> syn::Result<Option<proc_macro2::TokenStream>> {
        let keyword = match self {
            Self::None => return Ok(None),
            Self::ForUpdate(_) => "FOR_UPDATE",
            Self::ForShare(_) => "FOR_SHARE",
        };
        if matches!(dialect.kind, backend::Kind::Sqlite) {
            self.reject("this backend does not support row locking")?;
        }
        Ok(Some(dialect.kw(keyword)))
    }
}

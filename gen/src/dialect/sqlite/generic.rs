use syn::visit::{self, Visit as _};

pub(super) struct Generic<'a> {
    generics: &'a syn::Generics,
    found: bool,
}

impl<'a> Generic<'a> {
    pub(super) fn contains(generics: &'a syn::Generics, ty: &syn::Type) -> bool {
        let mut generic = Self {
            generics,
            found: false,
        };
        generic.visit_type(ty);
        generic.found
    }
}

impl<'ast> visit::Visit<'ast> for Generic<'_> {
    fn visit_path(&mut self, path: &'ast syn::Path) {
        if path.leading_colon.is_none()
            && let Some(first) = path.segments.first()
        {
            self.found |= first.ident == "Self"
                || self.generics.params.iter().any(|param| match param {
                    syn::GenericParam::Type(param) => param.ident == first.ident,
                    syn::GenericParam::Const(param) => param.ident == first.ident,
                    syn::GenericParam::Lifetime(_) => false,
                });
        }
        visit::visit_path(self, path);
    }

    fn visit_lifetime(&mut self, lifetime: &'ast syn::Lifetime) {
        self.found |= self
            .generics
            .lifetimes()
            .any(|param| param.lifetime == *lifetime);
    }

    /// Type macros can mention parameters absent from their input tokens.
    fn visit_type_macro(&mut self, _ty: &'ast syn::TypeMacro) {
        self.found |= !self.generics.params.is_empty();
    }
}

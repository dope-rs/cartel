use syn::visit_mut;
use syn::visit_mut::VisitMut as _;

pub(super) struct Param<'a, const BORROW_OWNED: bool>(&'a syn::Lifetime);

impl<'a, const BORROW_OWNED: bool> Param<'a, BORROW_OWNED> {
    pub(super) fn apply(mut types: Vec<syn::Type>, lifetime: &'a syn::Lifetime) -> Vec<syn::Type> {
        for ty in &mut types {
            Self(lifetime).visit_type_mut(ty);
        }
        types
    }

    fn is_string(ty: &syn::Type) -> bool {
        let syn::Type::Path(path) = ty else {
            return false;
        };
        path.qself.is_none()
            && path
                .path
                .segments
                .last()
                .is_some_and(|segment| segment.ident == "String" && segment.arguments.is_empty())
    }

    fn is_bytes(ty: &syn::Type) -> bool {
        let syn::Type::Path(path) = ty else {
            return false;
        };
        let Some(segment) = path.path.segments.last() else {
            return false;
        };
        let syn::PathArguments::AngleBracketed(args) = &segment.arguments else {
            return false;
        };
        segment.ident == "Vec"
            && args.args.len() == 1
            && matches!(
                args.args.first(),
                Some(syn::GenericArgument::Type(syn::Type::Path(element)))
                    if element.path.is_ident("u8")
            )
    }
}

impl<const BORROW_OWNED: bool> visit_mut::VisitMut for Param<'_, BORROW_OWNED> {
    fn visit_type_mut(&mut self, ty: &mut syn::Type) {
        let element = match &*ty {
            syn::Type::FnPtr(_) => return,
            syn::Type::Reference(value) => value.elem.as_ref(),
            other => other,
        };
        let lifetime = self.0;
        if BORROW_OWNED && Self::is_string(element) {
            *ty = syn::parse_quote!(&#lifetime str);
        } else if BORROW_OWNED && Self::is_bytes(element) {
            *ty = syn::parse_quote!(&#lifetime [u8]);
        } else {
            visit_mut::visit_type_mut(self, ty);
        }
    }

    /// Callable parentheses bind their own regions; only type arguments inherit the input region.
    fn visit_path_arguments_mut(&mut self, args: &mut syn::PathArguments) {
        if let syn::PathArguments::AngleBracketed(args) = args {
            visit_mut::visit_angle_bracketed_generic_arguments_mut(self, args);
        }
    }

    fn visit_type_reference_mut(&mut self, ty: &mut syn::TypeReference) {
        if ty
            .lifetime
            .as_ref()
            .is_none_or(|lifetime| lifetime.ident == "_")
        {
            ty.lifetime = Some(self.0.clone());
        }
        visit_mut::visit_type_reference_mut(self, ty);
    }
}

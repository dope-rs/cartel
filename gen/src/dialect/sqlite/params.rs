use crate::{backend, plan};

pub(super) struct Params {
    pub(super) ty: proc_macro2::TokenStream,
    pub(super) value: proc_macro2::TokenStream,
}

impl Params {
    pub(super) fn new(
        params: &[backend::Param],
        captures: &[backend::Capture],
        checks: &[(usize, plan::Bound)],
    ) -> Self {
        let mut ty = quote::quote! { () };
        let mut value = quote::quote! { () };
        for &(index, span) in captures.iter().rev() {
            let (ident, plain) = &params[index];
            let mut capture = ident.clone();
            capture.set_span(span);
            let role = checks.iter().find(|entry| entry.0 == index);
            let role = role.map(|entry| match entry.1 {
                plan::Bound::Limit => quote::quote! { ::cartel_sqlite::Limit },
                plan::Bound::Offset => quote::quote! { ::cartel_sqlite::Offset },
            });
            let (item, bound) = match role {
                Some(role) => (
                    quote::quote! { ::cartel_sqlite::Bound<#role> },
                    quote::quote! { ::cartel_sqlite::Bound::<#role>::new(#capture) },
                ),
                None => (quote::quote! { #plain }, quote::quote! { #capture }),
            };
            ty = quote::quote! { (#item, #ty) };
            value = quote::quote! { (#bound, #value) };
        }
        Self { ty, value }
    }
}

use crate::{ext, plan};

pub(super) struct Params {
    pub(super) ty: proc_macro2::TokenStream,
    pub(super) value: proc_macro2::TokenStream,
}

impl Params {
    pub(super) fn new(
        params: &[crate::backend::Param],
        captures: &[syn::Ident],
        checks: &[(syn::Ident, plan::Bound)],
    ) -> syn::Result<Self> {
        let mut ty = quote::quote! { () };
        let mut value = quote::quote! { () };
        for capture in captures.iter().rev() {
            let display = capture.to_string();
            let name = ext::unraw(&display);
            let Some((_, plain)) = params
                .iter()
                .find(|(id, _)| ext::unraw(&id.to_string()) == name)
            else {
                return Err(syn::Error::new(
                    capture.span(),
                    "unknown captured parameter",
                ));
            };
            let role = checks
                .iter()
                .find(|entry| ext::unraw(&entry.0.to_string()) == name);
            drop(display);
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
        Ok(Self { ty, value })
    }
}

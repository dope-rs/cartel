use crate::plan;
use syn::ext::IdentExt as _;

pub(super) struct Params {
    pub(super) ty: proc_macro2::TokenStream,
    pub(super) value: proc_macro2::TokenStream,
}

impl Params {
    pub(super) fn new(
        ids: &[syn::Ident],
        types: &[syn::Type],
        captures: &[syn::Ident],
        checks: &[(syn::Ident, plan::Bound)],
    ) -> syn::Result<Self> {
        let mut ty = quote::quote! { () };
        let mut value = quote::quote! { () };
        for capture in captures.iter().rev() {
            let Some(index) = ids.iter().position(|id| id.unraw() == capture.unraw()) else {
                return Err(syn::Error::new(
                    capture.span(),
                    "unknown captured parameter",
                ));
            };
            let plain = &types[index];
            let role = checks
                .iter()
                .find(|entry| entry.0.unraw() == capture.unraw())
                .map(|entry| match entry.1 {
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

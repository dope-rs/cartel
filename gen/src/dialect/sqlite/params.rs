use crate::{backend, plan};

pub(super) fn bind(
    params: &[backend::Param],
    captures: &[backend::Capture],
    checks: &[(usize, plan::Bound)],
) -> proc_macro2::TokenStream {
    let mut value = quote::quote! { () };
    for &(index, span) in captures.iter().rev() {
        let mut capture = params[index].0.clone();
        capture.set_span(span);
        let bound = match checks.iter().find(|entry| entry.0 == index) {
            Some((_, kind)) => {
                let role = match kind {
                    plan::Bound::Limit => quote::quote! { ::cartel_sqlite::Limit },
                    plan::Bound::Offset => quote::quote! { ::cartel_sqlite::Offset },
                };
                quote::quote! { ::cartel_sqlite::Bound::<#role>::new(#capture) }
            }
            None => quote::quote! { #capture },
        };
        value = quote::quote! { (#bound, #value) };
    }
    value
}

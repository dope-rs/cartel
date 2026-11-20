mod kind;
mod tag;

pub(super) trait TypeExt {
    fn param(&self) -> syn::Result<&syn::Type>;
    fn row_read(&self) -> syn::Result<proc_macro2::TokenStream>;
    fn slice_type(&self) -> Option<proc_macro2::TokenStream>;
}

impl TypeExt for syn::Type {
    fn param(&self) -> syn::Result<&syn::Type> {
        kind::Kind::new(self).param()
    }

    fn row_read(&self) -> syn::Result<proc_macro2::TokenStream> {
        kind::Kind::new(self).row_read()
    }

    fn slice_type(&self) -> Option<proc_macro2::TokenStream> {
        kind::Kind::new(self).tag.slice_type()
    }
}

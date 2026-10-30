use crate::backend;

pub(super) struct Retain(pub(super) proc_macro2::TokenStream);

impl Retain {
    pub(super) fn of(first: &syn::Type, fields: &[backend::TableField<'_>]) -> Self {
        fields.iter().fold(Self(Self::field(first)), |mode, field| {
            let mode = mode.0;
            let ty = field.ty;
            Self(quote::quote! {
                <#mode as ::cartel_sqlite::row::modes::Kind>::With<
                    <#ty as ::cartel_sqlite::row::raw::Decode>::Retention
                >
            })
        })
    }

    pub(super) fn bind(
        first: &syn::Type,
        fields: &[backend::TableField<'_>],
        generics: &mut syn::Generics,
        names: &mut backend::Names,
    ) -> Self {
        let mut mode = Self::field(first);
        for field in fields {
            let next = names.fresh("__Retain");
            let ty = Self::field(field.ty);
            generics.params.push(syn::parse_quote!(#next));
            generics
                .make_where_clause()
                .predicates
                .push(syn::parse_quote!(
                    #mode: ::cartel_sqlite::row::modes::Kind<With<#ty> = #next>
                ));
            mode = quote::quote!(#next);
        }
        Self(mode)
    }

    pub(super) fn field(ty: &syn::Type) -> proc_macro2::TokenStream {
        quote::quote! { <#ty as ::cartel_sqlite::row::raw::Decode>::Retention }
    }

    pub(super) fn require(&self, generics: &mut syn::Generics, ty: &syn::Type) {
        let mode = &self.0;
        let field = Self::field(ty);
        generics
            .make_where_clause()
            .predicates
            .push(syn::parse_quote!(
                #mode: ::cartel_sqlite::row::modes::Contains<#field>
            ));
    }
}

use crate::backend;
use crate::dialect::pg::types::TypeExt as _;
use syn::visit::Visit as _;

pub(crate) struct Row<'a>(pub(crate) &'a syn::DeriveInput);

impl Row<'_> {
    pub(crate) fn emit(self) -> syn::Result<proc_macro2::TokenStream> {
        let input = self.0;
        if !input.generics.params.is_empty() || input.generics.where_clause.is_some() {
            return Err(syn::Error::new_spanned(
                &input.generics,
                "#[derive(PgRow)] does not support generic structs",
            ));
        }
        let syn::Data::Struct(data) = &input.data else {
            return Err(syn::Error::new_spanned(
                input,
                "PgRow requires named fields",
            ));
        };
        let syn::Fields::Named(fields) = &data.fields else {
            return Err(syn::Error::new_spanned(
                input,
                "PgRow requires named fields",
            ));
        };
        if fields.named.is_empty() || fields.named.len() > i16::MAX as usize {
            return Err(syn::Error::new_spanned(
                fields,
                format!(
                    "PostgreSQL row requires 1..={} fields; actual={}",
                    i16::MAX,
                    fields.named.len()
                ),
            ));
        }
        let name = &input.ident;
        let fields = fields.named.pairs().map(|pair| pair.into_value());
        let shape = fields
            .clone()
            .rev()
            .fold(quote::quote! { () }, |tail, field| {
                let ty = &field.ty;
                quote::quote! { (#ty, #tail) }
            });
        let mut owned = Vec::new();
        for field in fields.clone() {
            let name = &field.ident;
            let read = field.ty.row_read()?;
            owned.push(quote::quote! { #name: #read, });
        }
        let row = quote::format_ident!("{name}View");
        let mut reserved = backend::Names::default();
        reserved.visit_derive_input(input);
        let lease = reserved.fresh("__cartel_lease_");
        let vis = &input.vis;
        let mut decls = Vec::new();
        for field in fields.clone() {
            let name = &field.ident;
            let vis = &field.vis;
            let ty = &field.ty;
            decls.push(quote::quote! {
                #vis #name: <#ty as ::cartel_pg::schema::Row>::View<'row>
            });
        }
        let names = fields.map(|field| &field.ident);
        let pattern = names
            .clone()
            .rev()
            .fold(quote::quote! { () }, |tail, name| {
                quote::quote! { (#name, #tail) }
            });
        Ok(quote::quote! {
            #[derive(Clone, Copy)]
            #vis struct #row<'row> {
                #(#decls,)*
                #lease: ::core::marker::PhantomData<&'row ()>,
            }

            impl ::cartel_pg::schema::Project for #name {
                type Fields = #shape;

                fn project<'row>(fields: <Self::Fields as ::cartel_pg::schema::Row>::View<'row>) -> Self::View<'row> {
                    let #pattern = fields;
                    #row { #(#names,)* #lease: ::core::marker::PhantomData }
                }
            }

            impl ::cartel_pg::schema::Row for #name {
                type View<'row> = #row<'row>;

                fn decode<'row>(row: &mut ::cartel_pg::protocol::value::decode::RowReader<'row>) -> ::core::result::Result<Self::View<'row>, ::cartel_pg::Error> {
                    let fields = <<Self as ::cartel_pg::schema::Project>::Fields as ::cartel_pg::schema::Row>::decode(row)?;
                    ::core::result::Result::Ok(<Self as ::cartel_pg::schema::Project>::project(fields))
                }

                fn decode_owned(row: &mut ::cartel_pg::protocol::value::decode::RowReader<'_>) -> ::core::result::Result<Self, ::cartel_pg::Error> {
                    ::core::result::Result::Ok(Self { #(#owned)* })
                }
            }

            impl ::cartel_pg::schema::desc::Validate for #name {
                const DYNAMIC_TYPES: u8 =
                    <#shape as ::cartel_pg::schema::desc::Validate>::DYNAMIC_TYPES;

                fn validate(types: &mut ::cartel_pg::schema::desc::Types<'_>) -> ::core::result::Result<(), ::cartel_pg::Error> {
                    <#shape as ::cartel_pg::schema::desc::Validate>::validate(types)
                }

                fn formats(values: &mut ::cartel_pg::schema::desc::Formats<'_>) -> ::core::result::Result<(), ::cartel_pg::protocol::encode::Error> {
                    <#shape as ::cartel_pg::schema::desc::Validate>::formats(values)
                }
            }
        })
    }
}

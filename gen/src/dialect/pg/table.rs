use crate::backend;
use crate::dialect::pg::types::TypeExt as _;
use crate::ext::SqlIdent as _;

impl backend::TableBackend for super::PgBackend {
    fn emit_table(
        spec: backend::TableSpec<'_>,
        dialect: &backend::Dialect,
    ) -> syn::Result<proc_macro2::TokenStream> {
        let columns = spec.columns();
        let backend::TableSpec {
            input,
            table_name: table,
            fields,
        } = spec;
        if let Some(param) = input.generics.params.first() {
            return Err(syn::Error::new_spanned(
                param,
                "#[derive(PgTable)] does not support generic structs",
            ));
        }
        if let Some(clause) = &input.generics.where_clause {
            return Err(syn::Error::new_spanned(
                clause,
                "#[derive(PgTable)] does not support generic structs",
            ));
        }
        let rt = &dialect.rt_crate;
        let name = &input.ident;
        if fields.len() > i16::MAX as usize {
            return Err(syn::Error::new(
                name.span(),
                format!(
                    "PostgreSQL row has {} columns; maximum is {}",
                    fields.len(),
                    i16::MAX
                ),
            ));
        }
        let mut batch = false;
        for attr in &input.attrs {
            if attr.path().is_ident("table") {
                attr.parse_nested_meta(|meta| {
                    if !meta.path.is_ident("batch") {
                        return Err(meta.error("expected batch"));
                    }
                    if batch {
                        return Err(meta.error("duplicate batch"));
                    }
                    batch = true;
                    Ok(())
                })?;
            }
        }
        let filter = quote::quote! {
            pub fn filter(
                scope: &#rt::dsl::Scope,
                pred: impl ::core::ops::FnOnce(Self) -> bool,
            ) -> #rt::dsl::query::Filter<Self> {
                match (scope.0, pred) {}
            }
        };
        let sql = table.quoted();
        if fields.is_empty() {
            if batch {
                return Err(syn::Error::new_spanned(input, "batch requires columns"));
            }
            return Ok(quote::quote! {
                impl #name {
                    #[doc(hidden)]
                    pub const __CARTEL_TABLE: &'static str = #sql;

                    #filter
                }
            });
        }
        let mut fmts: Vec<u16> = Vec::new();
        let mut text = false;
        for f in &fields {
            let fmt = f.ty.format();
            if fmt == 0 {
                text = true;
            }
            fmts.push(fmt);
        }
        let codes = if text {
            quote::quote! {
                #[doc(hidden)]
                pub const __CARTEL_RESULT_FORMAT_CODES: &'static [u16] = &[ #( #fmts, )* ];
            }
        } else {
            quote::quote! {
                #[doc(hidden)]
                pub const __CARTEL_RESULT_FORMAT_CODES: &'static [u16] = &[1u16];
            }
        };
        let emit = if batch {
            let slices = quote::format_ident!("{name}Slices");
            let vis = &input.vis;
            let mut slice = Vec::new();
            for f in &fields {
                let fname = f.name;
                let vis = f.vis;
                let ty = f.ty.slice_type().ok_or_else(|| {
                    syn::Error::new_spanned(f.ty, "batch column has no slice representation")
                })?;
                slice.push(quote::quote! { #vis #fname: #ty });
            }
            quote::quote! {
                #vis struct #slices<'data> { #(#slice,)* }

                impl #name {
                    pub fn insert_each<'data>(
                        scope: &#rt::dsl::Scope,
                        init: impl ::core::ops::FnOnce(&mut #slices<'data>),
                    ) -> #rt::dsl::write::Insert<Self> {
                        match (scope.0, init) {}
                    }
                }
            }
        } else {
            quote::quote! {}
        };

        Ok(quote::quote! {
            #emit

            impl #name {
                #columns

                #[doc(hidden)]
                pub const __CARTEL_TABLE: &'static str = #sql;
                #codes
            }

            impl #name {
                #filter

                pub fn join<T>(
                    scope: &#rt::dsl::Scope,
                    pred: impl ::core::ops::FnOnce(Self, T) -> bool,
                ) -> #rt::dsl::query::join::Plan2<Self, T> {
                    match (scope.0, pred) {}
                }
                pub fn left_join<T>(
                    scope: &#rt::dsl::Scope,
                    pred: impl ::core::ops::FnOnce(Self, T) -> bool,
                ) -> #rt::dsl::query::join::Plan2<Self, T> {
                    match (scope.0, pred) {}
                }
                pub fn lateral_join<T>(
                    scope: &#rt::dsl::Scope,
                    plan: impl ::core::ops::FnOnce(Self) -> #rt::dsl::query::Filter<T>,
                ) -> #rt::dsl::query::join::Plan2<Self, T> {
                    match (scope.0, plan) {}
                }
                pub fn lateral_left<T>(
                    scope: &#rt::dsl::Scope,
                    plan: impl ::core::ops::FnOnce(Self) -> #rt::dsl::query::Filter<T>,
                ) -> #rt::dsl::query::join::Plan2<Self, T> {
                    match (scope.0, plan) {}
                }

                pub fn insert(
                    scope: &#rt::dsl::Scope,
                    init: impl ::core::ops::FnOnce(&mut Self),
                )
                    -> #rt::dsl::write::Insert<Self>
                {
                    match (scope.0, init) {}
                }
                pub fn insert_from<S: #rt::dsl::shape::SourceRow>(
                    scope: &#rt::dsl::Scope,
                    source: S,
                    init: impl ::core::ops::FnOnce(&mut Self, S::Row),
                ) -> #rt::dsl::write::Insert<Self> {
                    match (scope.0, source, init) {}
                }
            }
        })
    }
}

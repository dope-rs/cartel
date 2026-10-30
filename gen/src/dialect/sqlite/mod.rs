use crate::backend;
use crate::ext::SqlIdent as _;
use syn::visit::Visit as _;
use syn::visit_mut::VisitMut as _;

mod catalog;
mod generic;
mod owner;
mod params;
mod query;
mod retain;
pub(crate) use catalog::Catalog;
pub(crate) use query::Query;

pub(crate) struct Backend;

impl backend::Backend for Backend {
    fn dialect() -> backend::Dialect {
        backend::Dialect {
            rt_crate: syn::parse_quote! { ::cartel_sqlite },
            placeholder: backend::Placeholder::Question,
            kind: backend::Kind::Sqlite,
        }
    }
}

impl backend::TableBackend for Backend {
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
        let rt = &dialect.rt_crate;
        let name = &input.ident;
        let (impl_g, type_g, where_g) = input.generics.split_for_impl();
        let model = syn::parse_quote!(#name #type_g);
        let mut owner = owner::Owner::new(&model);
        let types: Vec<_> = fields
            .iter()
            .map(|field| {
                let mut ty = field.ty.clone();
                owner.visit_type_mut(&mut ty);
                ty
            })
            .collect();
        let fields: Vec<_> = fields
            .iter()
            .zip(&types)
            .map(|(field, ty)| backend::TableField {
                name: field.name,
                ty,
                vis: field.vis,
            })
            .collect();
        let Some((first, tail)) = fields.split_first() else {
            return Err(syn::Error::new(
                name.span(),
                "SQLite Table requires at least one field",
            ));
        };
        if fields.len() > u16::MAX as usize {
            return Err(syn::Error::new(
                name.span(),
                format!(
                    "SQLite row has {} columns; maximum is {}",
                    fields.len(),
                    u16::MAX
                ),
            ));
        }
        let view = quote::format_ident!("{name}Ref");
        let vis = &input.vis;
        let mut names = backend::Names::default();
        names.visit_derive_input(input);
        let row_lt = names.lifetime("__cartel_row");
        let src_lt = names.lifetime("__cartel_source");
        let slot_ty = names.fresh("__Slot");
        let join_ty = names.fresh("__CartelJoin");
        let input_ty = names.fresh("__CartelSource");
        let mut row_g = input.generics.clone();
        owner.visit_generics_mut(&mut row_g);
        for field in &fields {
            if generic::Generic::contains(&input.generics, field.ty) {
                let ty = field.ty;
                row_g
                    .make_where_clause()
                    .predicates
                    .push(syn::parse_quote!(#ty: #rt::row::raw::Decode));
            }
        }
        let mut view_g = row_g.clone();
        view_g.params.insert(
            0,
            syn::GenericParam::Lifetime(syn::LifetimeParam::new(row_lt.clone())),
        );
        let (view_i, view_t, view_w) = view_g.split_for_impl();
        let tbl_sql = table.quoted();

        let mut view_fs = Vec::new();
        let mut loads = Vec::new();
        let mut keeps = Vec::new();
        let mut dec_g = row_g;
        let mode = if input.generics.params.is_empty() {
            retain::Retain::of(first.ty, tail)
        } else {
            retain::Retain::bind(first.ty, tail, &mut dec_g, &mut names)
        };
        for f in &fields {
            let fname = f.name;
            let fty = f.ty;
            let fvis = f.vis;
            view_fs.push(quote::quote! {
                #fvis #fname: <#fty as #rt::row::raw::Decode>::Borrowed<#row_lt>
            });
            loads.push(quote::quote! {
                #fname: <#fty as #rt::row::raw::Decode>::borrow(reader)?,
            });
            let kind = retain::Retain::field(fty);
            if !input.generics.params.is_empty() && !tail.is_empty() {
                mode.require(&mut dec_g, fty);
            }
            let state = if tail.is_empty() {
                quote::quote!(state)
            } else {
                quote::quote! {
                    <Self::Retention as #rt::row::modes::Contains<#kind>>::get::<#slot_ty>(state)
                }
            };
            keeps.push(quote::quote! {
                #fname: <#fty as #rt::row::raw::Decode>::retain::<#slot_ty>(
                    value.#fname,
                    #state,
                )?,
            });
        }
        let sum = quote::quote! { 0usize #( + <#types as #rt::row::raw::Decode>::N_COLS )* };
        let (dec_i, _, dec_w) = dec_g.split_for_impl();
        let mode = mode.0;

        Ok(quote::quote! {
            #[doc(hidden)]
            #vis struct #view #view_g #view_w {
                #( #view_fs, )*
            }

            impl #view_i ::core::marker::Copy
                for #view #view_t #view_w
            {}

            impl #view_i ::core::clone::Clone
                for #view #view_t #view_w
            {
                fn clone(&self) -> Self {
                    *self
                }
            }

            unsafe impl #dec_i #rt::row::raw::Decode
                for #name #type_g #dec_w
            {
                const N_COLS: usize = #sum;
                type Borrowed<#row_lt> = #view #view_t;
                type Retention = #mode;

                fn borrow<#row_lt>(
                    reader: &mut #rt::row::raw::Reader<#row_lt>,
                ) -> ::core::result::Result<Self::Borrowed<#row_lt>, #rt::Error> {
                    ::core::result::Result::Ok(#view {
                        #( #loads )*
                    })
                }

                fn retain<#src_lt, #row_lt, #slot_ty: ::core::marker::Copy + #row_lt>(
                    value: Self::Borrowed<#src_lt>,
                    state: &mut <Self::Retention as
                        #rt::row::modes::Kind
                    >::State<#row_lt, #slot_ty>,
                ) -> ::core::result::Result<Self::Borrowed<#row_lt>, #rt::Error> {
                    ::core::result::Result::Ok(#view {
                        #( #keeps )*
                    })
                }

            }

            impl #impl_g #name #type_g #where_g {
                #columns

                #[doc(hidden)]
                pub const __CARTEL_TABLE: &'static str = #tbl_sql;
            }

            impl #impl_g #name #type_g #where_g {
                pub fn filter(
                    scope: &#rt::dsl::Scope,
                    pred: impl ::core::ops::FnOnce(#name #type_g) -> bool,
                )
                    -> #rt::dsl::query::Filter<#name #type_g>
                {
                    match (scope.0, pred) {}
                }

                pub fn join<#join_ty>(
                    scope: &#rt::dsl::Scope,
                    pred: impl ::core::ops::FnOnce(#name #type_g, #join_ty) -> bool,
                ) -> #rt::dsl::query::join::Plan2<#name #type_g, #join_ty> {
                    match (scope.0, pred) {}
                }
                pub fn left_join<#join_ty>(
                    scope: &#rt::dsl::Scope,
                    pred: impl ::core::ops::FnOnce(#name #type_g, #join_ty) -> bool,
                ) -> #rt::dsl::query::join::Plan2<#name #type_g, #join_ty> {
                    match (scope.0, pred) {}
                }

                pub fn insert(
                    scope: &#rt::dsl::Scope,
                    init: impl ::core::ops::FnOnce(&mut #name #type_g),
                )
                    -> #rt::dsl::write::Insert<#name #type_g>
                {
                    match (scope.0, init) {}
                }
                pub fn insert_from<#input_ty: #rt::dsl::shape::Source>(
                    scope: &#rt::dsl::Scope,
                    source: #input_ty,
                    init: impl ::core::ops::FnOnce(&mut #name #type_g, #input_ty::Row),
                ) -> #rt::dsl::write::Insert<#name #type_g> {
                    match (scope.0, source, init) {}
                }
            }
        })
    }
}

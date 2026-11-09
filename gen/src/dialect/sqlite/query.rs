use crate::backend;
use crate::backend::Backend as _;
use crate::dialect::{ScopeCalls, name, param};
use crate::emit;

use crate::dialect::sqlite::{Backend, params};

use syn::visit::Visit as _;

pub(crate) struct Query;

impl Query {
    pub(super) fn marker(ident: &syn::Ident) -> syn::Ident {
        let suffix = name::Name::new(ident);
        let suffix = suffix.as_str();
        quote::format_ident!("__CartelSqliteQuery{suffix}")
    }

    pub(crate) fn expand(f: syn::ItemFn) -> syn::Result<(syn::Ident, proc_macro2::TokenStream)> {
        f.modifiers.require_empty()?;
        backend::Compiled::validate(&f.sig, &f.attrs, "#[query] functions")?;
        let mut names = backend::Names::default();
        names.visit_item_fn(&f);
        let backend::Compiled {
            mut params,
            captures,
            nonnegative: nonneg,
            plan,
            block,
        } = backend::Compiled::build::<Backend>(
            &f.sig.generics,
            f.sig.inputs.into_iter().map(backend::Compiled::fn_arg),
            *f.block,
            &f.sig.output,
        )?;
        let source = names.fresh("source");
        let query = names.fresh("query");
        let catalog = names.fresh("I");
        let input = names.fresh("S");
        let row = names.lifetime("query");
        let conn = names.lifetime("connection");
        let driver = names.lifetime("d");
        let region = names.lifetime("params");
        let proof = {
            let p_body = ScopeCalls::sqlite(block);
            let output = &plan.output;
            let scope = crate::backend::scope();
            let ids = params.iter().map(|(id, _)| id);
            let tys = params.iter().map(|(_, ty)| ty);
            let decl_tys = params.iter().map(|(_, ty)| ty);
            quote::quote! {
                const _: fn(
                    &::cartel_sqlite::dsl::Scope,
                    #( #tys, )*
                ) #output = |
                    #scope: &::cartel_sqlite::dsl::Scope,
                    #( #ids: #decl_tys, )*
                | {
                    #p_body
                };
            }
        };
        param::Param::<true>::apply(params.iter_mut().map(|(_, ty)| ty), &region);
        let bound = params::Params::new(&params, &captures, &nonneg);
        let ids = params.iter().map(|(id, _)| id);
        let run_tys = params.iter().map(|(_, ty)| ty);
        let n_params = captures.len();
        let n_cols = match &plan.dispatch {
            emit::Kind::NoRows => quote::quote! { 0usize },
            emit::Kind::One(t)
            | emit::Kind::First(t)
            | emit::Kind::Aggregate(t)
            | emit::Kind::All(t)
            | emit::Kind::Stream(t) => {
                quote::quote! { <#t as ::cartel_sqlite::row::raw::Decode>::N_COLS }
            }
        };
        let fn_vis = &f.vis;
        let fn_name = &f.sig.ident;
        let attrs = &f.attrs;
        let marker = Self::marker(fn_name);
        let p_ty = bound.ty;
        let p_val = bound.value;

        let parts = &plan.sql_parts;
        let sql = Backend::dialect().text(quote::quote! { &[#(#parts),*] });

        let method = match &plan.dispatch {
            emit::Kind::NoRows => quote::quote! { run_no_rows },
            emit::Kind::One(t) | emit::Kind::Aggregate(t) => quote::quote! { run_one::<#t, _, _> },
            emit::Kind::First(t) => quote::quote! { run_first::<#t, _, _> },
            emit::Kind::All(t) => quote::quote! { run_all::<#t, _, _> },
            emit::Kind::Stream(t) => quote::quote! { stream::<#t, _, _> },
        };
        let body = quote::quote! {
            #query.#method(
                ::cartel_sqlite::Statement::<#catalog, #marker>::of(),
                #p_val,
            )
        };

        let ret = match &plan.dispatch {
            emit::Kind::NoRows => quote::quote! { () },
            emit::Kind::One(t) | emit::Kind::Aggregate(t) => quote::quote! {
                <#t as ::cartel_sqlite::row::raw::Decode>::Borrowed<#row>
            },
            emit::Kind::First(t) => quote::quote! {
                ::core::option::Option<<#t as ::cartel_sqlite::row::raw::Decode>::Borrowed<#row>>
            },
            emit::Kind::All(t) => quote::quote! { ::cartel_sqlite::Rows<#row, #t> },
            emit::Kind::Stream(t) => quote::quote! {
                ::cartel_sqlite::Cursor<
                    #row,
                    #conn,
                    #driver,
                    #t,
                    #marker,
                    #p_ty,
                    #catalog,
                >
            },
        };
        let ret = match &plan.dispatch {
            emit::Kind::Stream(_) => ret,
            _ => quote::quote! {
                impl ::cartel_sqlite::Fiber<
                    #driver,
                    Output = ::core::result::Result<#ret, ::cartel_sqlite::Error>,
                > + use<#row, #conn, #region, #driver, #catalog, #input>
            },
        };

        let tokens = quote::quote! {
            #[doc(hidden)]
            #fn_vis struct #marker;

            impl ::cartel_sqlite::TypedQuery for #marker {
                const SQL: &str = #sql;
                const PARAMETERS: usize = #n_params;
                const COLUMNS: usize = #n_cols;
            }

            #(#attrs)*
            #fn_vis fn #fn_name<
                #row,
                #conn: #row,
                #driver: #conn,
                #region,
                #catalog,
                #input,
            >(
                #source: #input,
                #( #ids: #run_tys, )*
            ) -> #ret
            where
                #catalog: ::cartel_sqlite::Catalog
                    + ::cartel_sqlite::HasQuery<#marker>
                    + #row,
                #input: ::cartel_sqlite::IntoQuery<
                    #row,
                    #conn,
                    #driver,
                    #catalog,
                >,
            {
                let #query = ::cartel_sqlite::IntoQuery::into_query(#source);
                #body
            }

            #proof
        };
        Ok((marker, tokens))
    }
}

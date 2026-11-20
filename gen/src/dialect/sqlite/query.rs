use crate::backend;
use crate::backend::Backend as _;
use crate::dialect::{name, param};
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
        let backend::Compiled {
            mut params,
            captures,
            nonnegative: nonneg,
            plan,
            proof,
            mut names,
        } = backend::Compiled::build::<Backend>(
            &f.sig.generics,
            f.sig.inputs.into_iter().map(backend::Compiled::fn_arg),
            *f.block,
            &f.sig.output,
        )?;
        names.visit_ident(&f.sig.ident);
        let source = names.fresh("source");
        let query = names.fresh("query");
        let catalog = names.fresh("I");
        let input = names.fresh("S");
        let row = names.lifetime("query");
        let conn = names.lifetime("connection");
        let driver = names.lifetime("d");
        let region = names.lifetime("params");
        param::Param::<true>::apply(params.iter_mut().map(|(_, ty)| ty), &region);
        let bound = params::bind(&params, &captures, &nonneg);
        let ids = params.iter().map(|(id, _)| id);
        let run_tys = params.iter().map(|(_, ty)| ty);
        let n_params = captures.len();
        let n_cols = match &plan.dispatch {
            emit::Kind::NoRows(_) => quote::quote! { 0usize },
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
        let parts = &plan.sql_parts;
        let sql = Backend::dialect().text(quote::quote! { &[#(#parts),*] });

        let mut types = quote::quote! { #catalog, #input, };
        let mut arguments = quote::quote! { #( #ids: #run_tys, )* };
        let mut values = quote::quote! { #bound, };
        let mut visitor_bound = None;
        let (method, ret) = match &plan.dispatch {
            emit::Kind::NoRows(_) => (quote::quote! { run_no_rows }, quote::quote! { () }),
            emit::Kind::One(t) | emit::Kind::Aggregate(t) => (
                quote::quote! { run_one::<#t, _, _> },
                quote::quote! { <#t as ::cartel_sqlite::row::raw::Decode>::Borrowed<#row> },
            ),
            emit::Kind::First(t) => (
                quote::quote! { run_first::<#t, _, _> },
                quote::quote! { ::core::option::Option<<#t as ::cartel_sqlite::row::raw::Decode>::Borrowed<#row>> },
            ),
            emit::Kind::All(t) => (
                quote::quote! { run_all::<#t, _, _> },
                quote::quote! { &#row [<#t as ::cartel_sqlite::row::raw::Decode>::Borrowed<#row>] },
            ),
            emit::Kind::Stream(t) => {
                let visitor = names.fresh("Visitor");
                let visit = names.fresh("visit");
                let item = names.lifetime("item");
                types.extend(quote::quote! { #visitor, });
                arguments.extend(quote::quote! { #visit: #visitor, });
                values.extend(quote::quote! { #visit, });
                visitor_bound = Some(quote::quote! {
                    #visitor: for<#item> ::core::ops::FnMut(
                        <#t as ::cartel_sqlite::row::raw::Decode>::Borrowed<#item>
                    ) -> ::core::ops::ControlFlow<()>,
                });
                (quote::quote! { stream::<#t, _, _> }, quote::quote! { () })
            }
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
                #types
            >(
                #source: #input,
                #arguments
            ) -> impl ::cartel_sqlite::Fiber<
                #driver,
                Output = ::core::result::Result<#ret, ::cartel_sqlite::Error>,
            > + use<#row, #conn, #region, #driver, #types>
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
                #visitor_bound
            {
                let #query = ::cartel_sqlite::IntoQuery::into_query(#source);
                #query.#method(::cartel_sqlite::Statement::<#catalog, #marker>::of(), #values)
            }

            #proof
        };
        Ok((marker, tokens))
    }
}

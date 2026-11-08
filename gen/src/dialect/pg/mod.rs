use crate::backend;
use crate::backend::Backend as _;
use crate::dialect::param;
use crate::dialect::pg::types::TypeExt as _;
use crate::emit;
use syn::ext::IdentExt as _;
use syn::visit::Visit as _;

pub(crate) mod group;
pub(crate) mod row;
pub(crate) mod table;
mod types;

pub(crate) struct PgBackend;

impl backend::Backend for PgBackend {
    fn dialect() -> backend::Dialect {
        backend::Dialect {
            rt_crate: syn::parse_quote! { ::cartel_pg },
            kind: backend::Kind::Pg,
        }
    }
}

pub(super) struct GroupQuery {
    pub(super) compiled: backend::Compiled,
    pub(super) group_ty: syn::Type,
    pub(super) group_ident: syn::Ident,
    pub(super) marker: syn::Ident,
    pub(super) method: syn::Ident,
    pub(super) vis: syn::Visibility,
}

impl PgBackend {
    pub(crate) fn expand(input: proc_macro::TokenStream) -> syn::Result<proc_macro2::TokenStream> {
        let decl: group::InstanceDecl = syn::parse(input)?;
        Ok(decl.expand())
    }

    pub(super) fn emit_query(
        q: GroupQuery,
        attrs: &[syn::Attribute],
    ) -> syn::Result<proc_macro2::TokenStream> {
        let GroupQuery {
            compiled: code,
            group_ty: group,
            group_ident: ident,
            marker,
            method,
            vis,
        } = q;
        let mut params = code.params;
        let caps = &code.captures;
        let plan = code.plan;
        let stmt = format!("{ident}.{method}");
        let query = &marker;
        let mut names = backend::Names::default();
        names.visit_type(&group);
        names.visit_ident(query);
        names.visit_return_type(&plan.output);
        names.visit_block(&code.block);
        for (id, ty) in &params {
            names.visit_ident(id);
            names.visit_type(ty);
        }
        let caller = names.fresh("__CartelClient");
        let route_id = names.fresh("__CARTEL_ROUTE_");
        let driver = names.lifetime("__cartel_driver_");
        let loan = names.lifetime("__cartel_pool_");
        let input = names.lifetime("__cartel_input_");
        let client = names.fresh("__cartel_client_");
        let pool = names.fresh("__cartel_pool_");
        let args = names.fresh("__cartel_arguments_");
        let argument = names.fresh("__cartel_argument_");
        let writer = names.fresh("__cartel_writer_");

        if caps.len() > i16::MAX as usize {
            return Err(syn::Error::new(
                method.span(),
                format!(
                    "PostgreSQL query has {} parameters; maximum is {}",
                    caps.len(),
                    i16::MAX
                ),
            ));
        }
        let count = caps.len() as i16;

        let mut oids = Vec::new();
        let mut calls = Vec::new();
        let mut fmts = Vec::new();
        let mut text = false;
        for cap in caps {
            let (fn_idx, (_, ty)) = params
                .iter()
                .enumerate()
                .find(|(_, (id, _))| id.unraw() == cap.unraw())
                .ok_or_else(|| syn::Error::new(cap.span(), "captured ident not in fn params"))?;
            let bind = ty.bind()?;
            let oid = bind.oid;
            oids.push(quote::quote! { #oid });
            fmts.push(bind.format);
            if bind.format == 0 {
                text = true;
            }
            let idx = syn::Index::from(fn_idx);
            let target = match bind.target {
                types::Target::Scalar => quote::format_ident!("scalar"),
                types::Target::Data => quote::format_ident!("data"),
                types::Target::Array => quote::format_ident!("array"),
            };
            let method = bind.method;
            let arg = match bind.arg {
                types::Arg::Move => quote::quote! { #args.#idx },
                types::Arg::Borrow => quote::quote! { #args.#idx.as_ref() },
                types::Arg::UuidBytes => quote::quote! { #args.#idx.into_bytes() },
            };
            calls.push(quote::quote! { #writer.#target().#method(#arg)?; });
        }

        let decl = if text {
            quote::quote! {
                const PARAM_FORMAT_CODES: &[u16] = &[ #( #fmts, )* ];
            }
        } else {
            quote::quote! {}
        };

        let ids = params.iter().map(|(id, _)| id);
        let values = quote::quote! { ( #( #ids, )* ) };

        let parts = &plan.sql_parts;
        let cols = &plan.n_result_cols;
        let (row, value, run) = match &plan.dispatch {
            emit::Kind::One(ty) => (
                Some(ty),
                quote::quote! { ::cartel_pg::api::results::Row<<#caller::Results as ::cartel_pg::api::results::Storage>::Payload, #query> },
                quote::format_ident!("run_one"),
            ),
            emit::Kind::First(ty) => (
                Some(ty),
                quote::quote! { ::core::option::Option<::cartel_pg::api::results::Row<<#caller::Results as ::cartel_pg::api::results::Storage>::Payload, #query>> },
                quote::format_ident!("run_first"),
            ),
            emit::Kind::Aggregate(ty) => (
                Some(ty),
                quote::quote! { ::core::option::Option<::cartel_pg::api::results::Row<<#caller::Results as ::cartel_pg::api::results::Storage>::Payload, #query, true>> },
                quote::format_ident!("run_scalar"),
            ),
            emit::Kind::All(ty) => (
                Some(ty),
                quote::quote! { ::cartel_pg::api::results::Collection<<#caller::Results as ::cartel_pg::api::results::Storage>::Batch, #query> },
                quote::format_ident!("run_all"),
            ),
            emit::Kind::Stream(ty) => (
                Some(ty),
                quote::quote! { () },
                quote::format_ident!("run_stream"),
            ),
            emit::Kind::NoRows => (
                None,
                quote::quote! { () },
                quote::format_ident!("run_no_rows"),
            ),
        };
        let response = quote::quote! {
            ::core::result::Result<#value, ::cartel_pg::reply::Error<#driver, #route_id>>
        };
        let ret = if matches!(plan.dispatch, emit::Kind::Stream(_)) {
            quote::quote! { #caller::Stream<#query> }
        } else {
            quote::quote! {
                impl ::cartel_pg::Fiber<#driver, Output = #response> + use<#driver, #caller, #route_id>
            }
        };
        let call = quote::quote! {
            ::cartel_pg::api::traits::Query::#run::<#query>(#client, #values)
        };
        let (row_ty, shape, codes) = match row {
            Some(ty) => {
                let codes = match ty.formats() {
                    Some(path) => {
                        quote::quote! { const RESULT_FORMAT_CODES: &[u16] = #path; }
                    }
                    None => quote::quote! {},
                };
                let (row, shape) = match (&plan.dispatch, &plan.output) {
                    (emit::Kind::Aggregate(_), syn::ReturnType::Type(_, declared)) => (
                        quote::quote! { <#declared as ::core::iter::IntoIterator>::Item },
                        quote::quote! { ::cartel_pg::schema::result::Rows<true> },
                    ),
                    _ => (
                        quote::quote! { #ty },
                        quote::quote! { ::cartel_pg::schema::result::Rows },
                    ),
                };
                (row, shape, codes)
            }
            None => (
                quote::quote! { () },
                quote::quote! { ::cartel_pg::schema::result::Command },
                quote::quote! {},
            ),
        };

        let text = Self::dialect().text(quote::quote! { &[#(#parts),*] });
        let sql = quote::quote! {
            const SQL: &str = #text;
        };

        let decls: Vec<proc_macro2::TokenStream> = params
            .iter()
            .map(|(id, ty)| quote::quote! { #id: #ty })
            .collect();

        let proof = {
            let body = match (plan.probe_override, code.block) {
                (Some(block), _) => super::ScopeCalls::pg_assign(block),
                (None, block) => super::ScopeCalls::pg(block),
            };
            let output = &plan.output;
            let scope = backend::scope();
            let tys = params.iter().map(|(_, ty)| ty);
            quote::quote! {
                const _: fn(
                    &::cartel_pg::dsl::Scope,
                    #( #tys, )*
                ) #output = |
                    #scope: &::cartel_pg::dsl::Scope,
                    #( #decls, )*
                | {
                    #body
                };
            }
        };
        let batch = if !matches!(plan.dispatch, emit::Kind::Stream(_)) {
            let count = names.fresh("__CARTEL_GROUP_LEN_");
            let tys = params.iter().map(|(_, ty)| ty);
            let (params, values, item) = match params.as_slice() {
                [(_, ty)] => (
                    quote::quote! { [#ty; #count] },
                    quote::quote! { #args.map(|#argument| (#argument,)) },
                    quote::quote! { (#argument,) },
                ),
                _ => (
                    quote::quote! { [(#(#tys,)*); #count] },
                    quote::quote! { #args },
                    quote::quote! { #argument },
                ),
            };
            let grouped = if matches!(plan.dispatch, emit::Kind::One(_)) {
                let method = group::Group::batch_method(&method, "group");
                quote::quote! {
                    #vis fn #method<#driver, #caller, const #route_id: u8, const #count: usize>(
                        #client: #caller,
                        #args: #params,
                    ) -> impl ::cartel_pg::Fiber<
                        #driver,
                        Output = ::core::result::Result<
                            [#value; #count],
                            ::cartel_pg::api::results::GroupFailure<
                                #driver,
                                #value,
                                <#caller::Results as ::cartel_pg::api::results::Storage>::Diagnostics,
                                #count,
                                #route_id,
                            >,
                        >,
                    > + use<#driver, #caller, #route_id, #count>
                    where
                        #caller: ::cartel_pg::api::traits::Query<#driver, #route_id>,
                        #caller::Schema: ::cartel_pg::schema::HasGroup<Self>,
                    {
                        ::cartel_pg::api::traits::Query::run_group::<#query, #count>(
                            #client,
                            #values,
                        )
                    }
                }
            } else {
                quote::quote! {}
            };
            let method = group::Group::batch_method(&method, "each");
            quote::quote! {
                #grouped

                #vis fn #method<#loan, #driver, #caller, const #route_id: u8, const #count: usize>(
                    #client: #caller,
                    #pool: &#loan mut ::cartel_pg::dope_fiber::task::batch::Pool<#driver, #count>,
                    #args: #params,
                ) -> ::cartel_pg::dope_fiber::task::batch::Join<
                    #loan,
                    #driver,
                    impl ::cartel_pg::Fiber<#driver, Output = #response>
                        + use<#driver, #caller, #route_id, #count>,
                    #count,
                >
                where
                    #caller: ::cartel_pg::api::traits::Query<#driver, #route_id>,
                    #caller::Schema: ::cartel_pg::schema::HasGroup<Self>,
                {
                    ::cartel_pg::dope_fiber::task::batch::Join::from_map(
                        #pool,
                        #args,
                        |#argument| ::cartel_pg::api::traits::Query::#run::<#query>(#client, #item),
                    )
                }
            }
        } else {
            quote::quote! {}
        };
        param::Param::<false>::apply(params.iter_mut().map(|(_, ty)| ty), &input);
        let bound = params.iter().map(|(_, ty)| ty);
        let params = quote::quote! { ( #( #bound, )* ) };

        Ok(quote::quote! {
            #vis struct #query;

            impl ::cartel_pg::schema::TypedQuery for #query {
                type Params<#input> = #params;
                type Row = #row_ty;
                type Result = #shape;
                type Group = #group;
                const STATEMENT_NAME: &str = #stmt;
                #sql
                const PARAM_OIDS: &[u32] = &[ #( #oids, )* ];
                const N_PARAMS: i16 = #count;
                const N_RESULT_COLS: u16 = #cols;
                #decl
                #codes

                fn encode(
                    #args: Self::Params<'_>,
                    #writer: &mut ::cartel_pg::protocol::value::encode::BindWriter<'_, '_>,
                ) -> ::core::result::Result<(), ::cartel_pg::protocol::encode::Error> {
                    #( #calls )*
                    ::core::result::Result::Ok(())
                }
            }

            impl #group {
                #batch

                #(#attrs)*
                #vis fn #method<#driver, #caller, const #route_id: u8>(
                    #client: #caller,
                    #( #decls, )*
                ) -> #ret
                where
                    #caller: ::cartel_pg::api::traits::Query<#driver, #route_id>,
                    #caller::Schema: ::cartel_pg::schema::HasGroup<Self>,
                {
                    #call
                }
            }

            #proof
        })
    }
}

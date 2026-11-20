use crate::backend;
use crate::backend::Backend as _;
use crate::dialect::param;
use crate::dialect::pg::types::TypeExt as _;
use crate::emit;
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

pub(super) struct GroupQuery<'a> {
    pub(super) compiled: backend::Compiled<'a>,
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
        q: GroupQuery<'_>,
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
        let input = names.lifetime("__cartel_input_");
        let executor = quote::quote! {
            <#caller as ::cartel_pg::api::traits::IntoQuery<#driver, #route_id>>::Query
        };
        let query_trait = quote::quote! { ::cartel_pg::api::traits::Query<#driver, #route_id> };
        let client = names.fresh("__cartel_client_");
        let args = names.fresh("__cartel_arguments_");

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
        let single = params.len() == 1;
        let input_shape = |fields: proc_macro2::TokenStream| {
            if single { fields } else { quote::quote! { (#fields) } }
        };

        let mut oids = Vec::new();
        let mut arguments = Vec::new();
        let mut fmts = Vec::new();
        for &(fn_idx, _) in caps {
            let (_, ty) = &params[fn_idx];
            let meta = ty.param()?;
            oids.push(
                quote::quote! { <#meta as ::cartel_pg::protocol::value::encode::Param>::OID },
            );
            fmts.push(
                quote::quote! { <#meta as ::cartel_pg::protocol::value::encode::Param>::FORMAT },
            );
            let idx = syn::Index::from(fn_idx);
            let arg = match (single, ty) {
                (true, syn::Type::Reference(_)) => quote::quote! { &**#args },
                (true, _) => quote::quote! { #args },
                (false, syn::Type::Reference(_)) => quote::quote! { &*#args.#idx },
                (false, _) => quote::quote! { &#args.#idx },
            };
            arguments.push(arg);
        }

        let arguments = arguments
            .into_iter()
            .rev()
            .fold(quote::quote! { () }, |tail, head| {
                quote::quote! { (#head, #tail) }
            });

        let ids = params.iter().map(|(id, _)| id);
        let values = input_shape(quote::quote! { #(#ids),* });

        let parts = &plan.sql_parts;
        let cols = &plan.n_result_cols;
        let (row, value, run) = match &plan.dispatch {
            emit::Kind::One(ty) => (
                Some(ty),
                quote::quote! { ::cartel_pg::api::results::Row<<#executor as #query_trait>::Payload, #query> },
                quote::quote! { run_one::<#query> },
            ),
            emit::Kind::First(ty) | emit::Kind::Aggregate(ty) => {
                let scalar = matches!(plan.dispatch, emit::Kind::Aggregate(_));
                (
                    Some(ty),
                    quote::quote! { ::core::option::Option<::cartel_pg::api::results::Row<<#executor as #query_trait>::Payload, #query, #scalar>> },
                    quote::quote! { run_first::<#query, #scalar> },
                )
            }
            emit::Kind::All(ty) => (
                Some(ty),
                quote::quote! { ::cartel_pg::api::results::Collection<<#executor as #query_trait>::Batch, #query> },
                quote::quote! { run_all::<#query> },
            ),
            emit::Kind::Stream(ty) => (
                Some(ty),
                quote::quote! { () },
                quote::quote! { run_stream::<#query> },
            ),
            emit::Kind::NoRows(required) => (
                None,
                quote::quote! { () },
                quote::quote! { run_command::<#query, #required> },
            ),
        };
        let response = quote::quote! {
            ::core::result::Result<#value, ::cartel_pg::reply::Error<#driver, #route_id>>
        };
        let ret = if matches!(plan.dispatch, emit::Kind::Stream(_)) {
            quote::quote! { <#executor as #query_trait>::Stream<#query> }
        } else {
            quote::quote! {
                impl ::cartel_pg::Fiber<#driver, Output = #response> + use<#driver, #caller, #route_id>
            }
        };
        let call = quote::quote! {
            ::cartel_pg::api::traits::Query::#run(
                ::cartel_pg::api::traits::IntoQuery::into_query(#client), #values
            )
        };
        let (row_ty, shape) = match row {
            Some(ty) => match (&plan.dispatch, plan.output.as_ref()) {
                (emit::Kind::Aggregate(_), syn::ReturnType::Type(_, declared)) => (
                    quote::quote! { <#declared as ::core::iter::IntoIterator>::Item },
                    quote::quote! { ::cartel_pg::schema::result::Rows<true> },
                ),
                _ => (
                    quote::quote! { #ty },
                    quote::quote! { ::cartel_pg::schema::result::Rows },
                ),
            },
            None => (
                quote::quote! { () },
                quote::quote! { ::cartel_pg::schema::result::Command },
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
        param::Param::<false>::apply(params.iter_mut().map(|(_, ty)| ty), &input);
        let bound = params.iter().map(|(_, ty)| ty);
        let params = input_shape(quote::quote! { #(#bound),* });

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
                const PARAM_FORMAT_CODES: &[u16] =
                    ::cartel_pg::protocol::encode::format_codes::<true>(&[#(#fmts,)*]);
                const RESULT_FORMAT_CODES: &[u16] = {
                    const CODES: [u16; <#query as ::cartel_pg::schema::TypedQuery>::N_RESULT_COLS as usize] =
                        ::cartel_pg::protocol::encode::result_formats(&<#row_ty as ::cartel_pg::schema::desc::Validate>::FORMATS);
                    ::cartel_pg::protocol::encode::format_codes::<false>(&CODES)
                };

                #[inline(always)]
                fn bind(
                    #args: &Self::Params<'_>,
                ) -> impl ::cartel_pg::protocol::value::encode::Arguments {
                    #arguments
                }
            }

            impl #group {
                #(#attrs)*
                #vis fn #method<#driver, #caller, const #route_id: u8>(
                    #client: #caller,
                    #( #decls, )*
                ) -> #ret
                where
                    #caller: ::cartel_pg::api::traits::IntoQuery<#driver, #route_id>,
                    #executor: #query_trait,
                    <#executor as #query_trait>::Schema: ::cartel_pg::schema::HasGroup<Self>,
                {
                    #call
                }
            }

            #proof
        })
    }
}

use proc_macro::TokenStream;
use quote::{format_ident, quote};
use syn::{Ident, ItemImpl, Type, Visibility};

use crate::backend::{Backend, Compiled, Dialect, Placeholder, TableSpec};
use crate::emit::{DecodeKind, DispatchKind};
use crate::pg::type_meta::{ArgForm, TypeExt};
use crate::util::{FnParamsExt, SqlIdent};

mod group;
mod type_meta;

pub(super) struct PgBackend;

impl PgBackend {
    fn param_cast(ty: &Type) -> Option<&'static str> {
        ty.param_info().ok().and_then(|info| info.cast)
    }
}

impl Backend for PgBackend {
    fn dialect() -> Dialect {
        Dialect {
            rt_crate: syn::parse_quote! { ::cartel_pg },
            placeholder: Placeholder::Dollar,
            unsupported_ops: &["glob"],
            param_cast: Self::param_cast,
        }
    }

    fn emit_table(spec: TableSpec<'_>, dialect: &Dialect) -> syn::Result<proc_macro2::TokenStream> {
        Self::emit_table(spec, dialect)
    }
}

pub(super) struct GroupQuery {
    pub(super) compiled: Compiled,
    pub(super) group_ty: Type,
    pub(super) group_ident: Ident,
    pub(super) method: Ident,
    pub(super) vis: Visibility,
}

impl PgBackend {
    pub(super) fn expand_query_group(block: ItemImpl) -> syn::Result<proc_macro2::TokenStream> {
        group::QueryGroupItem::expand(block)
    }

    pub(super) fn expand_instance(input: TokenStream) -> syn::Result<proc_macro2::TokenStream> {
        let decl: group::InstanceDecl = syn::parse(input)?;
        Ok(decl.expand())
    }

    pub(super) fn emit_grouped_query(q: &GroupQuery) -> syn::Result<proc_macro2::TokenStream> {
        let GroupQuery {
            compiled,
            group_ty,
            group_ident,
            method,
            vis,
        } = q;
        let Compiled {
            param_ids,
            param_tys,
            captures,
            plan,
            ..
        } = compiled;
        let fn_vis = vis;
        let fn_name = method;
        let stmt_name = format!("{group_ident}.{method}");
        let q_struct = format_ident!("__CartelPgQuery_{}_{}", group_ident, method);

        let mut param_oids = Vec::new();
        let mut bind_calls = Vec::new();
        let mut param_format_codes = Vec::new();
        let mut any_text_param = false;
        for cap in captures {
            let fn_idx = param_ids.index_of(cap)?;
            let ty = &param_tys[fn_idx];
            let info = ty.param_info()?;
            let oid = info.oid;
            param_oids.push(quote! { #oid });
            param_format_codes.push(info.format_code);
            if info.format_code == 0 {
                any_text_param = true;
            }
            let idx = syn::Index::from(fn_idx);
            let write_method = format_ident!("{}", info.write_method);
            let arg_expr = match info.arg_form {
                ArgForm::Move => quote! { __params.#idx },
                ArgForm::Borrow => quote! { __params.#idx.as_ref() },
                ArgForm::UuidIntoBytes => quote! { __params.#idx.into_bytes() },
                ArgForm::LtreeAsStr => quote! { __params.#idx.as_str() },
            };
            bind_calls.push(quote! { __w.#write_method(#arg_expr); });
        }

        let n_params_u16 = captures.len() as u16;
        let param_format_codes_const = if any_text_param {
            quote! {
                const PARAM_FORMAT_CODES: &'static [u16] = &[ #( #param_format_codes, )* ];
            }
        } else {
            quote! {}
        };

        let n_fn_params = param_ids.len();
        let params_tuple_ty = if n_fn_params == 0 {
            quote! { () }
        } else {
            let lifetime_bound: Vec<Type> = param_tys.iter().map(TypeExt::rewrite_to_p).collect();
            quote! { ( #( #lifetime_bound, )* ) }
        };
        let arg_uses = param_ids.iter().map(|id| quote! { #id });
        let params_construct = if n_fn_params == 0 {
            quote! { () }
        } else {
            quote! { ( #( #arg_uses, )* ) }
        };

        let row_ty = &plan.row_ty;
        let sql_parts = &plan.sql_parts;
        let n_result_cols = &plan.n_result_cols;
        let decode_body = match &plan.decode {
            DecodeKind::Row(t) => quote! { <#t as ::cartel_pg::Row>::decode(__r) },
            DecodeKind::Unit => quote! { ::core::result::Result::Ok(()) },
        };
        let result_format_codes_const = match row_ty.result_format_codes() {
            Some(path) => quote! { const RESULT_FORMAT_CODES: &'static [u16] = #path; },
            None => quote! {},
        };

        let (wrapper_ret, dispatch_call, each_elem_out) = match &plan.dispatch {
            DispatchKind::One(t) => (
                quote! { ::cartel_pg::Fiber<'__d, impl ::core::future::Future<Output = ::core::result::Result<#t, ::cartel_pg::Error>> + use<'__d, __R, __I, __S, __E>> },
                quote! { __client.run_one::<#q_struct>(#params_construct) },
                quote! { ::core::result::Result<#t, ::cartel_pg::Error> },
            ),
            DispatchKind::First(t) => (
                quote! { ::cartel_pg::Fiber<'__d, impl ::core::future::Future<Output = ::core::result::Result<::core::option::Option<#t>, ::cartel_pg::Error>> + use<'__d, __R, __I, __S, __E>> },
                quote! { __client.run_first::<#q_struct>(#params_construct) },
                quote! { ::core::result::Result<::core::option::Option<#t>, ::cartel_pg::Error> },
            ),
            DispatchKind::All(t) => (
                quote! { ::cartel_pg::Fiber<'__d, impl ::core::future::Future<Output = ::core::result::Result<::std::vec::Vec<#t>, ::cartel_pg::Error>> + use<'__d, __R, __I, __S, __E>> },
                quote! { __client.run_all::<#q_struct>(#params_construct) },
                quote! { ::core::result::Result<::std::vec::Vec<#t>, ::cartel_pg::Error> },
            ),
            DispatchKind::Stream(t) => (
                quote! { ::cartel_pg::RunStream<'__d, __I, __S, __E, #t> },
                quote! { __client.run_stream::<#q_struct>(#params_construct) },
                quote! { ::cartel_pg::RunStream<'__d, __I, __S, __E, #t> },
            ),
            DispatchKind::NoRows => (
                quote! { ::cartel_pg::Fiber<'__d, ::cartel_pg::Dispatched<'__d, __I, __S, __E, ::cartel_pg::ExtractUnit>> },
                quote! { __client.run_no_rows::<#q_struct>(#params_construct) },
                quote! { ::core::result::Result<(), ::cartel_pg::Error> },
            ),
        };

        let sql_const = quote! {
            const SQL: &'static str = {
                const __PARTS: &[&str] = &[ #( #sql_parts ),* ];
                const __LEN: usize = ::cartel_pg::__internal::concat_len(__PARTS);
                const __BYTES: [u8; __LEN] = ::cartel_pg::__internal::concat::<__LEN>(__PARTS);
                match ::core::str::from_utf8(&__BYTES) {
                    ::core::result::Result::Ok(s) => s,
                    ::core::result::Result::Err(_) => panic!("cartel_pg: SQL build failed"),
                }
            };
        };

        let each_emit = match &plan.dispatch {
            DispatchKind::Stream(_) => quote! {},
            _ => {
                let each_fn_name = format_ident!("{}_each", fn_name);
                let (each_arg_ty, each_arg_pat) = if n_fn_params == 1 {
                    let ty = &param_tys[0];
                    let id = &param_ids[0];
                    (quote! { &[#ty] }, quote! { #id })
                } else {
                    let tuple_elems = param_tys.iter().map(|t| quote! { #t });
                    let pat_elems = param_ids.iter().map(|id| quote! { #id });
                    (
                        quote! { &[( #( #tuple_elems, )* )] },
                        quote! { ( #( #pat_elems, )* ) },
                    )
                };
                let each_call_args = param_ids
                    .iter()
                    .enumerate()
                    .map(|(i, _)| {
                        let idx = syn::Index::from(i);
                        quote! { __captured.#idx }
                    })
                    .collect::<Vec<_>>();
                let each_capture = param_ids
                    .iter()
                    .map(|id| quote! { ::core::clone::Clone::clone(#id) });
                quote! {
                    #fn_vis fn #each_fn_name<'__d, __R, __I, __S, __E>(
                        __client: &__R,
                        __args: #each_arg_ty,
                    ) -> ::cartel_pg::Batch<
                        impl ::core::future::Future<Output = #each_elem_out>,
                    >
                    where
                        __R: ::cartel_pg::PgOps<'__d, __I, __S, __E>,
                        __I: ::cartel_pg::QuerySet + ::cartel_pg::HasGroup<#group_ty> + '__d,
                        __S: ::dope::manifold::connector::source::Dialer<__E::Transport> + '__d,
                        __E: ::dope::manifold::env::Env + '__d,
                        __E::Transport: ::dope::transport::Transport<Addr: ::core::clone::Clone>,
                    {
                        let __holding = __client.holding();
                        let __pin = __client.batch_pin();
                        ::cartel_pg::Batch::new(
                            __args
                                .iter()
                                .map(|#each_arg_pat| {
                                    let __captured = ( #( #each_capture, )* );
                                    let __runner = ::cartel_pg::Runner::new(__holding.clone(), __pin);
                                    ::cartel_pg::Lazy::new(move || {
                                        <#group_ty>::#fn_name(&__runner, #( #each_call_args, )*)
                                    })
                                })
                                .collect::<::std::vec::Vec<_>>(),
                        )
                    }
                }
            }
        };
        let arg_decls: Vec<proc_macro2::TokenStream> = param_ids
            .iter()
            .zip(param_tys.iter())
            .map(|(id, ty)| quote! { #id: #ty })
            .collect();

        let probe_fn_name = format_ident!("__cartel_pg_probe_{}_{}", group_ident, method);
        let probe_arg_decls = arg_decls.clone();
        let probe_body_block = &compiled.block;
        let probe_returns_unit = match &compiled.output {
            syn::ReturnType::Default => true,
            syn::ReturnType::Type(_, t) => {
                matches!(t.as_ref(), Type::Tuple(tup) if tup.elems.is_empty())
            }
        };
        let (probe_ret_arrow, probe_body) = match &plan.probe_override {
            Some(ts) => (quote! {}, quote! { let _ = { #ts }; }),
            None if probe_returns_unit => (quote! {}, quote! { let _ = #probe_body_block; }),
            None => {
                let ret_ty = match &compiled.output {
                    syn::ReturnType::Type(_, t) => t.clone(),
                    syn::ReturnType::Default => unreachable!(),
                };
                (quote! { -> #ret_ty }, quote! { #probe_body_block })
            }
        };
        let probe_emit = quote! {
            #[doc(hidden)]
            #[allow(
                dead_code, unreachable_code, unused_variables, unused_assignments,
                non_snake_case, clippy::let_unit_value, clippy::unused_unit,
                clippy::no_effect
            )]
            fn #probe_fn_name(#( #probe_arg_decls, )*) #probe_ret_arrow {
                #probe_body
            }
        };

        Ok(quote! {
            #[allow(non_camel_case_types)]
            #fn_vis struct #q_struct;

            impl ::cartel_pg::TypedQuery for #q_struct {
                type Params<'p> = #params_tuple_ty;
                type Row = #row_ty;
                type Group = #group_ty;
                const STATEMENT_NAME: &'static str = #stmt_name;
                #sql_const
                const PARAM_OIDS: &'static [u32] = &[ #( #param_oids, )* ];
                const N_PARAMS: u16 = #n_params_u16;
                const N_RESULT_COLS: u16 = #n_result_cols;
                #param_format_codes_const
                #result_format_codes_const

                fn encode_params<__Sink: ::cartel_pg::Sink>(
                    __params: Self::Params<'_>,
                    __w: &mut ::cartel_pg::BindWriter<'_, __Sink>,
                ) {
                    #( #bind_calls )*
                }

                fn decode_row(__r: &mut ::cartel_pg::RowReader<'_>) -> ::core::result::Result<Self::Row, ::cartel_pg::Error> {
                    #decode_body
                }
            }

            impl #group_ty {
                #fn_vis fn #fn_name<'__d, __R, __I, __S, __E>(
                    __client: &__R,
                    #( #arg_decls, )*
                ) -> #wrapper_ret
                where
                    __R: ::cartel_pg::PgOps<'__d, __I, __S, __E>,
                    __I: ::cartel_pg::QuerySet + ::cartel_pg::HasGroup<#group_ty> + '__d,
                    __S: ::dope::manifold::connector::source::Dialer<__E::Transport> + '__d,
                    __E: ::dope::manifold::env::Env + '__d,
                    __E::Transport: ::dope::transport::Transport<Addr: ::core::clone::Clone>,
                {
                    #dispatch_call
                }

                #each_emit
            }

            #probe_emit
        })
    }

    fn emit_table(
        spec: TableSpec<'_>,
        _dialect: &Dialect,
    ) -> syn::Result<proc_macro2::TokenStream> {
        let TableSpec {
            input,
            table_name,
            fields,
            pk_cols,
        } = spec;
        let name = &input.ident;
        let (impl_g, type_g, where_g) = input.generics.split_for_impl();
        let table_name_sql = table_name.quote_if_needed();

        let mut field_inits = Vec::new();
        let mut col_names: Vec<String> = Vec::new();
        let mut result_format_codes: Vec<u16> = Vec::new();
        let mut any_text_col = false;
        for f in fields {
            let fname = f.ident.as_ref().expect("named field");
            let read_call = f.ty.row_read()?;
            field_inits.push(quote! { #fname: #read_call, });
            col_names.push(fname.to_string());
            let fmt = f.ty.column_format_code();
            if fmt == 0 {
                any_text_col = true;
            }
            result_format_codes.push(fmt);
        }
        let result_format_codes_const = if any_text_col {
            quote! {
                #[doc(hidden)]
                pub const __CARTEL_RESULT_FORMAT_CODES: &'static [u16] = &[ #( #result_format_codes, )* ];
            }
        } else {
            quote! {
                #[doc(hidden)]
                pub const __CARTEL_RESULT_FORMAT_CODES: &'static [u16] = &[1u16];
            }
        };

        let col_names_sql: Vec<String> = col_names.iter().map(|c| c.quote_if_needed()).collect();
        let select_cols = col_names_sql.join(",");
        let qualified_cols: Vec<String> = col_names_sql
            .iter()
            .map(|c| format!("{table_name_sql}.{c}"))
            .collect();
        let qualified_select_cols = qualified_cols.join(",");
        let pk_const = pk_cols
            .iter()
            .map(|c| c.quote_if_needed())
            .collect::<Vec<_>>()
            .join(",");
        let n_cols = col_names.len() as u16;

        let slices_ident = format_ident!("{name}__Slices");
        let mut slice_field_decls = Vec::new();
        let mut all_slices_supported = true;
        for f in fields {
            let fname = f.ident.as_ref().expect("named field");
            match f.ty.slice_field_type() {
                Ok(ty) => slice_field_decls.push(quote! { pub #fname: #ty }),
                Err(_) => {
                    all_slices_supported = false;
                    break;
                }
            }
        }
        let slices_emit = if all_slices_supported {
            quote! {
                #[doc(hidden)]
                #[allow(non_camel_case_types, dead_code)]
                pub struct #slices_ident<'__a> {
                    #(#slice_field_decls,)*
                    #[doc(hidden)]
                    pub __cartel_pg_lifetime_marker: ::core::marker::PhantomData<&'__a ()>,
                }
            }
        } else {
            quote! {}
        };
        let (insert_each_lifetime, insert_each_arg_ty) = if all_slices_supported {
            (quote! { <'__a> }, quote! { &mut #slices_ident<'__a> })
        } else {
            (quote! {}, quote! { &mut #name #type_g })
        };

        Ok(quote! {
            #slices_emit

            impl #impl_g ::cartel_pg::Row for #name #type_g #where_g {
                fn decode(__r: &mut ::cartel_pg::RowReader<'_>) -> ::core::result::Result<Self, ::cartel_pg::Error> {
                    ::core::result::Result::Ok(Self {
                        #(#field_inits)*
                    })
                }
            }

            impl #impl_g #name #type_g #where_g {
                #[doc(hidden)]
                pub const __CARTEL_TABLE: &'static str = #table_name_sql;
                #[doc(hidden)]
                pub const __CARTEL_SELECT_COLS: &'static str = #select_cols;
                #[doc(hidden)]
                pub const __CARTEL_SELECT_COLS_QUALIFIED: &'static str = #qualified_select_cols;
                #[doc(hidden)]
                pub const __CARTEL_PK_COL: &'static str = #pk_const;
                #[doc(hidden)]
                pub const __CARTEL_N_COLS: u16 = #n_cols;
                #result_format_codes_const
            }

            #[allow(unused_variables, clippy::unused_self)]
            impl #impl_g #name #type_g #where_g {
                pub fn filter(_f: impl ::core::ops::FnOnce(#name #type_g) -> bool)
                    -> ::cartel_pg::FilterBuilder<#name #type_g>
                {
                    ::core::unreachable!("cartel_pg: Table::filter only valid inside #[query] body")
                }

                pub fn join<__T>(
                    _f: impl ::core::ops::FnOnce(#name #type_g, __T) -> bool,
                ) -> ::cartel_pg::JoinBuilder<#name #type_g, __T> {
                    ::core::unreachable!("cartel_pg: Table::join only valid inside #[query] body")
                }
                pub fn left_join<__T>(
                    _f: impl ::core::ops::FnOnce(#name #type_g, __T) -> bool,
                ) -> ::cartel_pg::JoinBuilder<#name #type_g, __T> {
                    ::core::unreachable!("cartel_pg: Table::left_join only valid inside #[query] body")
                }
                pub fn right_join<__T>(
                    _f: impl ::core::ops::FnOnce(#name #type_g, __T) -> bool,
                ) -> ::cartel_pg::JoinBuilder<#name #type_g, __T> {
                    ::core::unreachable!("cartel_pg: Table::right_join only valid inside #[query] body")
                }
                pub fn full_join<__T>(
                    _f: impl ::core::ops::FnOnce(#name #type_g, __T) -> bool,
                ) -> ::cartel_pg::JoinBuilder<#name #type_g, __T> {
                    ::core::unreachable!("cartel_pg: Table::full_join only valid inside #[query] body")
                }
                pub fn lateral_join<__T>(
                    _f: impl ::core::ops::FnOnce(#name #type_g) -> ::cartel_pg::FilterBuilder<__T>,
                ) -> ::cartel_pg::JoinBuilder<#name #type_g, __T> {
                    ::core::unreachable!("cartel_pg: Table::lateral_join only valid inside #[query] body")
                }
                pub fn lateral_left_join<__T>(
                    _f: impl ::core::ops::FnOnce(#name #type_g) -> ::cartel_pg::FilterBuilder<__T>,
                ) -> ::cartel_pg::JoinBuilder<#name #type_g, __T> {
                    ::core::unreachable!("cartel_pg: Table::lateral_left_join only valid inside #[query] body")
                }

                pub fn filter_each<__D, __F>(
                    _cols: __D,
                    _pred: __F,
                ) -> ::cartel_pg::UpdateEachBuilder<#name #type_g, __D>
                where
                    __D: ::cartel_pg::EachCols,
                    __F: ::cartel_pg::EachClosure<__D, #name #type_g>,
                {
                    ::core::unreachable!("cartel_pg: Table::filter_each only valid inside #[query] body")
                }

                pub fn insert(_f: impl ::core::ops::FnOnce(&mut #name #type_g))
                    -> ::cartel_pg::InsertBuilder<#name #type_g>
                {
                    ::core::unreachable!("cartel_pg: Table::insert only valid inside #[query] body")
                }
                pub fn insert_each #insert_each_lifetime (
                    _f: impl ::core::ops::FnOnce(#insert_each_arg_ty),
                ) -> ::cartel_pg::InsertBuilder<#name #type_g> {
                    ::core::unreachable!("cartel_pg: Table::insert_each only valid inside #[query] body")
                }
                pub fn insert_from<__S: ::cartel_pg::dsl::SourceRow>(
                    _s: __S,
                    _f: impl ::core::ops::FnOnce(&mut #name #type_g, __S::Row),
                ) -> ::cartel_pg::InsertBuilder<#name #type_g> {
                    ::core::unreachable!("cartel_pg: Table::insert_from only valid inside #[query] body")
                }
            }
        })
    }
}

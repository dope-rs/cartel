use quote::{format_ident, quote};
use syn::spanned::Spanned;
use syn::{ItemFn, ReturnType, Type};

use crate::backend::{Backend, Compiled, Dialect, Placeholder, TableSpec};
use crate::emit::{DecodeKind, DispatchKind};
use crate::util::SqlIdent;

const UNSUPPORTED_OPS: &[&str] = &[
    "ilike",
    "not_ilike",
    "pg_contains",
    "pg_overlaps",
    "regex_match",
    "regex_imatch",
    "not_regex_match",
    "not_regex_imatch",
    "is_descendant_of",
    "is_ancestor_of",
    "fts_match",
    "in_",
    "not_in",
];

pub(super) struct SqliteBackend;

impl SqliteBackend {
    fn no_param_cast(_: &syn::Type) -> Option<&'static str> {
        None
    }
}

impl Backend for SqliteBackend {
    fn dialect() -> Dialect {
        Dialect {
            rt_crate: syn::parse_quote! { ::cartel_sqlite },
            placeholder: Placeholder::Question,
            unsupported_ops: UNSUPPORTED_OPS,
            param_cast: Self::no_param_cast,
        }
    }

    fn emit_table(spec: TableSpec<'_>, dialect: &Dialect) -> syn::Result<proc_macro2::TokenStream> {
        Self::emit_table(spec, dialect)
    }
}

impl SqliteBackend {
    pub(super) fn query_free(f: ItemFn, no_probe: bool) -> syn::Result<proc_macro2::TokenStream> {
        if f.sig.asyncness.is_some() {
            return Err(syn::Error::new(
                f.sig.span(),
                "#[query] functions must not be async",
            ));
        }
        let compiled = Compiled::build::<Self>(
            &f.sig.generics,
            f.sig.inputs.iter().map(Compiled::fn_arg),
            &f.block,
            &f.sig.output,
            f.sig.span(),
        )?;
        Self::emit_query(&f, &compiled, no_probe)
    }

    fn emit_query(
        f: &ItemFn,
        compiled: &Compiled,
        no_probe: bool,
    ) -> syn::Result<proc_macro2::TokenStream> {
        let Compiled {
            param_ids,
            param_tys,
            captures,
            plan,
            ..
        } = compiled;

        let fn_vis = &f.vis;
        let fn_name = &f.sig.ident;
        let arg_decls = param_ids
            .iter()
            .zip(param_tys.iter())
            .map(|(id, ty)| quote! { #id: #ty })
            .collect::<Vec<_>>();
        let bind_idents = captures.iter().map(|c| quote! { #c }).collect::<Vec<_>>();

        let sql_parts = &plan.sql_parts;
        let sql_const = quote! {
            const __SQL: &str = {
                const __PARTS: &[&str] = &[ #( #sql_parts ),* ];
                const __LEN: usize = ::cartel_sqlite::__internal::concat_len(__PARTS);
                const __BYTES: [u8; __LEN] = ::cartel_sqlite::__internal::concat::<__LEN>(__PARTS);
                match ::core::str::from_utf8(&__BYTES) {
                    ::core::result::Result::Ok(s) => s,
                    ::core::result::Result::Err(_) => panic!("cartel_sqlite: SQL build failed"),
                }
            };
        };

        let decode_one = match &plan.decode {
            DecodeKind::Row(t) => quote! { <#t as ::cartel_sqlite::Decode>::decode(__r)? },
            DecodeKind::Unit => quote! { () },
        };

        let body = match &plan.dispatch {
            DispatchKind::NoRows => quote! {
                #sql_const
                let mut __stmt = __conn.prepare_cached(__SQL)?;
                __stmt.execute(::cartel_sqlite::params![ #( #bind_idents ),* ])?;
                ::core::result::Result::Ok(())
            },
            DispatchKind::One(_) => quote! {
                #sql_const
                let mut __stmt = __conn.prepare_cached(__SQL)?;
                let mut __rows = __stmt.query(::cartel_sqlite::params![ #( #bind_idents ),* ])?;
                match __rows.next()? {
                    ::core::option::Option::Some(__r) => ::core::result::Result::Ok(#decode_one),
                    ::core::option::Option::None => ::core::result::Result::Err(::cartel_sqlite::Error::QueryReturnedNoRows),
                }
            },
            DispatchKind::First(_) => quote! {
                #sql_const
                let mut __stmt = __conn.prepare_cached(__SQL)?;
                let mut __rows = __stmt.query(::cartel_sqlite::params![ #( #bind_idents ),* ])?;
                match __rows.next()? {
                    ::core::option::Option::Some(__r) => ::core::result::Result::Ok(::core::option::Option::Some(#decode_one)),
                    ::core::option::Option::None => ::core::result::Result::Ok(::core::option::Option::None),
                }
            },
            DispatchKind::All(_) | DispatchKind::Stream(_) => quote! {
                #sql_const
                let mut __stmt = __conn.prepare_cached(__SQL)?;
                let mut __rows = __stmt.query(::cartel_sqlite::params![ #( #bind_idents ),* ])?;
                let mut __out = ::std::vec::Vec::new();
                while let ::core::option::Option::Some(__r) = __rows.next()? {
                    __out.push(#decode_one);
                }
                ::core::result::Result::Ok(__out)
            },
        };

        let ret = match &plan.dispatch {
            DispatchKind::NoRows => quote! { ::cartel_sqlite::Result<()> },
            DispatchKind::One(t) => quote! { ::cartel_sqlite::Result<#t> },
            DispatchKind::First(t) => {
                quote! { ::cartel_sqlite::Result<::core::option::Option<#t>> }
            }
            DispatchKind::All(t) | DispatchKind::Stream(t) => {
                quote! { ::cartel_sqlite::Result<::std::vec::Vec<#t>> }
            }
        };

        let probe_emit = if no_probe {
            quote! {}
        } else {
            let probe_fn_name = format_ident!("__cartel_sqlite_probe_{}", fn_name);
            let probe_body_block = &f.block;
            let probe_returns_unit = match &f.sig.output {
                ReturnType::Default => true,
                ReturnType::Type(_, t) => {
                    matches!(t.as_ref(), Type::Tuple(tup) if tup.elems.is_empty())
                }
            };
            let (probe_ret_arrow, probe_body) = if probe_returns_unit {
                (quote! {}, quote! { let _ = #probe_body_block; })
            } else {
                let ret_ty = match &f.sig.output {
                    ReturnType::Type(_, t) => t.clone(),
                    ReturnType::Default => unreachable!(),
                };
                (quote! { -> #ret_ty }, quote! { #probe_body_block })
            };
            quote! {
                #[doc(hidden)]
                #[allow(
                    dead_code, unreachable_code, unused_variables,
                    non_snake_case, clippy::let_unit_value, clippy::unused_unit,
                    clippy::no_effect
                )]
                fn #probe_fn_name(#( #arg_decls, )*) #probe_ret_arrow {
                    #probe_body
                }
            }
        };

        Ok(quote! {
            #fn_vis fn #fn_name(
                __conn: &::cartel_sqlite::Connection,
                #( #arg_decls, )*
            ) -> #ret {
                #body
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

        let mut field_decodes = Vec::new();
        let mut n_cols_acc = quote! { 0usize };
        let mut col_names: Vec<String> = Vec::new();
        for f in fields {
            let fname = f.ident.as_ref().expect("named field");
            let fty = &f.ty;
            field_decodes.push(quote! {
                #fname: <#fty as ::cartel_sqlite::Decode>::decode_at(__r, __off + (#n_cols_acc))?,
            });
            n_cols_acc = quote! { #n_cols_acc + <#fty as ::cartel_sqlite::Decode>::N_COLS };
            col_names.push(fname.to_string());
        }

        let col_names_sql: Vec<String> = col_names.iter().map(|c| c.quote_if_needed()).collect();
        let select_cols = col_names_sql.join(",");
        let qualified_select_cols = col_names_sql
            .iter()
            .map(|c| format!("{table_name_sql}.{c}"))
            .collect::<Vec<_>>()
            .join(",");
        let pk_const = pk_cols
            .iter()
            .map(|c| c.quote_if_needed())
            .collect::<Vec<_>>()
            .join(",");
        let n_cols = col_names.len() as u16;

        Ok(quote! {
            impl #impl_g ::cartel_sqlite::Decode for #name #type_g #where_g {
                const N_COLS: usize = #n_cols_acc;
                fn decode_at(__r: &::cartel_sqlite::Row<'_>, __off: usize) -> ::cartel_sqlite::Result<Self> {
                    ::core::result::Result::Ok(Self { #( #field_decodes )* })
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
            }

            #[allow(unused_variables, clippy::unused_self)]
            impl #impl_g #name #type_g #where_g {
                pub fn filter(_f: impl ::core::ops::FnOnce(#name #type_g) -> bool)
                    -> ::cartel_sqlite::FilterBuilder<#name #type_g>
                {
                    ::core::unreachable!("cartel_sqlite: Table::filter only valid inside #[query] body")
                }

                pub fn join<__T>(
                    _f: impl ::core::ops::FnOnce(#name #type_g, __T) -> bool,
                ) -> ::cartel_sqlite::JoinBuilder<#name #type_g, __T> {
                    ::core::unreachable!("cartel_sqlite: Table::join only valid inside #[query] body")
                }
                pub fn left_join<__T>(
                    _f: impl ::core::ops::FnOnce(#name #type_g, __T) -> bool,
                ) -> ::cartel_sqlite::JoinBuilder<#name #type_g, __T> {
                    ::core::unreachable!("cartel_sqlite: Table::left_join only valid inside #[query] body")
                }
                pub fn right_join<__T>(
                    _f: impl ::core::ops::FnOnce(#name #type_g, __T) -> bool,
                ) -> ::cartel_sqlite::JoinBuilder<#name #type_g, __T> {
                    ::core::unreachable!("cartel_sqlite: Table::right_join only valid inside #[query] body")
                }
                pub fn full_join<__T>(
                    _f: impl ::core::ops::FnOnce(#name #type_g, __T) -> bool,
                ) -> ::cartel_sqlite::JoinBuilder<#name #type_g, __T> {
                    ::core::unreachable!("cartel_sqlite: Table::full_join only valid inside #[query] body")
                }

                pub fn insert(_f: impl ::core::ops::FnOnce(&mut #name #type_g))
                    -> ::cartel_sqlite::InsertBuilder<#name #type_g>
                {
                    ::core::unreachable!("cartel_sqlite: Table::insert only valid inside #[query] body")
                }
                pub fn insert_from<__S: ::cartel_sqlite::SourceRow>(
                    _s: __S,
                    _f: impl ::core::ops::FnOnce(&mut #name #type_g, __S::Row),
                ) -> ::cartel_sqlite::InsertBuilder<#name #type_g> {
                    ::core::unreachable!("cartel_sqlite: Table::insert_from only valid inside #[query] body")
                }
            }
        })
    }
}

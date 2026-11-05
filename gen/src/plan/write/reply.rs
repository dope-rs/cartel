use crate::backend;
use crate::emit;
use crate::plan;
use crate::plan::row;
use crate::shape;

use syn::spanned::Spanned as _;

pub(in crate::plan) struct Reply<'a>(pub(in crate::plan) plan::State<'a>);

pub(in crate::plan) enum Op {
    Insert,
    Update,
    Delete,
}

pub(in crate::plan) struct Output {
    output: syn::ReturnType,
    returning: Option<Returning>,
}

struct Returning {
    cols: proc_macro2::TokenStream,
    dispatch: emit::Kind,
}

impl<'a> Reply<'a> {
    pub(in crate::plan) fn build(
        self,
        base: Vec<proc_macro2::TokenStream>,
        table: &syn::Path,
        prefix: proc_macro2::TokenStream,
        output: Output,
    ) -> plan::Step<'a, emit::Plan> {
        let Output {
            output,
            returning: rows,
        } = output;
        let value = match rows {
            None => emit::Plan::no_rows(output, base),
            Some(Returning { cols, dispatch }) => {
                let mut sql = base;
                sql.push(self.0.dialect.kw("RETURNING_KW"));
                let columns = match self.0.dialect.kind {
                    backend::Kind::Pg => {
                        let ty = syn::parse_quote!(#table);
                        self.0.dialect.columns(&ty, prefix)
                    }
                    backend::Kind::Sqlite => quote::quote! { <#table>::__CARTEL_SELECT_COLS },
                };
                sql.push(columns);
                emit::Plan {
                    output,
                    sql_parts: sql,
                    n_result_cols: cols,
                    dispatch,
                    probe_override: None,
                }
            }
        };
        plan::Step {
            state: self.0,
            value,
        }
    }

    pub(in crate::plan) fn validate(
        self,
        op: Op,
        ret: shape::Returning,
        table: &syn::Path,
    ) -> syn::Result<plan::Step<'a, Output>> {
        let rt = &self.0.dialect.rt_crate;
        let (op, command) = match op {
            Op::Insert => (
                "INSERT",
                syn::parse_quote!(-> #rt::dsl::write::Insert<#table>),
            ),
            Op::Update => (
                "UPDATE",
                syn::parse_quote!(-> #rt::dsl::write::Update<#table>),
            ),
            Op::Delete => (
                "DELETE",
                syn::parse_quote!(-> #rt::dsl::write::Delete<#table>),
            ),
        };
        let output = shape::ReturnShape::parse(self.0.return_type);
        let value = match ret {
            shape::Returning::None => {
                if !output.is_unit() {
                    return Err(syn::Error::new(
                        self.0.return_type.span(),
                        format!(
                            "{op} without `.returning_*()` must return `()` (or omit the return type)"
                        ),
                    ));
                }
                Output {
                    output: command,
                    returning: None,
                }
            }
            kind => {
                let shape::ReturnShape::Explicit(explicit) = output else {
                    return Err(syn::Error::new(
                        self.0.return_type.span(),
                        format!(
                            "{op} with `.returning_{}()` requires an explicit return type ({})",
                            kind.suffix(),
                            kind.expected().syntax(),
                        ),
                    ));
                };
                let actual = explicit.kind();
                if actual != kind.expected() {
                    return Err(syn::Error::new(
                        explicit.row().span(),
                        format!(
                            "return type {} does not match `.returning_{}()`; expected {}",
                            actual.syntax(),
                            kind.suffix(),
                            kind.expected().syntax(),
                        ),
                    ));
                }
                let (output, row, actual) = explicit.into_parts();
                let dispatch = match actual {
                    shape::RowReturn::Plain => emit::Kind::One(row.clone()),
                    shape::RowReturn::Optional => emit::Kind::First(row.clone()),
                    shape::RowReturn::Many => emit::Kind::All(row.clone()),
                    shape::RowReturn::Stream => {
                        return Err(syn::Error::new(row.span(), "invalid RETURNING output kind"));
                    }
                };
                Output {
                    output,
                    returning: Some(Returning {
                        cols: row::View::new(&row).cols(),
                        dispatch,
                    }),
                }
            }
        };
        Ok(plan::Step {
            state: self.0,
            value,
        })
    }
}

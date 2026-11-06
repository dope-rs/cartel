use crate::backend;
use crate::emit;
use crate::plan;
use crate::plan::row;
use crate::shape;

use syn::spanned::Spanned as _;

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

pub(in crate::plan) fn build(
    state: &plan::State<'_>,
    base: Vec<proc_macro2::TokenStream>,
    table: &syn::Path,
    prefix: proc_macro2::TokenStream,
    output: Output,
) -> emit::Plan {
    let Output {
        output,
        returning: rows,
    } = output;
    match rows {
        None => emit::Plan::no_rows(output, base),
        Some(Returning { cols, dispatch }) => {
            let mut sql = base;
            sql.push(state.dialect.kw("RETURNING_KW"));
            let columns = match state.dialect.kind {
                backend::Kind::Pg => {
                    let ty = syn::parse_quote!(#table);
                    state.dialect.columns(&ty, prefix)
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
    }
}

pub(in crate::plan) fn validate(
    state: &plan::State<'_>,
    op: Op,
    ret: shape::Returning,
    table: &syn::Path,
) -> syn::Result<Output> {
    let rt = &state.dialect.rt_crate;
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
    let output = shape::ReturnShape::parse(state.return_type);
    Ok(match ret {
        shape::Returning::None => {
            if !output.is_unit() {
                return Err(syn::Error::new(
                    state.return_type.span(),
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
                    state.return_type.span(),
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
    })
}

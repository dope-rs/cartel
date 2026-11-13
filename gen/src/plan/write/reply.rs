use crate::backend;
use crate::emit;
use crate::plan;
use crate::plan::row;
use crate::shape;
use std::borrow::Cow;

use syn::spanned::Spanned as _;

pub(in crate::plan) enum Op {
    Insert,
    Update,
    Delete,
}

pub(in crate::plan) struct Output<'a> {
    output: Cow<'a, syn::ReturnType>,
    cols: proc_macro2::TokenStream,
    dispatch: emit::Kind<'a>,
}

pub(in crate::plan) fn build<'a>(
    state: &plan::State<'_, '_>,
    mut sql: Vec<proc_macro2::TokenStream>,
    table: &syn::Path,
    prefix: proc_macro2::TokenStream,
    output: Output<'a>,
) -> emit::Plan<'a> {
    if !matches!(output.dispatch, emit::Kind::NoRows(_)) {
        sql.push(state.dialect.kw("RETURNING_KW"));
        let columns = match state.dialect.kind {
            backend::Kind::Pg => {
                let ty = syn::parse_quote!(#table);
                state.dialect.columns(&ty, prefix)
            }
            backend::Kind::Sqlite => quote::quote! { <#table>::__CARTEL_SELECT_COLS },
        };
        sql.push(columns);
    }
    emit::Plan {
        output: output.output,
        sql_parts: sql,
        n_result_cols: output.cols,
        dispatch: output.dispatch,
        probe_override: None,
    }
}

pub(in crate::plan) fn validate<'a>(
    state: &plan::State<'_, 'a>,
    op: Op,
    ret: shape::Returning,
    table: &syn::Path,
) -> syn::Result<Output<'a>> {
    let required = matches!(ret, shape::Returning::Required);
    if required && !(matches!(op, Op::Update) && matches!(state.dialect.kind, backend::Kind::Pg)) {
        return Err(syn::Error::new(
            table.span(),
            "`.required()` requires a PostgreSQL UPDATE",
        ));
    }
    let rt = &state.dialect.rt_crate;
    let (op, command) = match op {
        Op::Insert => ("INSERT", "Insert"),
        Op::Update => ("UPDATE", "Update"),
        Op::Delete => ("DELETE", "Delete"),
    };
    let output = shape::ReturnShape::parse(state.return_type);
    Ok(match ret.expected() {
        None => {
            if !output.is_unit() {
                return Err(syn::Error::new(
                    state.return_type.span(),
                    format!(
                        "{op} without `.returning_*()` must return `()` (or omit the return type)"
                    ),
                ));
            }
            let command = syn::Ident::new(command, proc_macro2::Span::call_site());
            Output {
                output: Cow::Owned(syn::parse_quote!(-> #rt::dsl::write::#command<#table>)),
                cols: quote::quote! { 0u16 },
                dispatch: emit::Kind::NoRows(required),
            }
        }
        Some(kind) => {
            let shape::ReturnShape::Explicit(explicit) = output else {
                return Err(syn::Error::new(
                    state.return_type.span(),
                    format!(
                        "{op} with `.returning_{}()` requires an explicit return type ({})",
                        kind.method(),
                        kind.syntax(),
                    ),
                ));
            };
            let actual = explicit.kind();
            if actual != kind {
                return Err(syn::Error::new(
                    explicit.row().span(),
                    format!(
                        "return type {} does not match `.returning_{}()`; expected {}",
                        actual.syntax(),
                        kind.method(),
                        kind.syntax(),
                    ),
                ));
            }
            let (output, row, kind) = explicit.into_parts();
            Output {
                output,
                cols: row::View::new(row).cols(),
                dispatch: emit::Kind::rows(kind, row),
            }
        }
    })
}

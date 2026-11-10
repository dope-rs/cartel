use crate::backend;
use crate::emit;
use crate::plan;
use crate::plan::row;
use crate::pred;
use crate::shape;
use crate::shape::qualify;

use syn::spanned::Spanned as _;

use crate::ext::ExprExt as _;

use crate::plan::read::{Qualify, filter, join_parts, project};

pub(in crate::plan) fn build<'a>(
    state: &mut plan::State<'_, 'a>,
    source: &shape::QuerySource,
    preds: &[syn::Ident],
    pred: &syn::Expr,
    proj: Option<&shape::Projection>,
    term: shape::Terminator,
    quals: &qualify::State,
) -> syn::Result<emit::Plan<'a>> {
    let explicit = match shape::ReturnShape::parse(state.return_type) {
        shape::ReturnShape::Default => {
            return Err(syn::Error::new(
                state.return_type.span(),
                "SELECT queries require an explicit return type",
            ));
        }
        shape::ReturnShape::Explicit(explicit) => explicit,
    };
    let (output, row_ty, kind) = explicit.into_parts();
    if kind != term {
        return Err(syn::Error::new(
            row_ty.span(),
            format!(
                "return type {} does not match terminator `.{}()`; expected {}",
                kind.syntax(),
                term.method(),
                term.syntax(),
            ),
        ));
    }

    let scope = pred::RowScope::for_source(source, preds)?;
    let qual = Qualify::new(quals, Some(term), &scope, state)?;

    let ons = join_parts(state, source)?;

    let skip = pred.is_true();
    let where_ = if skip {
        Vec::new()
    } else {
        filter::build(state, pred, &scope, false)?
    };

    let tail = qual.render(state);

    let mut sql = vec![state.dialect.kw("SELECT_KW")];
    sql.extend(project::distinct(state, quals, &scope)?);
    if let Some(proj) = proj {
        let scope = pred::RowScope::for_source(source, &proj.idents)?;
        for (i, elem) in proj.elems.iter().enumerate() {
            if i > 0 {
                sql.push(state.dialect.kw("COMMA"));
            }
            sql.extend(project::element(state, elem, &scope, quals.lock)?);
        }
    } else if source.joins.is_empty() {
        let table = &source.primary_path;
        sql.push(quote::quote! { <#table>::__CARTEL_SELECT_COLS });
    } else {
        sql.extend(row::View::new(row_ty).select(source, state.dialect)?);
    }
    from_clause(&mut sql, source, ons, state.dialect);
    if !skip {
        sql.push(state.dialect.kw("WHERE_KW"));
        sql.extend(where_);
    }
    sql.extend(tail);

    let cols = match proj {
        Some(proj) => {
            let actual = proj.elems.len();
            if actual > i16::MAX as usize {
                return Err(syn::Error::new(
                    row_ty.span(),
                    format!(
                        "PostgreSQL row has {actual} columns; the maximum is {}",
                        i16::MAX,
                    ),
                ));
            }
            let n = actual as u16;
            quote::quote! { #n }
        }
        None => row::View::new(row_ty).cols(),
    };

    Ok(emit::Plan {
        output,
        sql_parts: sql,
        n_result_cols: cols,
        dispatch: emit::Kind::rows(kind, row_ty),
        probe_override: None,
    })
}

pub(in crate::plan) fn from_clause(
    parts: &mut Vec<proc_macro2::TokenStream>,
    source: &shape::QuerySource,
    ons: Vec<Vec<proc_macro2::TokenStream>>,
    dialect: &backend::Dialect,
) {
    parts.push(dialect.kw("FROM_KW"));
    parts.extend(source.render_base());
    for (index, (j, on_parts)) in source.joins.iter().zip(ons).enumerate() {
        parts.push(j.kind.sql_ref(dialect));
        if !j.kind.is_lateral() {
            parts.extend(source.render_join(index, j));
            parts.push(dialect.kw("ON_KW"));
        }
        parts.extend(on_parts);
    }
}

use crate::emit::token::{COMMA, FROM, SELECT, WHERE, fragment};
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
    selection: &shape::Selection<'_>,
    proj: Option<&crate::ext::Binding<'_>>,
    explicit_row: Option<&syn::Type>,
    term: shape::Terminator,
    quals: &qualify::State<'_>,
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

    let source = &selection.source;
    if !source.joins.is_empty() {
        quals.ordered(source.primary_path.span())?;
    }
    let scope = pred::RowScope::for_source(source, &selection.idents)?;

    let ons = join_parts(state, source)?;

    let skip = selection.predicate.is_none_or(syn::Expr::is_true);
    let where_ = if skip {
        Vec::new()
    } else {
        filter::build(state, selection.predicate, &scope, false)?
    };

    let qual = Qualify::new(quals, Some(term), &scope, state)?;
    let mut tail = Vec::new();
    qual.render_into(state, &mut tail);

    let mut sql = vec![fragment(SELECT)];
    project::distinct(state, quals, &scope, &mut sql)?;
    let witness = backend::scope();
    let (value, cols) = if let Some(proj) = proj {
        let scope = pred::RowScope::for_source(source, &proj.idents)?;
        let (value, n) = match proj.body {
            syn::Expr::Tuple(tuple) => {
                let mut values = Vec::with_capacity(tuple.elems.len());
                for elem in &tuple.elems {
                    if !values.is_empty() {
                        sql.push(fragment(COMMA));
                    }
                    values.push(project::element(state, elem, &scope, quals.lock, &mut sql)?);
                }
                (quote::quote! { (#(#values,)*) }, values.len())
            }
            elem => (
                project::element(state, elem, &scope, quals.lock, &mut sql)?,
                1,
            ),
        };
        if n == 0 {
            return Err(syn::Error::new(
                row_ty.span(),
                "projection must contain a column",
            ));
        }
        if n > i16::MAX as usize {
            return Err(syn::Error::new(
                row_ty.span(),
                format!(
                    "PostgreSQL row has {n} columns; the maximum is {}",
                    i16::MAX
                ),
            ));
        }
        let n = n as u16;
        (scope.bound(value), quote::quote!(#n))
    } else {
        let table = &source.primary_path;
        let value = if source.joins.is_empty() {
            sql.push(quote::quote! { <#table>::__CARTEL_SELECT_COLS });
            quote::quote! { #witness.value::<#table>() }
        } else {
            sql.extend(row::View::new(row_ty).select(source, state.dialect)?);
            if term == shape::Terminator::All {
                let selected = explicit_row.unwrap_or(row_ty);
                quote::quote! { #witness.value::<#selected>() }
            } else {
                let tables = std::iter::once(table).chain(source.joins.iter().map(|j| j.path));
                quote::quote! { #witness.value::<(#(#tables,)*)>() }
            }
        };
        (value, row::View::new(row_ty).cols())
    };
    from_clause(&mut sql, source, ons);
    if !skip {
        sql.push(fragment(WHERE));
        sql.extend(where_);
    }
    sql.extend(tail);

    Ok(emit::Plan {
        output,
        sql_parts: sql,
        n_result_cols: cols,
        dispatch: emit::Kind::rows(kind, row_ty),
        result: term.result(value),
    })
}

pub(in crate::plan) fn from_clause(
    parts: &mut Vec<proc_macro2::TokenStream>,
    source: &shape::QuerySource<'_>,
    ons: Vec<Vec<proc_macro2::TokenStream>>,
) {
    parts.push(fragment(FROM));
    parts.extend(source.render_base());
    for (index, (j, on_parts)) in source.joins.iter().zip(ons).enumerate() {
        parts.push(j.kind.sql_ref());
        if !j.kind.is_lateral() {
            parts.extend(source.render_join(index, j));
            parts.push(fragment(" ON "));
        }
        parts.extend(on_parts);
    }
}

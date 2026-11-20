use crate::emit::token::{COMMA, COUNT_STAR, PAREN_CLOSE, SELECT, WHERE, fragment};
use crate::emit;
use crate::plan;
use crate::pred;
use crate::shape;

use syn::spanned::Spanned as _;

use crate::plan::read::{Qualify, filter, join_parts, select};

pub(in crate::plan) fn build<'a>(
    state: &mut plan::State<'_, 'a>,
    agg: &shape::Aggregate<'_>,
) -> syn::Result<emit::Plan<'a>> {
    agg.qualifiers
        .lock
        .reject("row locking is not supported on aggregate queries")?;
    if !matches!(agg.qualifiers.distinct, shape::qualify::Distinct::None) {
        return Err(syn::Error::new(
            state.return_type.span(),
            "DISTINCT qualifiers are not supported on aggregate queries",
        ));
    }
    let mut explicit = match shape::ReturnShape::parse(state.return_type) {
        shape::ReturnShape::Default => {
            return Err(syn::Error::new(
                state.return_type.span(),
                "aggregate query needs an explicit return type (e.g. `-> i64` for `.count()`)",
            ));
        }
        shape::ReturnShape::Explicit(explicit) => explicit,
    };
    let scalar = agg.group_by.is_none()
        && matches!(agg.kind, shape::Aggregation::Column(..))
        && explicit.kind() != shape::Terminator::Stream;
    if scalar && matches!(state.dialect.kind, crate::backend::Kind::Sqlite) {
        explicit.scalar();
    }
    let (output, row, kind) = explicit.into_parts();
    let dispatch = match kind {
        _ if scalar => emit::Kind::Aggregate(row),
        shape::Terminator::Stream => {
            return Err(syn::Error::new(
                row.span(),
                "aggregate (.count/.sum/.avg/.min/.max) does not support `.stream()` — aggregate returns a single row",
            ));
        }
        kind => emit::Kind::rows(kind, row),
    };
    if let (Some(having), None) = (&agg.having, &agg.group_by) {
        return Err(syn::Error::new(
            having.pred.span(),
            "`.having(...)` requires a preceding `.group_by(...)`",
        ));
    }

    let source = &agg.selection.source;
    let scope = pred::RowScope::for_source(source, &agg.selection.idents)?;
    let group = match &agg.group_by {
        Some(group) => {
            let scope = pred::RowScope::for_source(source, &group.idents)?;
            let mut operand = state.parse(&scope, group.body, || {
                syn::Error::new(
                    group.body.span(),
                    "`.group_by(...)` body must be a column / arithmetic / function expression",
                )
            })?;
            operand.rust = scope.bound(operand.rust);
            Some(operand)
        }
        None => None,
    };
    let value = match agg.kind.function() {
        Some((name, column)) => {
            let scope = pred::RowScope::for_source(source, &column.idents)?;
            let mut sql = state.parse(&scope, column.body, || {
                syn::Error::new(
                    column.body.span(),
                    format!(
                        "`.{}(...)` body must be a column / arithmetic / function expression",
                        name.lower(),
                    ),
                )
            })?;
            sql.rust = scope.bound(sql.rust);
            Some((name, sql))
        }
        None => None,
    };
    let having = match &agg.having {
        Some(having) => {
            let scope = pred::RowScope::for_source(source, &having.row_args)?
                .with_agg(having.agg_arg.clone());
            let predicate = pred::build(Some(having.pred), &scope, false, &state.context())?;
            state.condition(&scope, predicate.rust);
            Some(predicate.sql)
        }
        None => None,
    };
    let quals = Qualify::new(&agg.qualifiers, None, &scope, state)?;
    let joins = join_parts(state, source)?;

    let pred = filter::build(state, agg.selection.predicate, &scope, false)?;

    let witness = crate::backend::scope();
    let result = match (&group, &value) {
        (Some(key), Some((_, operand))) => {
            let key = &key.rust;
            let operand = &operand.rust;
            quote::quote! { #witness.rows((#key, ::core::option::Option::Some(#operand))) }
        }
        (Some(key), None) => {
            let key = &key.rust;
            quote::quote! { #witness.rows((#key, 0i64)) }
        }
        (None, Some((_, operand))) => {
            let operand = &operand.rust;
            quote::quote! {{ let _ = #operand; ::core::option::Option::None }}
        }
        (None, None) => quote::quote!(0i64),
    };
    let group = group.map(|operand| state.render(operand.sql));
    let mut sql = vec![fragment(SELECT)];
    if let Some(parts) = &group {
        sql.extend(parts.iter().cloned());
        sql.push(fragment(COMMA));
    }
    match value {
        None => sql.push(fragment(COUNT_STAR)),
        Some((name, value)) => {
            let prefix = format!("{}(", name.upper());
            sql.push(quote::quote! { #prefix });
            sql.extend(state.render(value.sql));
            sql.push(fragment(PAREN_CLOSE));
        }
    }
    select::from_clause(&mut sql, source, joins);
    sql.push(fragment(WHERE));
    sql.extend(pred);
    let columns = match group {
        None => 1u16,
        Some(parts) => {
            sql.push(fragment(" GROUP BY "));
            sql.extend(parts);
            2u16
        }
    };
    if let Some(having) = having {
        sql.push(fragment(" HAVING "));
        sql.extend(state.render(having));
    }
    quals.render_into(state, &mut sql);

    Ok(emit::Plan {
        output,
        sql_parts: sql,
        n_result_cols: quote::quote! { #columns },
        dispatch,
        result,
    })
}

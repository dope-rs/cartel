use crate::emit;
use crate::parse::assign;
use crate::plan;
use crate::pred;
use crate::shape;

use syn::spanned::Spanned as _;

use crate::ext::SqlIdent as _;

use crate::plan::write::reply::Op;
use crate::plan::write::{Conflict, reply};

#[derive(Clone, Copy)]
enum Kind {
    Single,
    Each,
}

pub(in crate::plan) fn build(
    state: &mut plan::State<'_>,
    table: &syn::Path,
    arg: &syn::Ident,
    body: &syn::Expr,
    ret: shape::Returning,
    conflict: &shape::Conflict,
) -> syn::Result<emit::Plan> {
    insert(state, table, arg, body, ret, conflict, Kind::Single)
}

pub(in crate::plan) fn each(
    state: &mut plan::State<'_>,
    table: &syn::Path,
    arg: &syn::Ident,
    body: &syn::Expr,
    ret: shape::Returning,
    conflict: &shape::Conflict,
) -> syn::Result<emit::Plan> {
    insert(state, table, arg, body, ret, conflict, Kind::Each)
}

fn insert(
    state: &mut plan::State<'_>,
    table: &syn::Path,
    arg: &syn::Ident,
    body: &syn::Expr,
    ret: shape::Returning,
    conflict: &shape::Conflict,
    kind: Kind,
) -> syn::Result<emit::Plan> {
    let output = reply::validate(state, Op::Insert, ret, table)?;

    let assigns = assign::Assign::collect(
        body,
        arg,
        &pred::RowScope::single(arg.clone()),
        state.params,
        state.dialect,
        &state.cte_map,
    )?;
    if assigns.is_empty() {
        let message = match kind {
            Kind::Single => "`Table::insert(...)` body must assign at least one column",
            Kind::Each => "`Table::insert_each(...)` body must assign at least one column",
        };
        return Err(syn::Error::new(body.span(), message));
    }
    let target = shape::QuerySource::new(table.clone());
    let action = Conflict::new(conflict, &target, state)?;

    let mut cols_str = String::new();
    for (i, a) in assigns.iter().enumerate() {
        if i > 0 {
            cols_str.push(',');
        }
        cols_str.push_str(&a.column.quoted());
    }

    let mut sql = vec![state.dialect.kw("INSERT_INTO_KW")];
    sql.extend(target.render_base());
    sql.extend([
        state.dialect.kw("PAREN_OPEN_LEADING_SPACE"),
        quote::quote! { #cols_str },
        state.dialect.kw(match kind {
            Kind::Single => "VALUES_OPEN",
            Kind::Each => "UNNEST_OPEN",
        }),
    ]);
    for (index, assign) in assigns.into_iter().enumerate() {
        if index > 0 {
            sql.push(state.dialect.kw("COMMA_TIGHT"));
        }
        sql.extend(state.render(assign.value));
    }
    sql.push(state.dialect.kw("PAREN_CLOSE"));
    sql.extend(action.render(state));
    Ok(reply::build(state, sql, table, target.qualifier(0), output))
}

pub(in crate::plan) fn from(
    state: &mut plan::State<'_>,
    input: &shape::mutation::From,
    ret: shape::Returning,
    conflict: &shape::Conflict,
) -> syn::Result<emit::Plan> {
    let shape::mutation::From {
        table,
        source: query,
        pred_idents: preds,
        predicate: pred,
        target,
        source_idents: idents,
        body,
    } = input;
    let output = reply::validate(state, Op::Insert, ret, table)?;

    let scope = pred::RowScope::for_source(query, idents)?;
    let selected = assign::Assign::collect(
        body,
        target,
        &scope,
        state.params,
        state.dialect,
        &state.cte_map,
    )?;
    if selected.is_empty() {
        return Err(syn::Error::new(
            body.span(),
            "`Table::insert_from(...)` body must assign at least one column",
        ));
    }
    let scope = pred::RowScope::for_source(query, preds)?;
    let context = state.context();
    let where_ = pred::build(pred, &scope, false, &context)?;
    let mut ons = Vec::with_capacity(query.joins.len());
    for (index, join) in query.joins.iter().enumerate() {
        let scope = pred::RowScope::for_join_on(query, index)?;
        ons.push(pred::build(&join.cond, &scope, true, &context)?.join_tokens()?);
    }
    let target = shape::QuerySource::new(table.clone());
    let action = Conflict::new(conflict, &target, state)?;

    let mut cols_str = String::new();
    let mut select: Vec<proc_macro2::TokenStream> = Vec::new();
    for (index, assign) in selected.into_iter().enumerate() {
        let rhs = state.render(assign.value);
        if index > 0 {
            cols_str.push(',');
            select.push(state.dialect.kw("COMMA"));
        }
        cols_str.push_str(&assign.column.quoted());
        select.extend(rhs);
    }
    let where_ = state.render(where_);

    let mut sql = vec![state.dialect.kw("INSERT_INTO_KW")];
    sql.extend(target.render_base());
    sql.extend([
        state.dialect.kw("PAREN_OPEN_LEADING_SPACE"),
        quote::quote! { #cols_str },
        state.dialect.kw("INSERT_FROM_SELECT"),
    ]);
    sql.extend(select);
    sql.push(state.dialect.kw("FROM_KW"));
    sql.extend(query.render_base());
    for (i, (j, on)) in query.joins.iter().zip(ons).enumerate() {
        sql.push(j.kind.sql_ref(state.dialect));
        sql.extend(query.render_join(i, j));
        sql.push(state.dialect.kw("ON_KW"));
        sql.extend(on);
    }
    sql.push(state.dialect.kw("WHERE_KW"));
    sql.extend(where_);
    sql.extend(action.render(state));
    Ok(reply::build(state, sql, table, target.qualifier(0), output))
}

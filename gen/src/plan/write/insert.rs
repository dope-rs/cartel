use crate::emit;
use crate::parse::assign;
use crate::plan;
use crate::pred;
use crate::shape;

use crate::ext::SqlIdent as _;

use crate::plan::write::reply::Op;
use crate::plan::write::{Conflict, reply};

pub(in crate::plan) fn build<'a>(
    state: &mut plan::State<'_, 'a>,
    insert: &shape::mutation::Insert,
    ret: shape::Returning,
) -> syn::Result<emit::Plan<'a>> {
    use shape::mutation::{Input, Values};

    let (Values { table, arg, body }, keyword, empty) = match &insert.input {
        Input::One(values) => (
            values,
            "VALUES_OPEN",
            "`Table::insert(...)` body must assign at least one column",
        ),
        Input::Each(values) => (
            values,
            "UNNEST_OPEN",
            "`Table::insert_each(...)` body must assign at least one column",
        ),
        Input::From(source) => return from(state, source, ret, &insert.conflict),
    };
    let output = reply::validate(state, Op::Insert, ret, table)?;

    let assigns = assign::Assign::collect(
        body,
        arg,
        &pred::RowScope::single(arg.clone()),
        &state.context(),
        empty,
    )?;
    let target = shape::QuerySource::new(table.clone());
    let action = Conflict::new(&insert.conflict, &target, state)?;

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
        state.dialect.kw(keyword),
    ]);
    for (index, assign) in assigns.into_iter().enumerate() {
        if index > 0 {
            sql.push(state.dialect.kw("COMMA_TIGHT"));
        }
        state.render_into(assign.value, &mut sql);
    }
    sql.push(state.dialect.kw("PAREN_CLOSE"));
    action.append(state, &mut sql);
    Ok(reply::build(state, sql, table, target.qualifier(0), output))
}

fn from<'a>(
    state: &mut plan::State<'_, 'a>,
    input: &shape::mutation::From,
    ret: shape::Returning,
    conflict: &shape::Conflict,
) -> syn::Result<emit::Plan<'a>> {
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
        &state.context(),
        "`Table::insert_from(...)` body must assign at least one column",
    )?;
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
        if index > 0 {
            cols_str.push(',');
            select.push(state.dialect.kw("COMMA"));
        }
        cols_str.push_str(&assign.column.quoted());
        state.render_into(assign.value, &mut select);
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
    action.append(state, &mut sql);
    Ok(reply::build(state, sql, table, target.qualifier(0), output))
}

use crate::emit::token::{
    COMMA, COMMA_TIGHT, FROM, INSERT_INTO, PAREN_CLOSE, PAREN_OPEN_LEADING_SPACE, WHERE, fragment,
};
use crate::emit;
use crate::parse::assign;
use crate::plan;
use crate::pred;
use crate::shape;

use crate::ext::SqlIdent as _;
use syn::spanned::Spanned as _;

use crate::plan::write::reply::Op;
use crate::plan::write::{Conflict, reply};

pub(in crate::plan) fn build<'a>(
    state: &mut plan::State<'_, 'a>,
    insert: &shape::mutation::Insert<'_>,
    ret: shape::Returning,
) -> syn::Result<emit::Plan<'a>> {
    use shape::mutation::{Input, Values};

    let (Values { table, arg, body }, keyword, empty) = match &insert.input {
        Input::One(values) => (
            values,
            fragment(") VALUES ("),
            "`Table::insert(...)` body must assign at least one column",
        ),
        Input::Each(values) => (
            values,
            state.dialect.pg_clause(
                ") SELECT * FROM UNNEST(",
                "insert_each",
                values.table.span(),
            ),
            "`Table::insert_each(...)` body must assign at least one column",
        ),
        Input::From(source) => return from(state, source, ret, &insert.conflict),
    };
    let output = reply::validate(state, Op::Insert, ret, table)?;

    let ty = match insert.input {
        Input::Each(_) => quote::quote!(<#table as ::cartel_pg::dsl::shape::Batch>::Columns<'_>),
        _ => quote::quote!(#table),
    };
    let assigns = assign::Assign::collect::<false>(
        body,
        arg,
        &pred::RowScope::single((*arg).clone(), ty.clone()),
        ty,
        state,
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

    let mut sql = vec![fragment(INSERT_INTO)];
    sql.extend(target.render_base());
    sql.extend([
        fragment(PAREN_OPEN_LEADING_SPACE),
        quote::quote! { #cols_str },
        keyword,
    ]);
    for (index, assign) in assigns.into_iter().enumerate() {
        if index > 0 {
            sql.push(fragment(COMMA_TIGHT));
        }
        state.render_into(assign.value, &mut sql);
    }
    sql.push(fragment(PAREN_CLOSE));
    action.append(state, &mut sql);
    Ok(reply::build(state, sql, table, target.qualifier(0), output))
}

fn from<'a>(
    state: &mut plan::State<'_, 'a>,
    input: &shape::mutation::From<'_>,
    ret: shape::Returning,
    conflict: &shape::Conflict<'_>,
) -> syn::Result<emit::Plan<'a>> {
    let shape::mutation::From {
        table,
        selection,
        target,
        source,
        body,
    } = input;
    let output = reply::validate(state, Op::Insert, ret, table)?;

    let query = &selection.source;
    let scope = pred::RowScope::for_source(query, &selection.idents)?;
    let where_ = pred::build(selection.predicate, &scope, false, &state.context())?;
    state.condition(&scope, where_.rust);
    let scope = pred::RowScope::for_primary(query, source);
    let selected = assign::Assign::collect::<false>(
        body,
        target,
        &scope,
        quote::quote!(#table),
        state,
        "`Table::insert_from(...)` body must assign at least one column",
    )?;
    let target = shape::QuerySource::new(table.clone());
    let action = Conflict::new(conflict, &target, state)?;

    let mut cols_str = String::new();
    let mut select: Vec<proc_macro2::TokenStream> = Vec::new();
    for (index, assign) in selected.into_iter().enumerate() {
        if index > 0 {
            cols_str.push(',');
            select.push(fragment(COMMA));
        }
        cols_str.push_str(&assign.column.quoted());
        state.render_into(assign.value, &mut select);
    }
    let where_ = state.render(where_.sql);

    let mut sql = vec![fragment(INSERT_INTO)];
    sql.extend(target.render_base());
    sql.extend([
        fragment(PAREN_OPEN_LEADING_SPACE),
        quote::quote! { #cols_str },
        fragment(") SELECT "),
    ]);
    sql.extend(select);
    sql.push(fragment(FROM));
    sql.extend(query.render_base());
    sql.push(fragment(WHERE));
    sql.extend(where_);
    action.append(state, &mut sql);
    Ok(reply::build(state, sql, table, target.qualifier(0), output))
}

use crate::emit::token::{COMMA_TIGHT, PAREN_CLOSE, SET, UPDATE, WHERE, fragment};
use crate::emit;
use crate::parse::assign;
use crate::plan;
use crate::pred;
use crate::shape;
use std::slice;

use crate::ext::FnParamsExt as _;
use syn::spanned::Spanned as _;

use crate::plan::write::reply;
use crate::plan::write::reply::Op;

fn table<'a>(source: &'a shape::QuerySource<'_>) -> syn::Result<&'a syn::Path> {
    if let Some(alias) = &source.primary_alias {
        return Err(syn::Error::new(
            alias.span(),
            "UPDATE and DELETE require a table source, not a CTE",
        ));
    }
    Ok(&source.primary_path)
}

fn where_clause(
    state: &mut plan::State<'_, '_>,
    selection: &shape::Selection<'_>,
    sql: &mut Vec<proc_macro2::TokenStream>,
) -> syn::Result<()> {
    let scope = pred::RowScope::for_source(&selection.source, &selection.idents)?;
    sql.push(fragment(WHERE));
    let filter = pred::build(selection.predicate, &scope, false, &state.context())?;
    state.condition(&scope, filter.rust);
    state.render_into(filter.sql, sql);
    Ok(())
}

pub(in crate::plan) fn build<'a>(
    state: &mut plan::State<'_, 'a>,
    update: &shape::mutation::Update<'_>,
    ret: shape::Returning,
) -> syn::Result<emit::Plan<'a>> {
    let source = &update.selection.source;
    let table = table(source)?;
    let output = reply::validate(state, Op::Update, ret, table)?;
    let mut conditions = Vec::new();
    where_clause(state, &update.selection, &mut conditions)?;
    let scope = pred::RowScope::for_primary(source, update.set_arg);
    let assigns = assign::Assign::collect::<true>(
        update.body,
        update.set_arg,
        &scope,
        quote::quote!(#table),
        state,
        "`.update(...)` body must assign at least one column",
    )?;

    let mut sql = vec![fragment(UPDATE)];
    sql.extend(source.render_base());
    sql.push(fragment(SET));
    super::assignments(state, &mut sql, assigns);
    sql.extend(conditions);
    Ok(reply::build(state, sql, table, source.qualifier(0), output))
}

pub(in crate::plan) fn delete<'a>(
    state: &mut plan::State<'_, 'a>,
    selection: &shape::Selection<'_>,
    ret: shape::Returning,
) -> syn::Result<emit::Plan<'a>> {
    let source = &selection.source;
    let table = table(source)?;
    let output = reply::validate(state, Op::Delete, ret, table)?;

    let mut sql = vec![fragment("DELETE FROM ")];
    sql.extend(source.render_base());
    where_clause(state, selection, &mut sql)?;
    Ok(reply::build(state, sql, table, source.qualifier(0), output))
}

pub(in crate::plan) fn each<'a>(
    state: &mut plan::State<'_, 'a>,
    input: &shape::mutation::Each<'_>,
    ret: shape::Returning,
) -> syn::Result<emit::Plan<'a>> {
    let shape::mutation::Each {
        source,
        data,
        row: row_var,
        cols: col_vars,
        predicate: pred,
        body: set_body,
    } = input;
    let table = &source.primary_path;
    let output = reply::validate(state, Op::Update, ret, table)?;

    let mut params: Vec<proc_macro2::TokenStream> = Vec::with_capacity(data.len());
    for ds in data {
        let captured = state.params.resolve_ref(ds)?;
        let idx = pred::sql::intern(&mut state.captures, captured);
        params.push(state.dialect.placeholder(idx, &state.params[captured.0].1));
    }

    let inputs: Vec<_> = data
        .iter()
        .map(|_| state.names.fresh("__cartel_column"))
        .collect();
    state
        .checks
        .extend(quote::quote! { let (#(#inputs,)*) = (#(#data,)*); });
    let scope = pred::RowScope::for_source(source, slice::from_ref(row_var))?
        .with_unnest(col_vars, &inputs);

    let filter = pred::build(Some(*pred), &scope, true, &state.context())?;
    state.condition(&scope, filter.rust);
    let changes = assign::Assign::collect::<false>(
        set_body,
        row_var,
        &scope,
        quote::quote!(#table),
        state,
        "`.update(...)` body must assign at least one column",
    )?;
    let mut sql = vec![fragment(UPDATE)];
    sql.extend(source.render_base());
    sql.push(fragment(SET));
    super::assignments(state, &mut sql, changes);
    let filter = state.render(filter.sql);

    let mut cols = String::new();
    let mut digits = core::fmt::NumBuffer::<usize>::new();
    for i in 0..data.len() {
        if i > 0 {
            cols.push(',');
        }
        cols.push('f');
        cols.push_str(i.format_into(&mut digits));
    }

    sql.push(
        state
            .dialect
            .pg_clause(" FROM unnest(", "update_each", table.span()),
    );
    for (i, ph) in params.into_iter().enumerate() {
        if i > 0 {
            sql.push(fragment(COMMA_TIGHT));
        }
        sql.push(ph);
    }
    sql.push(fragment(") AS __d("));
    sql.push(quote::quote! { #cols });
    sql.push(fragment(PAREN_CLOSE));
    sql.push(fragment(WHERE));
    sql.extend(filter);

    Ok(reply::build(state, sql, table, source.qualifier(0), output))
}

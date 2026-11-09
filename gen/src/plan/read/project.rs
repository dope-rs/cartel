use crate::plan;
use crate::pred::{self, var};
use crate::shape::{self, qualify};

use syn::spanned::Spanned as _;

use shape::join;

use crate::plan::read::{Qualify, filter, window};

pub(in crate::plan) fn element(
    state: &mut plan::State<'_, '_>,
    elem: &syn::Expr,
    scope: &pred::RowScope<'_>,
    lock: shape::lock::Lock,
) -> syn::Result<Vec<proc_macro2::TokenStream>> {
    if let syn::Expr::MethodCall(mc) = elem
        && mc.method == "over"
    {
        lock.reject("row locking is not supported with window functions")?;
        return window::build(state, mc, scope);
    }
    let parsed = state
        .parse(
            scope,
            elem,
            || syn::Error::new(
                elem.span(),
                "`.select(...)` element must be a column / arithmetic / function expression or a window expression `<fn>().over(|w| ...)`",
            ),
        )?;
    Ok(state.render(parsed))
}

pub(in crate::plan) fn lateral(
    state: &mut plan::State<'_, '_>,
    j: &join::Join,
    index: usize,
    source: &shape::QuerySource,
) -> syn::Result<Vec<proc_macro2::TokenStream>> {
    let outer = pred::RowScope::for_outer(source, &j.on_idents, index)?;
    let mut quals = qualify::State::default();
    let body = quals.peel(&j.cond)?;
    let (inner, ids, pred) = shape::QuerySource::parse_chain(body, &state.cte_map)?;
    let inner = inner.nested(Some(source.alias_depth));
    if !inner.joins.is_empty() {
        return Err(syn::Error::new(
            j.cond.span(),
            "joined source not yet supported as a lateral subquery body",
        ));
    }
    let table = &inner.primary_path;
    let scope = pred::RowScope::for_source(&inner, &ids)?.with_outer(&outer);
    let qual = Qualify::new(&quals, None, &scope, state)?;
    let filter = filter::build(state, &pred, &scope, true)?;

    let columns = state
        .dialect
        .columns(&syn::parse_quote!(#table), inner.qualifier(0));
    let mut sql = vec![state.dialect.kw("SUBQUERY_OPEN")];
    sql.extend(distinct(state, &quals, &scope)?);
    sql.extend([columns, state.dialect.kw("FROM_KW")]);
    sql.extend(inner.render_base());
    sql.push(state.dialect.kw("WHERE_KW"));
    sql.extend(filter);
    sql.extend(qual.render(state));
    sql.push(state.dialect.kw("PAREN_CLOSE_SPACE"));
    sql.push(source.qualifier(index + 1));
    sql.push(state.dialect.kw("ON_TRUE"));
    Ok(sql)
}

pub(in crate::plan) fn distinct(
    state: &mut plan::State<'_, '_>,
    quals: &qualify::State,
    scope: &pred::RowScope<'_>,
) -> syn::Result<Vec<proc_macro2::TokenStream>> {
    Ok(match &quals.distinct {
        qualify::Distinct::None => Vec::new(),
        qualify::Distinct::All => vec![state.dialect.kw("DISTINCT_KW")],
        qualify::Distinct::On(arg, expr) => {
            let mut vars = Vec::with_capacity(scope.vars.len().max(1));
            if let Some(first) = scope.vars.first() {
                vars.push(var::Var {
                    ident: arg.clone(),
                    table_const: first.table_const.clone(),
                });
            } else {
                vars.push(var::Var {
                    ident: arg.clone(),
                    table_const: None,
                });
            }
            let local = pred::RowScope {
                vars,
                agg_var: None,
                outer: scope.outer,
                alias_depth: scope.alias_depth,
                unnest_cols: &[],
            };
            let parsed = state.parse(&local, expr, || {
                syn::Error::new(
                    expr.span(),
                    "`.distinct_on(...)` body must be a column / arithmetic / function expression",
                )
            })?;
            let inner = state.render(parsed);
            let mut parts = vec![state.dialect.kw("DISTINCT_ON_OPEN")];
            parts.extend(inner);
            parts.push(state.dialect.kw("PAREN_CLOSE_SPACE"));
            parts
        }
    })
}

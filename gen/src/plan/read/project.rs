use crate::emit::token::{FROM, PAREN_CLOSE_SPACE, SUBQUERY_OPEN, WHERE, fragment};
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
    output: &mut Vec<proc_macro2::TokenStream>,
) -> syn::Result<proc_macro2::TokenStream> {
    if let syn::Expr::MethodCall(mc) = elem
        && mc.method == "over"
    {
        lock.reject("row locking is not supported with window functions")?;
        return window::build(state, mc, scope, output);
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
    state.render_into(parsed.sql, output);
    Ok(parsed.rust)
}

pub(in crate::plan) fn lateral(
    state: &mut plan::State<'_, '_>,
    j: &join::Join<'_>,
    index: usize,
    source: &shape::QuerySource<'_>,
) -> syn::Result<Vec<proc_macro2::TokenStream>> {
    if matches!(state.dialect.kind, crate::backend::Kind::Sqlite) {
        return Err(syn::Error::new(
            j.path.span(),
            "SQLite does not support lateral joins",
        ));
    }
    let outer = pred::RowScope::for_outer(source, &j.on.idents, index)?;
    let mut quals = qualify::State::default();
    let body = quals.peel(j.on.body)?;
    let shape::Selection {
        source: inner,
        idents,
        predicate,
    } = shape::Selection::parse(body, &state.cte_map)?;
    let inner = inner.nested(Some(source.alias_depth));
    if !inner.joins.is_empty() {
        return Err(syn::Error::new(
            j.on.body.span(),
            "joined source not yet supported as a lateral subquery body",
        ));
    }
    let table = &inner.primary_path;
    let joined = j.path;
    state
        .checks
        .extend(quote::quote! { let _: fn(#joined) -> #joined = |row: #table| -> #table { row }; });
    let scope = pred::RowScope::for_source(&inner, &idents)?.with_outer(&outer);
    let qual = Qualify::new(&quals, None, &scope, state)?;
    let filter = filter::build(state, predicate, &scope, true)?;

    let columns = state
        .dialect
        .columns(&syn::parse_quote!(#table), inner.qualifier(0));
    let mut sql = vec![fragment(SUBQUERY_OPEN)];
    distinct(state, &quals, &scope, &mut sql)?;
    sql.extend([columns, fragment(FROM)]);
    sql.extend(inner.render_base());
    sql.push(fragment(WHERE));
    sql.extend(filter);
    qual.render_into(state, &mut sql);
    sql.push(fragment(PAREN_CLOSE_SPACE));
    sql.push(source.qualifier(index + 1));
    sql.push(fragment(" ON true"));
    Ok(sql)
}

pub(in crate::plan) fn distinct(
    state: &mut plan::State<'_, '_>,
    quals: &qualify::State<'_>,
    scope: &pred::RowScope<'_>,
    output: &mut Vec<proc_macro2::TokenStream>,
) -> syn::Result<()> {
    match quals.distinct {
        qualify::Distinct::None => {}
        qualify::Distinct::All => output.push(fragment("DISTINCT ")),
        qualify::Distinct::On(arg, expr) => {
            let local = pred::RowScope {
                vars: vec![var::Var {
                    referenced: Default::default(),
                    ident: arg.clone(),
                    table_const: scope.vars[0].table_const.clone(),
                    ty: scope.vars[0].ty.clone(),
                }],
                agg_var: None,
                outer: scope.outer,
                alias_depth: scope.alias_depth,
                unnest_cols: &[],
                unnest_data: &[],
            };
            let parsed = state.parse(&local, expr, || {
                syn::Error::new(
                    expr.span(),
                    "`.distinct_on(...)` body must be a column / arithmetic / function expression",
                )
            })?;
            output.push(
                state
                    .dialect
                    .pg_clause("DISTINCT ON (", "DISTINCT ON", expr.span()),
            );
            let rust = local.bound(parsed.rust);
            state.checks.extend(quote::quote! { let _ = #rust; });
            state.render_into(parsed.sql, output);
            output.push(fragment(PAREN_CLOSE_SPACE));
        }
    }
    Ok(())
}

use crate::backend;
use crate::emit;
use crate::parse::assign;
use crate::plan;
use crate::pred;
use crate::shape;
use std::slice;

use syn::spanned::Spanned as _;
use syn::visit::{self, Visit as _};

use crate::ext::FnParamsExt as _;
use crate::ext::SqlIdent as _;

use crate::plan::read::filter;
use crate::plan::write::reply;
use crate::plan::write::reply::Op;

fn table(source: &shape::QuerySource) -> syn::Result<&syn::Path> {
    if let Some(alias) = &source.primary_alias {
        return Err(syn::Error::new(
            alias.span(),
            "UPDATE and DELETE require a table source, not a CTE",
        ));
    }
    Ok(&source.primary_path)
}

fn join_ons(
    state: &plan::State<'_>,
    source: &shape::QuerySource,
) -> syn::Result<Vec<Vec<proc_macro2::TokenStream>>> {
    let mut joins = Vec::with_capacity(source.joins.len());
    for (index, join) in source.joins.iter().enumerate() {
        let on_scope = pred::RowScope::for_join_on(source, index)?;
        joins.push(filter::join(state, &join.cond, &on_scope)?);
    }
    Ok(joins)
}

pub(in crate::plan) fn build(
    state: &mut plan::State<'_>,
    source: &shape::QuerySource,
    preds: &[syn::Ident],
    pred: &syn::Expr,
    set_id: &syn::Ident,
    set_body: &syn::Expr,
    ret: shape::Returning,
) -> syn::Result<emit::Plan> {
    let table = table(source)?;
    let output = reply::validate(state, Op::Update, ret, table)?;
    let lowered;
    let body = if let syn::Expr::Binary(expr) = set_body {
        if let syn::BinOp::AddAssign(op) = &expr.op {
            if let Some(attr) = expr.attrs.first() {
                return Err(syn::Error::new(
                    attr.span(),
                    "attributes are not supported on a compound assignment",
                ));
            }
            let left = &expr.left;
            let right = &expr.right;
            lowered = syn::parse_quote_spanned!(op.span()=> #left = #left + #right);
            &lowered
        } else {
            set_body
        }
    } else {
        set_body
    };
    let scope = pred::RowScope::for_primary(source, set_id);
    let assigns = assign::Assign::collect(
        body,
        set_id,
        &scope,
        state.params,
        state.dialect,
        &state.cte_map,
    )?;
    if assigns.is_empty() {
        return Err(syn::Error::new(
            set_body.span(),
            "`.update(...)` body must assign at least one column",
        ));
    }

    let scope = pred::RowScope::for_source(source, preds)?;

    let joins = join_ons(state, source)?;

    let filter = filter::build(state, pred, &scope, false)?;

    let mut sql = vec![state.dialect.kw("UPDATE_KW")];
    sql.extend(source.render_base());
    sql.push(state.dialect.kw("SET_KW"));
    for (index, assign) in assigns.into_iter().enumerate() {
        if index > 0 {
            sql.push(state.dialect.kw("COMMA"));
        }
        let col_eq = format!("{} = ", assign.column.quoted());
        sql.push(quote::quote! { #col_eq });
        sql.extend(state.render(assign.value));
    }
    if !source.joins.is_empty() {
        sql.push(state.dialect.kw("FROM_KW"));
        for (i, j) in source.joins.iter().enumerate() {
            if i > 0 {
                sql.push(state.dialect.kw("COMMA"));
            }
            sql.extend(source.render_join(i, j));
        }
    }
    sql.push(state.dialect.kw("WHERE_KW"));
    for on_parts in joins {
        sql.extend(on_parts);
        sql.push(state.dialect.kw("AND_KW"));
    }
    sql.extend(filter);

    Ok(reply::build(state, sql, table, source.qualifier(0), output))
}

pub(in crate::plan) fn delete(
    state: &mut plan::State<'_>,
    source: &shape::QuerySource,
    preds: &[syn::Ident],
    pred: &syn::Expr,
    ret: shape::Returning,
) -> syn::Result<emit::Plan> {
    let table = table(source)?;
    let output = reply::validate(state, Op::Delete, ret, table)?;

    let scope = pred::RowScope::for_source(source, preds)?;

    let joins = join_ons(state, source)?;

    let filter = filter::build(state, pred, &scope, false)?;

    let mut sql = vec![state.dialect.kw("DELETE_FROM_KW")];
    sql.extend(source.render_base());
    if !source.joins.is_empty() {
        sql.push(state.dialect.kw("USING_KW"));
        for (i, j) in source.joins.iter().enumerate() {
            if i > 0 {
                sql.push(state.dialect.kw("COMMA"));
            }
            sql.extend(source.render_join(i, j));
        }
    }
    sql.push(state.dialect.kw("WHERE_KW"));
    for on_parts in joins {
        sql.extend(on_parts);
        sql.push(state.dialect.kw("AND_KW"));
    }
    sql.extend(filter);

    Ok(reply::build(state, sql, table, source.qualifier(0), output))
}

pub(in crate::plan) fn each(
    state: &mut plan::State<'_>,
    input: &shape::mutation::Each,
    ret: shape::Returning,
) -> syn::Result<emit::Plan> {
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

    let scope = pred::RowScope::for_source(source, slice::from_ref(row_var))?.with_unnest(col_vars);

    let changes = assign::Assign::collect(
        set_body,
        row_var,
        &scope,
        state.params,
        state.dialect,
        &state.cte_map,
    )?;
    if changes.is_empty() {
        return Err(syn::Error::new(
            set_body.span(),
            "`.update(...)` body must assign at least one column",
        ));
    }
    let mut assigns: Vec<proc_macro2::TokenStream> = Vec::new();
    for (i, assign) in changes.into_iter().enumerate() {
        let rhs = state.render(assign.value);
        if i > 0 {
            assigns.push(state.dialect.kw("COMMA"));
        }
        let col_eq = format!("{} = ", assign.column.quoted());
        assigns.push(quote::quote! { #col_eq });
        assigns.extend(rhs);
    }
    let filter = filter::build(state, pred, &scope, true)?;

    let mut cols = String::new();
    let mut digits = core::fmt::NumBuffer::<usize>::new();
    for i in 0..data.len() {
        if i > 0 {
            cols.push(',');
        }
        cols.push('f');
        cols.push_str(i.format_into(&mut digits));
    }

    let mut sql = vec![state.dialect.kw("UPDATE_KW")];
    sql.extend(source.render_base());
    sql.push(state.dialect.kw("SET_KW"));
    sql.extend(assigns);
    sql.push(state.dialect.kw("UNNEST_FROM_OPEN"));
    for (i, ph) in params.into_iter().enumerate() {
        if i > 0 {
            sql.push(state.dialect.kw("COMMA_TIGHT"));
        }
        sql.push(ph);
    }
    sql.push(state.dialect.kw("UNNEST_ALIAS_OPEN"));
    sql.push(quote::quote! { #cols });
    sql.push(state.dialect.kw("PAREN_CLOSE"));
    sql.push(state.dialect.kw("WHERE_KW"));
    sql.extend(filter);

    let probe = {
        let scope = backend::scope();
        let rt = &state.dialect.rt_crate;
        let mut names = backend::Names::default();
        for name in state
            .params
            .iter()
            .map(|(id, _)| id)
            .chain(col_vars)
            .chain(slice::from_ref(row_var))
        {
            names.visit_ident(name);
        }
        let wildcard = syn::Ident::new("_", proc_macro2::Span::call_site());
        let mut patterns = vec![&wildcard; 2 * col_vars.len()];
        let (when, set) = patterns.split_at_mut(col_vars.len());
        for (body, patterns) in [(pred, &mut *when), (set_body, &mut *set)] {
            Binding {
                names: &mut names,
                columns: col_vars,
                patterns,
            }
            .visit_expr(body);
        }
        let inputs: Vec<_> = data.iter().map(|_| names.fresh("column")).collect();
        syn::parse_quote! {
            {
                let (#(#inputs,)*) = (#(#data,)*);
                #rt::dsl::query::ops::Change::update(
                    <#table>::filter(
                        #scope,
                        |#row_var: #table| -> bool {
                            #(let #when = #scope.element(&#inputs);)*
                            #pred
                        },
                    ),
                    |#row_var: &mut #table| {
                        #(let #set = #scope.element(&#inputs);)*
                        #set_body
                    },
                )
            }
        }
    };

    let mut plan = reply::build(state, sql, table, source.qualifier(0), output);
    plan.probe_override = Some(probe);
    Ok(plan)
}

struct Binding<'a, 'n> {
    names: &'n mut backend::Names,
    columns: &'a [syn::Ident],
    patterns: &'n mut [&'a syn::Ident],
}

impl<'ast> visit::Visit<'ast> for Binding<'_, '_> {
    fn visit_ident(&mut self, ident: &'ast syn::Ident) {
        self.names.visit_ident(ident);
    }

    fn visit_expr_path(&mut self, path: &'ast syn::ExprPath) {
        if path.qself.is_none() {
            for (name, pattern) in self.columns.iter().zip(self.patterns.iter_mut()) {
                if path.path.is_ident(name) {
                    *pattern = name;
                }
            }
        }
        visit::visit_expr_path(self, path);
    }
}

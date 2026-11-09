use crate::backend;
use crate::emit;
use crate::plan;
use crate::plan::row;
use crate::pred;
use crate::shape;
use crate::shape::qualify;
use std::iter;

use syn::spanned::Spanned as _;

use crate::ext::ExprExt as _;
use crate::ext::PatExt as _;
use crate::plan::read::{Qualify, filter};

struct Output<'a> {
    ty: std::borrow::Cow<'a, syn::ReturnType>,
    row: &'a syn::Type,
    kind: shape::RowReturn,
}

struct Order {
    col: pred::SqlColRef,
    dir: qualify::Dir,
}

pub(in crate::plan) fn build<'a>(
    state: &mut plan::State<'_, 'a>,
    set: &shape::SetQuery,
    end: shape::Terminator,
    outer: &qualify::State,
) -> syn::Result<emit::Plan<'a>> {
    let output = Output::new(state, set, end)?;
    let qual = Qualify::bounds(outer, end, state)?;
    let groups = match state.dialect.kind {
        backend::Kind::Pg => set.links.len().saturating_sub(1),
        backend::Kind::Sqlite => 0,
    };
    let mut sql: Vec<_> = iter::repeat_n(state.dialect.kw("PAREN_OPEN"), groups).collect();
    for (index, (op, branch)) in iter::once((None, &set.first))
        .chain(set.links.iter().map(|(op, branch)| (Some(*op), branch)))
        .enumerate()
    {
        let scope = pred::RowScope::for_source(&branch.source, &branch.pred_idents)?;
        let filter = if branch.predicate.is_true() {
            None
        } else {
            Some(filter::build(state, &branch.predicate, &scope, false)?)
        };
        if let Some(op) = op {
            sql.push(op.sql_ref(state.dialect));
        }
        let path = &branch.source.primary_path;
        sql.push(state.dialect.kw("SETOP_BRANCH_OPEN"));
        sql.push(quote::quote! { <#path>::__CARTEL_SELECT_COLS });
        sql.push(state.dialect.kw("FROM_KW"));
        sql.extend(branch.source.render_base());
        if let Some(filter) = filter {
            sql.push(state.dialect.kw("WHERE_KW"));
            sql.extend(filter);
        }
        sql.push(state.dialect.kw("SETOP_BRANCH_CLOSE"));
        if index > 0 && index <= groups {
            sql.push(state.dialect.kw("PAREN_CLOSE"));
        }
    }
    for (index, clause) in outer.order_by.iter().enumerate() {
        let order = Order::new(clause)?;
        let key = match index {
            0 => "ORDER_BY_KW",
            1.. => "COMMA",
        };
        sql.push(state.dialect.kw(key));
        sql.extend(order.col.into_parts());
        if matches!(order.dir, qualify::Dir::Desc) {
            sql.push(state.dialect.kw("DESC"));
        }
    }
    sql.extend(qual.render(state));

    let cols = row::View::new(output.row).cols();
    let dispatch = match output.kind {
        shape::RowReturn::Plain => emit::Kind::One(output.row),
        shape::RowReturn::Optional => emit::Kind::First(output.row),
        shape::RowReturn::Many => emit::Kind::All(output.row),
        shape::RowReturn::Stream => emit::Kind::Stream(output.row),
    };
    Ok(emit::Plan {
        output: output.ty,
        sql_parts: sql,
        n_result_cols: cols,
        dispatch,
        probe_override: None,
    })
}

impl<'a> Output<'a> {
    fn new(
        state: &plan::State<'_, 'a>,
        set: &shape::SetQuery,
        end: shape::Terminator,
    ) -> syn::Result<Self> {
        let explicit = match shape::ReturnShape::parse(state.return_type) {
            shape::ReturnShape::Default => {
                return Err(syn::Error::new(
                    state.return_type.span(),
                    "set-op (UNION / INTERSECT / EXCEPT) queries require an explicit return type",
                ));
            }
            shape::ReturnShape::Explicit(explicit) => explicit,
        };
        let actual = explicit.kind();
        if actual != end.expected() {
            return Err(syn::Error::new(
                explicit.row().span(),
                format!(
                    "return type {} does not match terminator `.{}()`; expected {}",
                    actual.syntax(),
                    end.method(),
                    end.expected().syntax(),
                ),
            ));
        }

        validate_branches(set)?;
        let (ty, row, kind) = explicit.into_parts();
        Ok(Self { ty, row, kind })
    }
}

fn validate_branches(set: &shape::SetQuery) -> syn::Result<()> {
    let first = &set.first.source.primary_path;
    validate_branch(first, &set.first, 1)?;
    for (index, (_, branch)) in set.links.iter().enumerate() {
        validate_branch(first, branch, index + 2)?;
    }
    Ok(())
}

fn validate_branch(
    first: &syn::Path,
    branch: &shape::SelectBranch,
    index: usize,
) -> syn::Result<()> {
    let path = &branch.source.primary_path;
    if path != first {
        return Err(syn::Error::new(
            branch.predicate.span(),
            format!(
                "set-op branch #{index} table differs from the first branch ({} vs {})",
                quote::quote! { #path },
                quote::quote! { #first },
            ),
        ));
    }
    if !branch.source.joins.is_empty() {
        return Err(syn::Error::new(
            branch.predicate.span(),
            "set-op branch with joined source not supported in v0",
        ));
    }
    Ok(())
}

impl Order {
    fn new(clause: &qualify::Order) -> syn::Result<Self> {
        let syn::Expr::Closure(closure) = &clause.closure else {
            return Err(syn::Error::new(
                clause.closure.span(),
                "set-op `.order_by(...)` argument must be a closure `|u| u.col`",
            ));
        };
        if closure.inputs.len() != 1 {
            return Err(syn::Error::new(
                closure.inputs.span(),
                "set-op `.order_by(...)` closure must take one parameter",
            ));
        }
        let arg = closure.inputs[0].ident()?.clone();
        let scope = pred::RowScope::single(arg);
        let col = scope.column_ref(&closure.body).ok_or_else(|| {
            syn::Error::new(
                closure.body.span(),
                "set-op `.order_by(...)` body must be a single column reference like `u.col`",
            )
        })?;
        Ok(Self {
            col,
            dir: clause.dir,
        })
    }
}

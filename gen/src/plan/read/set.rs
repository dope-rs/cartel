use crate::emit::token::{
    COMMA, DESC, FROM, ORDER_BY, PAREN_CLOSE, PAREN_OPEN, SELECT, SUBQUERY_OPEN, WHERE, fragment,
};
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
use crate::plan::read::{filter, qualify::Bounds};

struct Output<'a> {
    ty: &'a syn::ReturnType,
    row: &'a syn::Type,
    kind: shape::Terminator,
}

struct Order {
    col: pred::SqlColRef,
    dir: qualify::Dir,
}

pub(in crate::plan) fn build<'a>(
    state: &mut plan::State<'_, 'a>,
    set: &shape::SetQuery<'_>,
    end: shape::Terminator,
    outer: &qualify::State<'_>,
) -> syn::Result<emit::Plan<'a>> {
    let output = Output::new(state, set, end)?;
    outer
        .lock
        .reject("row locking is not supported on set-operation queries")?;
    if !matches!(outer.distinct, qualify::Distinct::None) {
        return Err(syn::Error::new(
            state.return_type.span(),
            "DISTINCT qualifiers are not supported on set-operation queries",
        ));
    }
    let bounds = Bounds::new(outer, Some(end), state)?;
    let (groups, start, end_branch) = match state.dialect.kind {
        backend::Kind::Pg => (
            set.links.len().saturating_sub(1),
            SUBQUERY_OPEN,
            Some(PAREN_CLOSE),
        ),
        backend::Kind::Sqlite => (0, SELECT, None),
    };
    let mut sql: Vec<_> = iter::repeat_n(fragment(PAREN_OPEN), groups).collect();
    for (index, (op, branch)) in iter::once((None, &set.first))
        .chain(set.links.iter().map(|(op, branch)| (Some(*op), branch)))
        .enumerate()
    {
        let scope = pred::RowScope::for_source(&branch.source, &branch.idents)?;
        let filter = if branch.predicate.is_none_or(syn::Expr::is_true) {
            None
        } else {
            Some(filter::build(state, branch.predicate, &scope, false)?)
        };
        if let Some(op) = op {
            sql.push(op.sql_ref(state.dialect, branch.source.primary_path.span()));
        }
        let path = &branch.source.primary_path;
        sql.push(fragment(start));
        sql.push(quote::quote! { <#path>::__CARTEL_SELECT_COLS });
        sql.push(fragment(FROM));
        sql.extend(branch.source.render_base());
        if let Some(filter) = filter {
            sql.push(fragment(WHERE));
            sql.extend(filter);
        }
        sql.extend(end_branch.map(fragment));
        if index > 0 && index <= groups {
            sql.push(fragment(PAREN_CLOSE));
        }
    }
    for (index, clause) in outer.order_by.iter().enumerate() {
        let order = Order::new(clause, &set.first.source, state)?;
        let key = match index {
            0 => ORDER_BY,
            1.. => COMMA,
        };
        sql.push(fragment(key));
        sql.extend(order.col.into_parts());
        if matches!(order.dir, qualify::Dir::Desc) {
            sql.push(fragment(DESC));
        }
    }
    bounds.render_into(state, &mut sql);

    let cols = row::View::new(output.row).cols();
    Ok(emit::Plan {
        output: output.ty,
        sql_parts: sql,
        n_result_cols: cols,
        dispatch: emit::Kind::rows(output.kind, output.row),
        result: end.result({
            let table = &set.first.source.primary_path;
            let scope = backend::scope();
            quote::quote! { #scope.value::<#table>() }
        }),
    })
}

impl<'a> Output<'a> {
    fn new(
        state: &plan::State<'_, 'a>,
        set: &shape::SetQuery<'_>,
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
        if actual != end {
            return Err(syn::Error::new(
                explicit.row().span(),
                format!(
                    "return type {} does not match terminator `.{}()`; expected {}",
                    actual.syntax(),
                    end.method(),
                    end.syntax(),
                ),
            ));
        }

        validate_branches(set)?;
        let (ty, row, kind) = explicit.into_parts();
        Ok(Self { ty, row, kind })
    }
}

fn validate_branches(set: &shape::SetQuery<'_>) -> syn::Result<()> {
    let first = &set.first.source.primary_path;
    validate_branch(first, &set.first, 1)?;
    for (index, (_, branch)) in set.links.iter().enumerate() {
        validate_branch(first, branch, index + 2)?;
    }
    Ok(())
}

fn validate_branch(
    first: &syn::Path,
    branch: &shape::Selection<'_>,
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
    fn new(
        clause: &qualify::Order<'_>,
        source: &shape::QuerySource<'_>,
        state: &mut plan::State<'_, '_>,
    ) -> syn::Result<Self> {
        let syn::Expr::Closure(closure) = clause.closure else {
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
        let table = &source.primary_path;
        let scope = pred::RowScope::single(arg, quote::quote!(#table));
        let col = scope.column_ref(&closure.body).ok_or_else(|| {
            syn::Error::new(
                closure.body.span(),
                "set-op `.order_by(...)` body must be a single column reference like `u.col`",
            )
        })?;
        let body = &closure.body;
        let value = scope.bound(quote::quote!(#body));
        state.checks.extend(quote::quote! { let _ = #value; });
        Ok(Self {
            col,
            dir: clause.dir,
        })
    }
}

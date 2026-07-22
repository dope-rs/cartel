use std::collections::HashMap;

use syn::spanned::Spanned;
use syn::{Expr, ExprCall, ExprPath, Ident, ItemFn, Pat, Path, Stmt, Type};

use crate::shape::{
    AggCol, AggregateKind, ChainQualifiers, ConflictTarget, ConflictUpdate, CteBinding,
    DistinctKind, HavingClause, JoinKind, JoinSpec, LockKind, OnConflictKind, OrderClause,
    OrderDir, ProjectionSpec, QueryShape, QuerySource, ReturningKind, SelectBranch, SetOpKind,
    Terminator,
};
use crate::util::{ExprExt, PathExt};
use crate::where_clause::{RowScope, SqlColRef};

pub(super) struct ColumnAssign {
    pub(super) column: String,
    pub(super) value_parts: Vec<proc_macro2::TokenStream>,
}

impl ColumnAssign {
    pub(super) fn collect(
        body: &Expr,
        row_var: &Ident,
        fn_params: &[Ident],
        param_tys: &[Type],
        captures: &mut Vec<Ident>,
        dialect: &crate::backend::Dialect,
        cte_map: &HashMap<String, Path>,
    ) -> syn::Result<Vec<Self>> {
        let mut out = Vec::new();
        match body {
            Expr::Block(eb) => {
                for stmt in &eb.block.stmts {
                    let assign_expr = match stmt {
                        Stmt::Expr(e, _) => e,
                        other => {
                            return Err(syn::Error::new(
                                other.span(),
                                "assign block may only contain `t.col = expr;` statements",
                            ));
                        }
                    };
                    out.push(Self::parse_single(
                        assign_expr,
                        row_var,
                        fn_params,
                        param_tys,
                        captures,
                        dialect,
                        cte_map,
                    )?);
                }
            }
            single => {
                out.push(Self::parse_single(
                    single, row_var, fn_params, param_tys, captures, dialect, cte_map,
                )?);
            }
        }
        Ok(out)
    }

    fn parse_single(
        expr: &Expr,
        row_var: &Ident,
        fn_params: &[Ident],
        param_tys: &[Type],
        captures: &mut Vec<Ident>,
        dialect: &crate::backend::Dialect,
        cte_map: &HashMap<String, Path>,
    ) -> syn::Result<Self> {
        let Expr::Assign(syn::ExprAssign { left, right, .. }) = expr else {
            return Err(syn::Error::new(
                expr.span(),
                "expected `t.col = <expr>` assignment",
            ));
        };
        let column = left.as_column_ref(row_var).ok_or_else(|| {
            syn::Error::new(
                left.span(),
                "left side of assignment must be `<row_var>.<column>`",
            )
        })?;
        let empty_scope = RowScope::single(row_var.clone());
        let value_parts = empty_scope.parse_side_expr(right, Some((fn_params, captures)), param_tys, dialect, cte_map).ok_or_else(|| {
            syn::Error::new(
                right.span(),
                "right side of assignment must be a fn-parameter, literal, or arithmetic/function expression",
            )
        })?;
        Ok(Self {
            column,
            value_parts,
        })
    }
}

impl CteBinding {
    pub(super) fn extract_from(f: &ItemFn) -> syn::Result<(Vec<Self>, Expr)> {
        let stmts = &f.block.stmts;
        if stmts.is_empty() {
            return Err(syn::Error::new(f.block.span(), "#[query] body is empty"));
        }
        let mut ctes = Vec::new();
        let last_idx = stmts.len() - 1;
        for (i, stmt) in stmts.iter().enumerate() {
            if i == last_idx {
                let Stmt::Expr(e, _) = stmt else {
                    return Err(syn::Error::new(
                        stmt.span(),
                        "#[query] body's last statement must be the main query expression (no trailing semicolon)",
                    ));
                };
                return Ok((ctes, e.clone()));
            }
            let Stmt::Local(local) = stmt else {
                return Err(syn::Error::new(
                    stmt.span(),
                    "#[query] body may only contain `let X = <chain>.cte();` bindings before the main query",
                ));
            };
            let Pat::Ident(pi) = &local.pat else {
                return Err(syn::Error::new(
                    local.pat.span(),
                    "CTE let-binding pattern must be a plain identifier",
                ));
            };
            let init = local.init.as_ref().ok_or_else(|| {
                syn::Error::new(local.span(), "CTE let-binding requires `= <chain>.cte();`")
            })?;
            let rhs = init.expr.as_ref();
            let Expr::MethodCall(mc) = rhs else {
                return Err(syn::Error::new(
                    rhs.span(),
                    "CTE let-binding rhs must end in `.cte()`",
                ));
            };
            if mc.method != "cte" || !mc.args.is_empty() {
                return Err(syn::Error::new(
                    mc.method.span(),
                    "CTE let-binding rhs must end in `.cte()` (no arguments)",
                ));
            }
            ctes.push(Self {
                name: pi.ident.clone(),
                inner_chain: (*mc.receiver).clone(),
            });
        }
        Err(syn::Error::new(
            f.block.span(),
            "#[query] body must end with the main query expression",
        ))
    }
}

impl QueryShape {
    pub(super) fn parse(expr: &Expr, cte_map: &HashMap<String, Path>) -> syn::Result<Self> {
        if let Expr::MethodCall(top) = expr {
            let method = top.method.to_string();

            if let Some(ret) = ReturningKind::from_method(&method) {
                if !top.args.is_empty() {
                    return Err(syn::Error::new(
                        top.args.span(),
                        format!("`.returning_{}()` takes no arguments", ret.name()),
                    ));
                }
                let inner = Self::parse(top.receiver.as_ref(), cte_map)?;
                return inner.attach_returning(ret, top.method.span());
            }
            if method == "on_conflict_do_nothing" {
                if !top.args.is_empty() {
                    return Err(syn::Error::new(
                        top.args.span(),
                        "`.on_conflict_do_nothing()` takes no arguments",
                    ));
                }
                let inner = Self::parse(top.receiver.as_ref(), cte_map)?;
                return inner.attach_on_conflict(OnConflictKind::DoNothing, top.method.span());
            }
            if method == "do_nothing" {
                if !top.args.is_empty() {
                    return Err(syn::Error::new(
                        top.args.span(),
                        "`.do_nothing()` takes no arguments",
                    ));
                }
                let (target, inner) =
                    ConflictTarget::parse_with_shape(top.receiver.as_ref(), cte_map)?;
                return inner.attach_on_conflict(
                    OnConflictKind::TargetDoNothing(target),
                    top.method.span(),
                );
            }
            if method == "do_update" {
                if top.args.len() != 1 {
                    return Err(syn::Error::new(
                        top.args.span(),
                        "`.do_update(...)` takes exactly one closure argument",
                    ));
                }
                let (set_arg, set_body) = top.args[0].as_closure_single("do_update")?;
                let (target, inner) =
                    ConflictTarget::parse_with_shape(top.receiver.as_ref(), cte_map)?;
                let upd = ConflictUpdate { set_arg, set_body };
                return inner.attach_on_conflict(
                    OnConflictKind::TargetDoUpdate(target, upd),
                    top.method.span(),
                );
            }
        }

        if let Expr::Call(ExprCall { func, args, .. }) = expr
            && let Expr::Path(ExprPath { path, .. }) = func.as_ref()
        {
            let last = path.segments.last().map(|s| s.ident.to_string());
            if last.as_deref() == Some("insert") {
                let table_path = path.without_last()?;
                if args.len() != 1 {
                    return Err(syn::Error::new(
                        args.span(),
                        "`insert` takes exactly one closure argument",
                    ));
                }
                let (closure_arg, body) = args[0].as_closure_single("insert")?;
                return Ok(Self::Insert {
                    table_path,
                    closure_arg,
                    body,
                    returning: ReturningKind::None,
                    on_conflict: OnConflictKind::None,
                });
            }
            if last.as_deref() == Some("insert_each") {
                let table_path = path.without_last()?;
                if args.len() != 1 {
                    return Err(syn::Error::new(
                        args.span(),
                        "`insert_each` takes exactly one closure argument",
                    ));
                }
                let (closure_arg, body) = args[0].as_closure_single("insert_each")?;
                return Ok(Self::InsertEach {
                    table_path,
                    closure_arg,
                    body,
                    returning: ReturningKind::None,
                    on_conflict: OnConflictKind::None,
                });
            }
            if last.as_deref() == Some("insert_from") {
                if args.len() != 2 {
                    return Err(syn::Error::new(
                        args.span(),
                        "`Table::insert_from(source, |t, src| { ... })` takes exactly two arguments",
                    ));
                }
                let table_path = path.without_last()?;
                let (source, source_pred_idents, source_predicate) =
                    QuerySource::parse_chain(&args[0], cte_map)?;
                let body_arity = 2 + source.joins.len();
                let (idents, body) = args[1].as_closure_n(body_arity, "insert_from")?;
                let target_arg = idents[0].clone();
                let source_idents = idents[1..].to_vec();
                return Ok(Self::InsertFrom {
                    table_path,
                    source,
                    source_pred_idents,
                    source_predicate,
                    target_arg,
                    source_idents,
                    body,
                    returning: ReturningKind::None,
                    on_conflict: OnConflictKind::None,
                });
            }
        }

        let Expr::MethodCall(top) = expr else {
            return Err(syn::Error::new(
                expr.span(),
                "expected `Table::filter(|t| ...).<term>()`, `Table::insert(|t| ...)`, or join chain",
            ));
        };
        let method_name = top.method.to_string();

        if method_name == "update" {
            if top.args.len() != 1 {
                return Err(syn::Error::new(
                    top.args.span(),
                    "`.update(...)` takes exactly one closure argument",
                ));
            }
            if let Some(shape) =
                Self::try_parse_filter_each_update(top.receiver.as_ref(), &top.args[0])?
            {
                return Ok(shape);
            }
            let (source, pred_idents, predicate) =
                QuerySource::parse_with_filter(top.receiver.as_ref(), "update", cte_map)?;
            let n = 1 + source.joins.len();
            let (set_idents, set_body) = top.args[0].as_closure_n(n, "update")?;
            return Ok(Self::Update {
                source,
                pred_idents,
                predicate,
                set_idents,
                set_body,
                returning: ReturningKind::None,
            });
        }

        if method_name == "delete" {
            if !top.args.is_empty() {
                return Err(syn::Error::new(
                    top.args.span(),
                    "`.delete()` takes no arguments",
                ));
            }
            let (source, pred_idents, predicate) =
                QuerySource::parse_with_filter(top.receiver.as_ref(), "delete", cte_map)?;
            return Ok(Self::Delete {
                source,
                pred_idents,
                predicate,
                returning: ReturningKind::None,
            });
        }

        if matches!(
            method_name.as_str(),
            "count" | "sum" | "avg" | "min" | "max"
        ) {
            let kind = AggregateKind::parse(&method_name, top)?;
            let mut qualifiers = ChainQualifiers::default();
            let mut current = qualifiers.peel(top.receiver.as_ref())?;
            let mut having: Option<HavingClause> = None;
            if let Expr::MethodCall(mc) = &current
                && mc.method == "having"
            {
                if mc.args.len() != 1 {
                    return Err(syn::Error::new(
                        mc.args.span(),
                        "`.having(...)` takes one closure argument",
                    ));
                }
                let (idents, body) = mc.args[0].as_closure_any_arity("having")?;
                if idents.is_empty() {
                    return Err(syn::Error::new(
                        mc.args[0].span(),
                        "`.having(...)` closure must take at least 2 args (row vars + agg handle)",
                    ));
                }
                let agg_arg = idents.last().expect("non-empty").clone();
                let row_args = idents[..idents.len() - 1].to_vec();
                having = Some(HavingClause {
                    row_args,
                    agg_arg,
                    pred: body,
                });
                current = (*mc.receiver).clone();
            }
            let mut group_by: Option<AggCol> = None;
            if let Expr::MethodCall(mc) = &current
                && mc.method == "group_by"
            {
                if mc.args.len() != 1 {
                    return Err(syn::Error::new(
                        mc.args.span(),
                        "`.group_by(...)` takes one closure argument",
                    ));
                }
                let (args, body) = mc.args[0].as_closure_any_arity("group_by")?;
                group_by = Some(AggCol { args, body });
                current = (*mc.receiver).clone();
            }
            let (source, pred_idents, predicate) = QuerySource::parse_chain(&current, cte_map)?;
            let n = 1 + source.joins.len();
            if let Some(gb) = &group_by
                && gb.args.len() != n
            {
                return Err(syn::Error::new(
                    gb.body.span(),
                    format!(
                        "`.group_by(...)` closure must take {n} parameter(s) to match the source"
                    ),
                ));
            }
            if let Some(h) = &having
                && h.row_args.len() != n
            {
                return Err(syn::Error::new(
                    h.pred.span(),
                    format!(
                        "`.having(...)` closure must take {} parameter(s) — {n} row var(s) + 1 agg handle",
                        n + 1
                    ),
                ));
            }
            if let Some(c) = kind.agg_col()
                && c.args.len() != n
            {
                return Err(syn::Error::new(
                    c.body.span(),
                    format!(
                        "aggregate column closure must take {n} parameter(s) to match the source"
                    ),
                ));
            }
            return Ok(Self::Aggregate(Box::new(crate::shape::Aggregate {
                source,
                pred_idents,
                predicate,
                kind,
                group_by,
                having,
                qualifiers,
            })));
        }

        let terminator = match method_name.as_str() {
            "one" => Terminator::One,
            "first" => Terminator::First,
            "all" => Terminator::All,
            "stream" => Terminator::Stream,
            other => {
                return Err(syn::Error::new(
                    top.method.span(),
                    format!(
                        "unknown method `.{other}()`; expected one/first/all/stream/count/update/delete"
                    ),
                ));
            }
        };
        if !top.args.is_empty() {
            return Err(syn::Error::new(
                top.args.span(),
                "terminator method must take no arguments",
            ));
        }
        let mut qualifiers = ChainQualifiers::default();
        let after_quals = qualifiers.peel(top.receiver.as_ref())?;

        if let Expr::MethodCall(mc) = &after_quals
            && SetOpKind::from_method(&mc.method.to_string()).is_some()
        {
            let (branches, ops) = SelectBranch::parse_set_op_chain(&after_quals, cte_map)?;
            return Ok(Self::SetOp {
                branches,
                ops,
                terminator,
                outer_qualifiers: qualifiers,
            });
        }

        let mut projection: Option<ProjectionSpec> = None;
        let mut after_select = after_quals;
        if let Expr::MethodCall(mc) = &after_select
            && mc.method == "select"
        {
            if mc.args.len() != 1 {
                return Err(syn::Error::new(
                    mc.args.span(),
                    "`.select(...)` takes one closure argument",
                ));
            }
            let inner_receiver = (*mc.receiver).clone();
            let inner_after_quals = qualifiers.peel(&inner_receiver)?;
            let (probe_source, _, _) = QuerySource::parse_chain(&inner_after_quals, cte_map)?;
            let n = 1 + probe_source.joins.len();
            let (idents, body) = mc.args[0].as_closure_n(n, "select")?;
            let elems = match &body {
                Expr::Tuple(t) => t.elems.iter().cloned().collect::<Vec<_>>(),
                single => vec![single.clone()],
            };
            projection = Some(ProjectionSpec { idents, elems });
            after_select = inner_after_quals;
        }

        let (source, pred_idents, predicate) = QuerySource::parse_chain(&after_select, cte_map)?;
        Ok(Self::Select {
            source,
            pred_idents,
            predicate,
            projection,
            terminator,
            qualifiers,
        })
    }

    fn attach_returning(self, ret: ReturningKind, span: proc_macro2::Span) -> syn::Result<Self> {
        match self {
            Self::Insert {
                table_path,
                closure_arg,
                body,
                returning: ReturningKind::None,
                on_conflict,
            } => Ok(Self::Insert {
                table_path,
                closure_arg,
                body,
                returning: ret,
                on_conflict,
            }),
            Self::InsertEach {
                table_path,
                closure_arg,
                body,
                returning: ReturningKind::None,
                on_conflict,
            } => Ok(Self::InsertEach {
                table_path,
                closure_arg,
                body,
                returning: ret,
                on_conflict,
            }),
            Self::InsertFrom {
                table_path,
                source,
                source_pred_idents,
                source_predicate,
                target_arg,
                source_idents,
                body,
                returning: ReturningKind::None,
                on_conflict,
            } => Ok(Self::InsertFrom {
                table_path,
                source,
                source_pred_idents,
                source_predicate,
                target_arg,
                source_idents,
                body,
                returning: ret,
                on_conflict,
            }),
            Self::Update {
                source,
                pred_idents,
                predicate,
                set_idents,
                set_body,
                returning: ReturningKind::None,
            } => Ok(Self::Update {
                source,
                pred_idents,
                predicate,
                set_idents,
                set_body,
                returning: ret,
            }),
            Self::Delete {
                source,
                pred_idents,
                predicate,
                returning: ReturningKind::None,
            } => Ok(Self::Delete {
                source,
                pred_idents,
                predicate,
                returning: ret,
            }),
            Self::UpdateEach {
                source,
                data_sources,
                row_var,
                col_vars,
                predicate,
                set_body,
                returning: ReturningKind::None,
            } => Ok(Self::UpdateEach {
                source,
                data_sources,
                row_var,
                col_vars,
                predicate,
                set_body,
                returning: ret,
            }),
            Self::Insert { .. }
            | Self::InsertEach { .. }
            | Self::InsertFrom { .. }
            | Self::Update { .. }
            | Self::UpdateEach { .. }
            | Self::Delete { .. } => Err(syn::Error::new(
                span,
                "duplicate `.returning_*()` — only one is allowed per mutation",
            )),
            Self::Select { .. } | Self::Aggregate(_) | Self::SetOp { .. } => Err(syn::Error::new(
                span,
                "`.returning_*()` follows mutations only; use `.one() / .first() / .all() / .count()` after `.filter(...)`",
            )),
        }
    }

    fn attach_on_conflict(
        self,
        on_conflict: OnConflictKind,
        span: proc_macro2::Span,
    ) -> syn::Result<Self> {
        match self {
            Self::Insert {
                table_path,
                closure_arg,
                body,
                returning,
                on_conflict: OnConflictKind::None,
            } => Ok(Self::Insert {
                table_path,
                closure_arg,
                body,
                returning,
                on_conflict,
            }),
            Self::InsertEach {
                table_path,
                closure_arg,
                body,
                returning,
                on_conflict: OnConflictKind::None,
            } => Ok(Self::InsertEach {
                table_path,
                closure_arg,
                body,
                returning,
                on_conflict,
            }),
            Self::InsertFrom {
                table_path,
                source,
                source_pred_idents,
                source_predicate,
                target_arg,
                source_idents,
                body,
                returning,
                on_conflict: OnConflictKind::None,
            } => Ok(Self::InsertFrom {
                table_path,
                source,
                source_pred_idents,
                source_predicate,
                target_arg,
                source_idents,
                body,
                returning,
                on_conflict,
            }),
            Self::Insert { .. } | Self::InsertEach { .. } | Self::InsertFrom { .. } => {
                Err(syn::Error::new(span, "duplicate ON CONFLICT clause"))
            }
            _ => Err(syn::Error::new(
                span,
                "ON CONFLICT clause follows `Table::insert(...)` / `Table::insert_each(...)` / `Table::insert_from(...)` only",
            )),
        }
    }

    fn try_parse_filter_each_update(
        receiver: &Expr,
        update_arg: &Expr,
    ) -> syn::Result<Option<Self>> {
        let Expr::Call(ExprCall { func, args, .. }) = receiver else {
            return Ok(None);
        };
        let Expr::Path(ExprPath { path, .. }) = func.as_ref() else {
            return Ok(None);
        };
        if path.segments.last().map(|s| s.ident.to_string()).as_deref() != Some("filter_each") {
            return Ok(None);
        }
        if args.len() != 2 {
            return Err(syn::Error::new(
                args.span(),
                "`Table::filter_each((cols...), |row, c0...| pred)` takes exactly two arguments",
            ));
        }
        let table_path = path.without_last()?;
        let source = QuerySource {
            primary_path: table_path,
            primary_alias: None,
            joins: Vec::new(),
        };
        let data_sources: Vec<Expr> = match &args[0] {
            Expr::Tuple(t) => t.elems.iter().cloned().collect(),
            single => vec![single.clone()],
        };
        if data_sources.is_empty() {
            return Err(syn::Error::new(
                args[0].span(),
                "`filter_each` first argument must list at least one `&[T]` column source",
            ));
        }
        let arity = 1 + data_sources.len();
        let (pred_idents, predicate) = args[1].as_closure_n(arity, "filter_each")?;
        let (set_idents, set_body) = update_arg.as_closure_n(arity, "update")?;
        for (a, b) in pred_idents.iter().zip(set_idents.iter()) {
            if a != b {
                return Err(syn::Error::new(
                    b.span(),
                    "`filter_each` and `.update` closures must use the same parameter names",
                ));
            }
        }
        let row_var = pred_idents[0].clone();
        let col_vars = pred_idents[1..].to_vec();
        Ok(Some(Self::UpdateEach {
            source,
            data_sources,
            row_var,
            col_vars,
            predicate,
            set_body,
            returning: ReturningKind::None,
        }))
    }
}

impl SelectBranch {
    fn parse_set_op_chain(
        expr: &Expr,
        cte_map: &HashMap<String, Path>,
    ) -> syn::Result<(Vec<Self>, Vec<SetOpKind>)> {
        let mut branches = Vec::new();
        let mut ops = Vec::new();
        Self::walk(expr, &mut branches, &mut ops, cte_map)?;
        Ok((branches, ops))
    }

    fn walk(
        expr: &Expr,
        branches: &mut Vec<Self>,
        ops: &mut Vec<SetOpKind>,
        cte_map: &HashMap<String, Path>,
    ) -> syn::Result<()> {
        if let Expr::MethodCall(mc) = expr
            && let Some(op) = SetOpKind::from_method(&mc.method.to_string())
        {
            if mc.args.len() != 1 {
                return Err(syn::Error::new(
                    mc.args.span(),
                    format!(
                        "`.{}(...)` takes one argument: another `Table::filter(|t| ...)` chain",
                        mc.method,
                    ),
                ));
            }
            Self::walk(mc.receiver.as_ref(), branches, ops, cte_map)?;
            ops.push(op);
            let (source, pred_idents, predicate) = QuerySource::parse_chain(&mc.args[0], cte_map)?;
            branches.push(Self {
                source,
                pred_idents,
                predicate,
            });
            return Ok(());
        }
        let (source, pred_idents, predicate) = QuerySource::parse_chain(expr, cte_map)?;
        branches.push(Self {
            source,
            pred_idents,
            predicate,
        });
        Ok(())
    }
}

impl AggregateKind {
    fn parse(method_name: &str, top: &syn::ExprMethodCall) -> syn::Result<Self> {
        if method_name == "count" {
            if !top.args.is_empty() {
                return Err(syn::Error::new(
                    top.args.span(),
                    "`.count()` takes no arguments",
                ));
            }
            return Ok(Self::Count);
        }
        if top.args.len() != 1 {
            return Err(syn::Error::new(
                top.args.span(),
                format!("`.{method_name}(...)` takes one closure argument"),
            ));
        }
        let (args, body) = top.args[0].as_closure_any_arity(method_name)?;
        let agg_col = AggCol { args, body };
        Ok(match method_name {
            "sum" => Self::Sum(agg_col),
            "avg" => Self::Avg(agg_col),
            "min" => Self::Min(agg_col),
            "max" => Self::Max(agg_col),
            _ => unreachable!(),
        })
    }
}

impl ChainQualifiers {
    pub(super) fn peel(&mut self, expr: &Expr) -> syn::Result<Expr> {
        let mut current = match expr {
            Expr::Block(eb) if eb.block.stmts.len() == 1 => match &eb.block.stmts[0] {
                Stmt::Expr(inner, _) => inner.clone(),
                _ => expr.clone(),
            },
            _ => expr.clone(),
        };
        let mut order_by_buf: Vec<OrderClause> = Vec::new();
        while let Expr::MethodCall(mc) = current {
            let method = mc.method.to_string();
            match method.as_str() {
                "order_by" | "order_by_desc" => {
                    if mc.args.len() != 1 {
                        return Err(syn::Error::new(
                            mc.args.span(),
                            format!("`.{method}(...)` takes exactly one closure argument"),
                        ));
                    }
                    let dir = if method == "order_by" {
                        OrderDir::Asc
                    } else {
                        OrderDir::Desc
                    };
                    order_by_buf.push(OrderClause {
                        closure: mc.args[0].clone(),
                        dir,
                    });
                }
                "limit" => {
                    if mc.args.len() != 1 {
                        return Err(syn::Error::new(
                            mc.args.span(),
                            "`.limit(n)` takes exactly one int / fn-param argument",
                        ));
                    }
                    if self.limit.is_some() {
                        return Err(syn::Error::new(mc.method.span(), "duplicate `.limit(...)`"));
                    }
                    self.limit = Some(mc.args[0].clone());
                }
                "offset" => {
                    if mc.args.len() != 1 {
                        return Err(syn::Error::new(
                            mc.args.span(),
                            "`.offset(n)` takes exactly one int / fn-param argument",
                        ));
                    }
                    if self.offset.is_some() {
                        return Err(syn::Error::new(
                            mc.method.span(),
                            "duplicate `.offset(...)`",
                        ));
                    }
                    self.offset = Some(mc.args[0].clone());
                }
                "distinct" => {
                    if !mc.args.is_empty() {
                        return Err(syn::Error::new(
                            mc.args.span(),
                            "`.distinct()` takes no arguments",
                        ));
                    }
                    if !matches!(self.distinct, DistinctKind::None) {
                        return Err(syn::Error::new(
                            mc.method.span(),
                            "duplicate `.distinct(...)`",
                        ));
                    }
                    self.distinct = DistinctKind::All;
                }
                "distinct_on" => {
                    if mc.args.len() != 1 {
                        return Err(syn::Error::new(
                            mc.args.span(),
                            "`.distinct_on(...)` takes one closure argument",
                        ));
                    }
                    if !matches!(self.distinct, DistinctKind::None) {
                        return Err(syn::Error::new(
                            mc.method.span(),
                            "duplicate `.distinct(...)`",
                        ));
                    }
                    let (arg, body) = mc.args[0].as_closure_single("distinct_on")?;
                    self.distinct = DistinctKind::On(arg, body);
                }
                "for_update" | "for_share" => {
                    if !mc.args.is_empty() {
                        return Err(syn::Error::new(
                            mc.args.span(),
                            format!("`.{method}()` takes no arguments"),
                        ));
                    }
                    if !matches!(self.lock, LockKind::None) {
                        return Err(syn::Error::new(
                            mc.method.span(),
                            "duplicate row-lock qualifier",
                        ));
                    }
                    self.lock = if method == "for_update" {
                        LockKind::ForUpdate
                    } else {
                        LockKind::ForShare
                    };
                }
                _ => {
                    current = Expr::MethodCall(mc);
                    break;
                }
            }
            current = *mc.receiver;
        }
        order_by_buf.reverse();
        self.order_by.extend(order_by_buf);
        Ok(current)
    }
}

impl QuerySource {
    pub(super) fn parse_chain(
        expr: &Expr,
        cte_map: &HashMap<String, Path>,
    ) -> syn::Result<(Self, Vec<Ident>, Expr)> {
        if let Expr::Block(eb) = expr {
            let stmts = &eb.block.stmts;
            if stmts.len() == 1
                && let Stmt::Expr(inner, _) = &stmts[0]
            {
                return Self::parse_chain(inner, cte_map);
            }
        }

        if let Expr::Path(ExprPath { path, .. }) = expr
            && path.segments.len() == 1
        {
            let id = &path.segments[0].ident;
            if let Some(table_path) = cte_map.get(&id.to_string()) {
                let dummy_arg = Ident::new("__cte", id.span());
                let true_pred: Expr = syn::parse_quote!(true);
                return Ok((
                    Self {
                        primary_path: table_path.clone(),
                        primary_alias: Some(id.clone()),
                        joins: Vec::new(),
                    },
                    vec![dummy_arg],
                    true_pred,
                ));
            }
        }

        if let Expr::Call(call) = expr {
            let (table_path, closure_arg, predicate) = Self::parse_filter_call(call)?;
            let source = Self {
                primary_path: table_path,
                primary_alias: None,
                joins: Vec::new(),
            };
            return Ok((source, vec![closure_arg], predicate));
        }
        if let Expr::MethodCall(mc) = expr
            && mc.method == "filter"
        {
            if mc.args.len() != 1 {
                return Err(syn::Error::new(
                    mc.args.span(),
                    "`.filter(...)` takes one closure argument",
                ));
            }
            if let Expr::Path(ExprPath { path, .. }) = mc.receiver.as_ref()
                && path.segments.len() == 1
            {
                let id = &path.segments[0].ident;
                if let Some(table_path) = cte_map.get(&id.to_string()) {
                    let (pred_idents, body) = mc.args[0].as_closure_n(1, "filter")?;
                    return Ok((
                        Self {
                            primary_path: table_path.clone(),
                            primary_alias: Some(id.clone()),
                            joins: Vec::new(),
                        },
                        pred_idents,
                        body,
                    ));
                }
            }
            let source = Self::parse_join_chain(mc.receiver.as_ref())?;
            let n = 1 + source.joins.len();
            let (pred_idents, body) = mc.args[0].as_closure_n(n, "filter")?;
            return Ok((source, pred_idents, body));
        }
        Err(syn::Error::new(
            expr.span(),
            "expected `Table::filter(|t| ...)`, `Table::join::<X>(|p,u| ...).filter(|p,u| ...)`, or a CTE name",
        ))
    }

    fn parse_with_filter(
        expr: &Expr,
        terminator_name: &str,
        cte_map: &HashMap<String, Path>,
    ) -> syn::Result<(Self, Vec<Ident>, Expr)> {
        Self::parse_chain(expr, cte_map).map_err(|e| {
            syn::Error::new(
                e.span(),
                format!("`.{terminator_name}(...)` source is invalid: {}", e,),
            )
        })
    }

    fn parse_join_chain(expr: &Expr) -> syn::Result<Self> {
        let mut outer_chain: Vec<syn::ExprMethodCall> = Vec::new();
        let mut current = expr.clone();
        loop {
            match current {
                Expr::MethodCall(mc) => {
                    outer_chain.push(mc.clone());
                    current = (*mc.receiver).clone();
                }
                Expr::Call(call) => {
                    let (primary_path, joined_path, kind, on_idents, cond) =
                        JoinSpec::parse_call(&call)?;
                    let mut joins = vec![JoinSpec {
                        kind,
                        path: joined_path,
                        on_idents,
                        cond,
                    }];
                    outer_chain.reverse();
                    for (prior_count, mc) in (2..).zip(outer_chain) {
                        let kind = JoinKind::from_method(&mc.method)?;
                        let joined_path = JoinSpec::parse_method_turbofish(&mc)?;
                        if mc.args.len() != 1 {
                            return Err(syn::Error::new(
                                mc.args.span(),
                                format!("`.{}(...)` takes one closure argument", mc.method),
                            ));
                        }
                        let expected_n = if kind.is_lateral() {
                            prior_count
                        } else {
                            prior_count + 1
                        };
                        let (idents, body) =
                            mc.args[0].as_closure_n(expected_n, &mc.method.to_string())?;
                        joins.push(JoinSpec {
                            kind,
                            path: joined_path,
                            on_idents: idents,
                            cond: body,
                        });
                    }
                    return Ok(Self {
                        primary_path,
                        primary_alias: None,
                        joins,
                    });
                }
                other => {
                    return Err(syn::Error::new(
                        other.span(),
                        "join chain must start with `Table::join::<X>(...)` (or `left_join` / `right_join` / `full_join`)",
                    ));
                }
            }
        }
    }

    pub(super) fn parse_filter_call(call: &ExprCall) -> syn::Result<(Path, Ident, Expr)> {
        let Expr::Path(ExprPath {
            path: filter_path, ..
        }) = call.func.as_ref()
        else {
            return Err(syn::Error::new(
                call.func.span(),
                "expected path `Table::filter`",
            ));
        };
        if filter_path
            .segments
            .last()
            .map(|s| s.ident.to_string())
            .as_deref()
            != Some("filter")
        {
            return Err(syn::Error::new(
                filter_path.span(),
                "expected `Table::filter` (last segment must be `filter`)",
            ));
        }
        let table_path = filter_path.without_last()?;
        if call.args.len() != 1 {
            return Err(syn::Error::new(
                call.args.span(),
                "`filter` takes exactly one closure argument",
            ));
        }
        let (closure_arg, body) = call.args[0].as_closure_single("filter")?;
        Ok((table_path, closure_arg, body))
    }
}

impl JoinSpec {
    fn parse_method_turbofish(mc: &syn::ExprMethodCall) -> syn::Result<Path> {
        let Some(turbofish) = &mc.turbofish else {
            return Err(syn::Error::new(
                mc.method.span(),
                "join must specify the joined table via turbofish: `.join::<Table>(...)`",
            ));
        };
        if turbofish.args.len() != 1 {
            return Err(syn::Error::new(
                turbofish.span(),
                "`.join` turbofish must have exactly one type argument",
            ));
        }
        let syn::GenericArgument::Type(joined_ty) = &turbofish.args[0] else {
            return Err(syn::Error::new(
                turbofish.args[0].span(),
                "`.join` turbofish argument must be a type",
            ));
        };
        let Type::Path(syn::TypePath {
            path, qself: None, ..
        }) = joined_ty
        else {
            return Err(syn::Error::new(
                joined_ty.span(),
                "`.join` turbofish argument must be a plain Table path",
            ));
        };
        Ok(path.clone())
    }

    fn parse_call(call: &ExprCall) -> syn::Result<(Path, Path, JoinKind, Vec<Ident>, Expr)> {
        let Expr::Path(ExprPath { path, .. }) = call.func.as_ref() else {
            return Err(syn::Error::new(
                call.func.span(),
                "expected `Table1::<join_kind>::<Table2>(...)` path",
            ));
        };
        let last = path
            .segments
            .last()
            .ok_or_else(|| syn::Error::new(path.span(), "empty path"))?;
        let kind = match last.ident.to_string().as_str() {
            "join" => JoinKind::Inner,
            "left_join" => JoinKind::Left,
            "right_join" => JoinKind::Right,
            "full_join" => JoinKind::Full,
            "lateral_join" => JoinKind::LateralInner,
            "lateral_left_join" => JoinKind::LateralLeft,
            other => {
                return Err(syn::Error::new(
                    last.ident.span(),
                    format!(
                        "expected `::join` / `::left_join` / `::right_join` / `::full_join` / `::lateral_join` / `::lateral_left_join`, got `::{other}`"
                    ),
                ));
            }
        };
        let syn::PathArguments::AngleBracketed(turbofish) = &last.arguments else {
            return Err(syn::Error::new(
                last.span(),
                "join must specify the joined table via turbofish: `Table1::join::<Table2>(...)`",
            ));
        };
        if turbofish.args.len() != 1 {
            return Err(syn::Error::new(
                turbofish.span(),
                "`join` turbofish must have exactly one type argument",
            ));
        }
        let syn::GenericArgument::Type(joined_ty) = &turbofish.args[0] else {
            return Err(syn::Error::new(
                turbofish.args[0].span(),
                "`join` turbofish argument must be a type",
            ));
        };
        let Type::Path(syn::TypePath {
            path: joined_path,
            qself: None,
            ..
        }) = joined_ty
        else {
            return Err(syn::Error::new(
                joined_ty.span(),
                "`join` turbofish argument must be a plain Table path",
            ));
        };

        let primary_path = path.without_last()?;

        if call.args.len() != 1 {
            return Err(syn::Error::new(
                call.args.span(),
                "`join` takes exactly one closure argument",
            ));
        }
        let method_name = last.ident.to_string();
        let n = if kind.is_lateral() { 1 } else { 2 };
        let (idents, body) = call.args[0].as_closure_n(n, &method_name)?;
        Ok((primary_path, joined_path.clone(), kind, idents, body))
    }
}

impl ConflictTarget {
    fn parse_with_shape(
        expr: &Expr,
        cte_map: &HashMap<String, Path>,
    ) -> syn::Result<(Self, QueryShape)> {
        let Expr::MethodCall(mc) = expr else {
            return Err(syn::Error::new(
                expr.span(),
                "`.do_nothing()` / `.do_update(...)` must follow `.on_conflict(|u| u.col)`",
            ));
        };
        if mc.method != "on_conflict" {
            return Err(syn::Error::new(
                mc.method.span(),
                "expected `.on_conflict(|u| u.col)` before `.do_nothing()` / `.do_update(...)`",
            ));
        }
        if mc.args.len() != 1 {
            return Err(syn::Error::new(
                mc.args.span(),
                "`.on_conflict(...)` takes one closure argument",
            ));
        }
        let (arg, body) = mc.args[0].as_closure_single("on_conflict")?;
        let cols = Self::parse_target_body(&arg, &body)?;
        let inner = QueryShape::parse(mc.receiver.as_ref(), cte_map)?;
        Ok((Self { cols }, inner))
    }

    fn parse_target_body(arg: &Ident, body: &Expr) -> syn::Result<Vec<String>> {
        let scope = RowScope::single(arg.clone());
        let extract = |e: &Expr| -> syn::Result<String> {
            let col = scope.column_ref(e).ok_or_else(|| {
                syn::Error::new(
                    e.span(),
                    "`.on_conflict` body element must be `<arg>.<col>`",
                )
            })?;
            match col {
                SqlColRef::Bare(s) => Ok(s),
                SqlColRef::Qualified(_, _) => Err(syn::Error::new(
                    e.span(),
                    "internal: unexpected qualified col in conflict target",
                )),
            }
        };
        match body {
            Expr::Tuple(t) => {
                let mut out = Vec::with_capacity(t.elems.len());
                for el in &t.elems {
                    out.push(extract(el)?);
                }
                Ok(out)
            }
            single => Ok(vec![extract(single)?]),
        }
    }
}

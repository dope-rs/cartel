use std::collections::HashMap;
use std::slice;

use quote::quote;
use syn::spanned::Spanned;
use syn::{Expr, ExprPath, Ident, Path, ReturnType, Stmt, Type};

use crate::backend::{Dialect, ParamCtx};
use crate::emit::{DecodeKind, DispatchKind, QueryPlan};
use crate::parse::ColumnAssign;
use crate::row_meta::RowTyExt;
use crate::shape::{
    AggCol, AggregateKind, ChainQualifiers, DistinctKind, HavingClause, JoinSpec, OnConflictKind,
    OrderClause, OrderDir, ProjectionSpec, QueryShape, QuerySource, ReturnShape, ReturningKind,
    SelectBranch, SetOpKind, Terminator,
};
use crate::util::{CaptureSet, ExprExt, FnParamsExt, PatExt};
use crate::where_clause::{RowScope, RowVar, WhereExpr};

pub(super) struct PlanBuilder<'a> {
    fn_params: &'a [Ident],
    param_tys: &'a [Type],
    captures: &'a mut Vec<Ident>,
    return_type: &'a ReturnType,
    dialect: &'a Dialect,
    cte_map: &'a HashMap<String, Path>,
}

impl QueryShape {
    pub(super) fn build_plan(
        &self,
        fn_params: &[Ident],
        param_tys: &[Type],
        captures: &mut Vec<Ident>,
        return_type: &ReturnType,
        dialect: &Dialect,
        cte_map: &HashMap<String, Path>,
    ) -> syn::Result<QueryPlan> {
        let mut pb = PlanBuilder {
            fn_params,
            param_tys,
            captures,
            return_type,
            dialect,
            cte_map,
        };
        match self {
            Self::Select {
                source,
                pred_idents,
                predicate,
                projection,
                terminator,
                qualifiers,
            } => pb.build_select(
                source,
                pred_idents,
                predicate,
                projection.as_ref(),
                *terminator,
                qualifiers,
            ),
            Self::Update {
                source,
                pred_idents,
                predicate,
                set_idents,
                set_body,
                returning,
            } => pb.build_update(
                source,
                pred_idents,
                predicate,
                set_idents,
                set_body,
                *returning,
            ),
            Self::Delete {
                source,
                pred_idents,
                predicate,
                returning,
            } => pb.build_delete(source, pred_idents, predicate, *returning),
            Self::UpdateEach {
                source,
                data_sources,
                row_var,
                col_vars,
                predicate,
                set_body,
                returning,
            } => pb.build_update_each(
                source,
                data_sources,
                row_var,
                col_vars,
                predicate,
                set_body,
                *returning,
            ),
            Self::Insert {
                table_path,
                closure_arg,
                body,
                returning,
                on_conflict,
            } => pb.build_insert(table_path, closure_arg, body, *returning, on_conflict),
            Self::InsertEach {
                table_path,
                closure_arg,
                body,
                returning,
                on_conflict,
            } => pb.build_insert_each(table_path, closure_arg, body, *returning, on_conflict),
            Self::InsertFrom {
                table_path,
                source,
                source_pred_idents,
                source_predicate,
                target_arg,
                source_idents,
                body,
                returning,
                on_conflict,
            } => pb.build_insert_from(
                table_path,
                source,
                source_pred_idents,
                source_predicate,
                target_arg,
                source_idents,
                body,
                *returning,
                on_conflict,
            ),
            Self::Aggregate(agg) => pb.build_aggregate(
                &agg.source,
                &agg.pred_idents,
                &agg.predicate,
                &agg.kind,
                agg.group_by.as_ref(),
                agg.having.as_ref(),
                &agg.qualifiers,
            ),
            Self::SetOp {
                branches,
                ops,
                terminator,
                outer_qualifiers,
            } => pb.build_set_op(branches, ops, *terminator, outer_qualifiers),
        }
    }
}

impl<'a> PlanBuilder<'a> {
    fn build_set_op(
        &mut self,
        branches: &[SelectBranch],
        ops: &[SetOpKind],
        terminator: Terminator,
        outer_qualifiers: &ChainQualifiers,
    ) -> syn::Result<QueryPlan> {
        debug_assert_eq!(branches.len(), ops.len() + 1);

        let outer = match self.return_type {
            ReturnType::Type(_, t) => (**t).clone(),
            ReturnType::Default => {
                return Err(syn::Error::new(
                    self.return_type.span(),
                    "set-op (UNION / INTERSECT / EXCEPT) queries require an explicit return type",
                ));
            }
        };
        let return_shape = ReturnShape::parse(&outer);
        let row_ty = return_shape.row_ty().clone();

        match (&return_shape, terminator) {
            (ReturnShape::Plain(_), Terminator::One)
            | (ReturnShape::Optional(_), Terminator::First)
            | (ReturnShape::Many(_), Terminator::All)
            | (ReturnShape::Stream(_), Terminator::Stream) => {}
            (rs, t) => {
                return Err(syn::Error::new(
                    self.return_type.span(),
                    format!(
                        "return type {} does not match terminator `.{}()`; expected {}",
                        rs.describe_actual(),
                        t.name(),
                        t.expected_return(),
                    ),
                ));
            }
        }

        let first_path = &branches[0].source.primary_path;
        let first_path_str = quote! { #first_path }.to_string();
        for (i, b) in branches.iter().enumerate().skip(1) {
            let bp = &b.source.primary_path;
            if quote! { #bp }.to_string() != first_path_str {
                return Err(syn::Error::new(
                    b.predicate.span(),
                    format!(
                        "set-op branch #{} table differs from the first branch ({} vs {})",
                        i + 1,
                        quote! { #bp },
                        quote! { #first_path },
                    ),
                ));
            }
            if !b.source.joins.is_empty() {
                return Err(syn::Error::new(
                    b.predicate.span(),
                    "set-op branch with joined source not supported in v0",
                ));
            }
        }
        if !branches[0].source.joins.is_empty() {
            return Err(syn::Error::new(
                branches[0].predicate.span(),
                "set-op branch with joined source not supported in v0",
            ));
        }

        let mut sql_parts: Vec<proc_macro2::TokenStream> = Vec::new();
        for (i, b) in branches.iter().enumerate() {
            if i > 0 {
                sql_parts.push(ops[i - 1].sql_ref(self.dialect));
            }
            let bp = &b.source.primary_path;
            let scope = RowScope::for_source(&b.source, &b.pred_idents);

            sql_parts.push(self.dialect.kw("SETOP_BRANCH_OPEN"));
            sql_parts.push(quote! { <#bp>::__CARTEL_SELECT_COLS });
            sql_parts.push(self.dialect.kw("FROM_KW"));
            sql_parts.extend(b.source.render_primary_table());
            if !b.predicate.is_synthetic_true() {
                let where_expr = WhereExpr::build(
                    &b.predicate,
                    &scope,
                    self.fn_params,
                    self.param_tys,
                    &mut *self.captures,
                    false,
                    self.dialect,
                    self.cte_map,
                )?;
                let mut where_parts = Vec::new();
                let ctx = ParamCtx {
                    captures: &*self.captures,
                    param_ids: self.fn_params,
                    param_tys: self.param_tys,
                };
                where_expr.render_parts(&mut where_parts, &ctx, self.dialect);
                sql_parts.push(self.dialect.kw("WHERE_KW"));
                sql_parts.extend(where_parts);
            }
            sql_parts.push(self.dialect.kw("SETOP_BRANCH_CLOSE"));
        }

        if !outer_qualifiers.order_by.is_empty() {
            sql_parts.push(self.dialect.kw("ORDER_BY_KW"));
            for (i, clause) in outer_qualifiers.order_by.iter().enumerate() {
                if i > 0 {
                    sql_parts.push(self.dialect.kw("COMMA"));
                }
                let Expr::Closure(c) = &clause.closure else {
                    return Err(syn::Error::new(
                        clause.closure.span(),
                        "set-op `.order_by(...)` argument must be a closure `|u| u.col`",
                    ));
                };
                if c.inputs.len() != 1 {
                    return Err(syn::Error::new(
                        c.inputs.span(),
                        "set-op `.order_by(...)` closure must take one parameter",
                    ));
                }
                let arg = c.inputs[0].closure_ident()?;
                let scope = RowScope::single(arg);
                let col = scope.column_ref(&c.body).ok_or_else(|| {
                    syn::Error::new(
                        c.body.span(),
                        "set-op `.order_by(...)` body must be a single column reference like `u.col`",
                    )
                })?;
                col.append_to(&mut sql_parts);
                if matches!(clause.dir, OrderDir::Desc) {
                    sql_parts.push(self.dialect.kw("DESC"));
                }
            }
        }
        sql_parts.extend(self.render_limit_offset(outer_qualifiers, Some(terminator))?);

        let dispatch = match terminator {
            Terminator::One => DispatchKind::One(row_ty.clone()),
            Terminator::First => DispatchKind::First(row_ty.clone()),
            Terminator::All => DispatchKind::All(row_ty.clone()),
            Terminator::Stream => DispatchKind::Stream(row_ty.clone()),
        };

        Ok(QueryPlan {
            row_ty: row_ty.clone(),
            sql_parts,
            n_result_cols: row_ty.n_cols_const(),
            decode: DecodeKind::Row(Box::new(row_ty.clone())),
            dispatch,
            probe_override: None,
        })
    }

    fn build_aggregate(
        &mut self,
        source: &QuerySource,
        pred_idents: &[Ident],
        predicate: &Expr,
        kind: &AggregateKind,
        group_by: Option<&AggCol>,
        having: Option<&HavingClause>,
        qualifiers: &ChainQualifiers,
    ) -> syn::Result<QueryPlan> {
        let outer = match self.return_type {
            ReturnType::Type(_, t) => (**t).clone(),
            ReturnType::Default => {
                return Err(syn::Error::new(
                    self.return_type.span(),
                    "aggregate query needs an explicit return type (e.g. `-> i64` for `.count()`)",
                ));
            }
        };
        let return_shape = ReturnShape::parse(&outer);
        let row_ty = return_shape.row_ty().clone();

        let scope = RowScope::for_source(source, pred_idents);

        let mut on_parts_per_join: Vec<Vec<proc_macro2::TokenStream>> =
            Vec::with_capacity(source.joins.len());
        for (i, j) in source.joins.iter().enumerate() {
            if j.kind.is_lateral() {
                let parts = self.render_lateral_subquery(j, i, source)?;
                on_parts_per_join.push(parts);
            } else {
                let on_scope = RowScope::for_join_on(source, i);
                let mut throwaway: Vec<Ident> = Vec::new();
                let on_expr = WhereExpr::build(
                    &j.cond,
                    &on_scope,
                    self.fn_params,
                    self.param_tys,
                    &mut throwaway,
                    true,
                    self.dialect,
                    self.cte_map,
                )?;
                if !throwaway.is_empty() {
                    return Err(syn::Error::new(
                        j.cond.span(),
                        "JOIN ON clause must compare columns only; bound parameters belong in `.filter(...)`",
                    ));
                }
                let mut parts = Vec::new();
                let ctx = ParamCtx {
                    captures: &*self.captures,
                    param_ids: self.fn_params,
                    param_tys: self.param_tys,
                };
                on_expr.render_parts(&mut parts, &ctx, self.dialect);
                on_parts_per_join.push(parts);
            }
        }

        let where_expr = WhereExpr::build(
            predicate,
            &scope,
            self.fn_params,
            self.param_tys,
            &mut *self.captures,
            false,
            self.dialect,
            self.cte_map,
        )?;
        let mut where_parts = Vec::new();
        {
            let ctx = ParamCtx {
                captures: &*self.captures,
                param_ids: self.fn_params,
                param_tys: self.param_tys,
            };
            where_expr.render_parts(&mut where_parts, &ctx, self.dialect);
        }

        let group_key_parts = match group_by {
            Some(gb) => {
                let gb_scope = RowScope::for_source(source, &gb.args);
                let inner = gb_scope
                    .parse_side_expr(
                        &gb.body,
                        Some((self.fn_params, &mut *self.captures)),
                        self.param_tys,
                        self.dialect,
                        self.cte_map,
                    )
                    .ok_or_else(|| {
                        syn::Error::new(
                            gb.body.span(),
                            "`.group_by(...)` body must be a column / arithmetic / function expression",
                        )
                    })?;
                Some(inner)
            }
            None => None,
        };

        let mut sql_parts = vec![self.dialect.kw("SELECT_KW")];
        if let Some(parts) = &group_key_parts {
            sql_parts.extend(parts.iter().cloned());
            sql_parts.push(self.dialect.kw("COMMA"));
        }
        match kind.fn_name() {
            None => sql_parts.push(self.dialect.kw("COUNT_STAR")),
            Some(fn_name) => {
                let agg = kind.agg_col().expect("non-Count agg has AggCol");
                let agg_scope = RowScope::for_source(source, &agg.args);
                let inner = agg_scope
                    .parse_side_expr(
                        &agg.body,
                        Some((self.fn_params, &mut *self.captures)),
                        self.param_tys,
                        self.dialect,
                        self.cte_map,
                    )
                    .ok_or_else(|| {
                        syn::Error::new(
                            agg.body.span(),
                            format!(
                                "`.{}(...)` body must be a column / arithmetic / function expression",
                                fn_name.to_lowercase()
                            ),
                        )
                    })?;
                let prefix = format!("{fn_name}(");
                sql_parts.push(quote! { #prefix });
                sql_parts.extend(inner);
                sql_parts.push(self.dialect.kw("PAREN_CLOSE"));
            }
        }
        sql_parts.extend(Self::render_from_clause_with_on(
            source,
            &on_parts_per_join,
            self.dialect,
        ));
        sql_parts.push(self.dialect.kw("WHERE_KW"));
        sql_parts.extend(where_parts);
        if let Some(parts) = &group_key_parts {
            sql_parts.push(self.dialect.kw("GROUP_BY_KW"));
            sql_parts.extend(parts.iter().cloned());
        }
        if let Some(h) = having {
            if group_by.is_none() {
                return Err(syn::Error::new(
                    h.pred.span(),
                    "`.having(...)` requires a preceding `.group_by(...)`",
                ));
            }
            let h_scope = RowScope::for_source(source, &h.row_args).with_agg(h.agg_arg.clone());
            let h_expr = WhereExpr::build(
                &h.pred,
                &h_scope,
                self.fn_params,
                self.param_tys,
                &mut *self.captures,
                false,
                self.dialect,
                self.cte_map,
            )?;
            let mut h_parts = Vec::new();
            {
                let ctx = ParamCtx {
                    captures: &*self.captures,
                    param_ids: self.fn_params,
                    param_tys: self.param_tys,
                };
                h_expr.render_parts(&mut h_parts, &ctx, self.dialect);
            }
            sql_parts.push(self.dialect.kw("HAVING_KW"));
            sql_parts.extend(h_parts);
        }
        sql_parts.extend(self.render_order_clauses(&qualifiers.order_by, &scope)?);
        sql_parts.extend(self.render_limit_offset(qualifiers, None)?);
        sql_parts.extend(qualifiers.lock.render(self.dialect));

        let n_cols = if group_by.is_some() { 2u16 } else { 1u16 };
        let dispatch = match return_shape {
            ReturnShape::Many(_) => DispatchKind::All(row_ty.clone()),
            ReturnShape::Optional(_) => DispatchKind::First(row_ty.clone()),
            ReturnShape::Plain(_) => DispatchKind::One(row_ty.clone()),
            ReturnShape::Stream(_) => {
                return Err(syn::Error::new(
                    row_ty.span(),
                    "aggregate (.count/.sum/.avg/.min/.max) does not support `.stream()` — aggregate returns a single row",
                ));
            }
        };

        Ok(QueryPlan {
            row_ty: row_ty.clone(),
            sql_parts,
            n_result_cols: quote! { #n_cols },
            decode: DecodeKind::Row(Box::new(row_ty.clone())),
            dispatch,
            probe_override: None,
        })
    }

    fn build_select(
        &mut self,
        source: &QuerySource,
        pred_idents: &[Ident],
        predicate: &Expr,
        projection: Option<&ProjectionSpec>,
        terminator: Terminator,
        qualifiers: &ChainQualifiers,
    ) -> syn::Result<QueryPlan> {
        let outer = match self.return_type {
            ReturnType::Type(_, t) => (**t).clone(),
            ReturnType::Default => {
                return Err(syn::Error::new(
                    self.return_type.span(),
                    "SELECT queries require an explicit return type",
                ));
            }
        };
        let return_shape = ReturnShape::parse(&outer);
        let row_ty = return_shape.row_ty().clone();

        match (&return_shape, terminator) {
            (ReturnShape::Plain(_), Terminator::One)
            | (ReturnShape::Optional(_), Terminator::First)
            | (ReturnShape::Many(_), Terminator::All)
            | (ReturnShape::Stream(_), Terminator::Stream) => {}
            (rs, t) => {
                return Err(syn::Error::new(
                    self.return_type.span(),
                    format!(
                        "return type {} does not match terminator `.{}()`; expected {}",
                        rs.describe_actual(),
                        t.name(),
                        t.expected_return(),
                    ),
                ));
            }
        }

        let scope = RowScope::for_source(source, pred_idents);

        let mut on_parts_per_join: Vec<Vec<proc_macro2::TokenStream>> =
            Vec::with_capacity(source.joins.len());
        for (i, j) in source.joins.iter().enumerate() {
            if j.kind.is_lateral() {
                let parts = self.render_lateral_subquery(j, i, source)?;
                on_parts_per_join.push(parts);
            } else {
                let on_scope = RowScope::for_join_on(source, i);
                let mut throwaway: Vec<Ident> = Vec::new();
                let on_expr = WhereExpr::build(
                    &j.cond,
                    &on_scope,
                    self.fn_params,
                    self.param_tys,
                    &mut throwaway,
                    true,
                    self.dialect,
                    self.cte_map,
                )?;
                if !throwaway.is_empty() {
                    return Err(syn::Error::new(
                        j.cond.span(),
                        "JOIN ON clause must compare columns only; bound parameters belong in `.filter(...)`",
                    ));
                }
                let mut parts = Vec::new();
                let ctx = ParamCtx {
                    captures: &*self.captures,
                    param_ids: self.fn_params,
                    param_tys: self.param_tys,
                };
                on_expr.render_parts(&mut parts, &ctx, self.dialect);
                on_parts_per_join.push(parts);
            }
        }

        let skip_where = predicate.is_synthetic_true();
        let where_parts = if skip_where {
            Vec::new()
        } else {
            let where_expr = WhereExpr::build(
                predicate,
                &scope,
                self.fn_params,
                self.param_tys,
                &mut *self.captures,
                false,
                self.dialect,
                self.cte_map,
            )?;
            let mut wp = Vec::new();
            let ctx = ParamCtx {
                captures: &*self.captures,
                param_ids: self.fn_params,
                param_tys: self.param_tys,
            };
            where_expr.render_parts(&mut wp, &ctx, self.dialect);
            wp
        };

        let order_parts = self.render_order_clauses(&qualifiers.order_by, &scope)?;
        let limit_offset_parts = self.render_limit_offset(qualifiers, Some(terminator))?;

        let mut sql_parts = vec![self.dialect.kw("SELECT_KW")];
        sql_parts.extend(self.render_distinct_prefix(qualifiers, &scope)?);
        if let Some(proj) = projection {
            let proj_scope = RowScope::for_source(source, &proj.idents);
            for (i, elem) in proj.elems.iter().enumerate() {
                if i > 0 {
                    sql_parts.push(self.dialect.kw("COMMA"));
                }
                sql_parts.extend(self.render_select_elem(elem, &proj_scope)?);
            }
        } else if source.joins.is_empty() {
            let table_path = &source.primary_path;
            sql_parts.push(quote! { <#table_path>::__CARTEL_SELECT_COLS });
        } else {
            sql_parts.extend(row_ty.qualified_select_cols(self.dialect));
        }
        sql_parts.extend(Self::render_from_clause_with_on(
            source,
            &on_parts_per_join,
            self.dialect,
        ));
        if !skip_where {
            sql_parts.push(self.dialect.kw("WHERE_KW"));
            sql_parts.extend(where_parts);
        }
        sql_parts.extend(order_parts);
        sql_parts.extend(limit_offset_parts);
        sql_parts.extend(qualifiers.lock.render(self.dialect));

        let dispatch = match terminator {
            Terminator::One => DispatchKind::One(row_ty.clone()),
            Terminator::First => DispatchKind::First(row_ty.clone()),
            Terminator::All => DispatchKind::All(row_ty.clone()),
            Terminator::Stream => DispatchKind::Stream(row_ty.clone()),
        };

        let n_result_cols = match projection {
            Some(proj) => {
                let n = proj.elems.len() as u16;
                quote! { #n }
            }
            None => row_ty.n_cols_const(),
        };

        Ok(QueryPlan {
            row_ty: row_ty.clone(),
            sql_parts,
            n_result_cols,
            decode: DecodeKind::Row(Box::new(row_ty.clone())),
            dispatch,
            probe_override: None,
        })
    }

    pub(super) fn render_from_clause_with_on(
        source: &QuerySource,
        on_parts_per_join: &[Vec<proc_macro2::TokenStream>],
        dialect: &Dialect,
    ) -> Vec<proc_macro2::TokenStream> {
        let mut parts = vec![dialect.kw("FROM_KW")];
        parts.extend(source.render_primary_table());
        for (j, on_parts) in source.joins.iter().zip(on_parts_per_join.iter()) {
            parts.push(j.kind.sql_ref(dialect));
            if j.kind.is_lateral() {
                parts.extend(on_parts.iter().cloned());
            } else {
                let p = &j.path;
                parts.push(quote! { <#p>::__CARTEL_TABLE });
                parts.push(dialect.kw("ON_KW"));
                parts.extend(on_parts.iter().cloned());
            }
        }
        parts
    }

    fn render_select_elem(
        &mut self,
        elem: &Expr,
        scope: &RowScope,
    ) -> syn::Result<Vec<proc_macro2::TokenStream>> {
        if let Expr::MethodCall(mc) = elem
            && mc.method == "over"
        {
            return self.render_window_expr(mc, scope);
        }
        scope
            .parse_side_expr(
                elem,
                Some((self.fn_params, &mut *self.captures)),
                self.param_tys,
                self.dialect,
                self.cte_map,
            )
            .ok_or_else(|| {
                syn::Error::new(
                    elem.span(),
                    "`.select(...)` element must be a column / arithmetic / function expression or a window expression `<fn>().over(|w| ...)`",
                )
            })
    }

    fn render_window_expr(
        &mut self,
        over_call: &syn::ExprMethodCall,
        scope: &RowScope,
    ) -> syn::Result<Vec<proc_macro2::TokenStream>> {
        if over_call.args.len() != 1 {
            return Err(syn::Error::new(
                over_call.args.span(),
                "`.over(...)` takes exactly one closure argument",
            ));
        }
        let fn_call = match over_call.receiver.as_ref() {
            Expr::Call(c) => c,
            other => {
                return Err(syn::Error::new(
                    other.span(),
                    "window receiver must be a function call like `row_number()` / `rank()` / `count()` / `sum(col)` / etc.",
                ));
            }
        };
        let fn_path = match fn_call.func.as_ref() {
            Expr::Path(ExprPath { path, .. }) => path,
            other => {
                return Err(syn::Error::new(
                    other.span(),
                    "window function must be a path ending in `row_number` / `rank` / `dense_rank` / `count` / `sum` / `avg` / `min` / `max` / `lag` / `lead`",
                ));
            }
        };
        let last_seg = fn_path
            .segments
            .last()
            .ok_or_else(|| syn::Error::new(fn_path.span(), "window function path is empty"))?;
        let fname = last_seg.ident.to_string();
        let mut parts: Vec<proc_macro2::TokenStream> = Vec::new();
        match fname.as_str() {
            "row_number" | "rank" | "dense_rank" => {
                if !fn_call.args.is_empty() {
                    return Err(syn::Error::new(
                        fn_call.args.span(),
                        format!("`{fname}()` takes no arguments"),
                    ));
                }
                let head = match fname.as_str() {
                    "row_number" => self.dialect.kw("ROW_NUMBER_OVER"),
                    "rank" => self.dialect.kw("RANK_OVER"),
                    "dense_rank" => self.dialect.kw("DENSE_RANK_OVER"),
                    _ => unreachable!(),
                };
                parts.push(head);
            }
            "count" => {
                if fn_call.args.is_empty() {
                    parts.push(self.dialect.kw("COUNT_STAR_OVER"));
                } else if fn_call.args.len() == 1 {
                    let inner = scope
                        .parse_side_expr(
                            &fn_call.args[0],
                            Some((self.fn_params, &mut *self.captures)),
                            self.param_tys,
                            self.dialect,
                            self.cte_map,
                        )
                        .ok_or_else(|| {
                            syn::Error::new(
                                fn_call.args[0].span(),
                                "`count(...)` window arg must be a column / expression",
                            )
                        })?;
                    parts.push(self.dialect.kw("COUNT_OPEN"));
                    parts.extend(inner);
                    parts.push(self.dialect.kw("OVER_OPEN"));
                } else {
                    return Err(syn::Error::new(
                        fn_call.args.span(),
                        "`count(...)` window takes 0 or 1 args",
                    ));
                }
            }
            "sum" | "avg" | "min" | "max" => {
                if fn_call.args.len() != 1 {
                    return Err(syn::Error::new(
                        fn_call.args.span(),
                        format!("`{fname}(...)` window takes one column / expression argument"),
                    ));
                }
                let inner = scope
                    .parse_side_expr(
                        &fn_call.args[0],
                        Some((self.fn_params, &mut *self.captures)),
                        self.param_tys,
                        self.dialect,
                        self.cte_map,
                    )
                    .ok_or_else(|| {
                        syn::Error::new(
                            fn_call.args[0].span(),
                            format!("`{fname}(...)` window arg must be a column / expression"),
                        )
                    })?;
                let head = match fname.as_str() {
                    "sum" => self.dialect.kw("SUM_OPEN"),
                    "avg" => self.dialect.kw("AVG_OPEN"),
                    "min" => self.dialect.kw("MIN_OPEN"),
                    "max" => self.dialect.kw("MAX_OPEN"),
                    _ => unreachable!(),
                };
                parts.push(head);
                parts.extend(inner);
                parts.push(self.dialect.kw("OVER_OPEN"));
            }
            "lag" | "lead" => {
                if fn_call.args.len() != 2 {
                    return Err(syn::Error::new(
                        fn_call.args.span(),
                        format!("`{fname}(col, offset)` window takes two arguments"),
                    ));
                }
                let inner = scope
                    .parse_side_expr(
                        &fn_call.args[0],
                        Some((self.fn_params, &mut *self.captures)),
                        self.param_tys,
                        self.dialect,
                        self.cte_map,
                    )
                    .ok_or_else(|| {
                        syn::Error::new(
                            fn_call.args[0].span(),
                            format!("`{fname}(...)` first arg must be a column / expression"),
                        )
                    })?;
                let off = self.render_int_arg(&fn_call.args[1])?;
                let head = if fname == "lag" { "lag(" } else { "lead(" };
                parts.push(quote! { #head });
                parts.extend(inner);
                parts.push(self.dialect.kw("COMMA"));
                parts.push(off);
                parts.push(self.dialect.kw("OVER_OPEN"));
            }
            other => {
                return Err(syn::Error::new(
                    fn_path.span(),
                    format!(
                        "unsupported window function `{other}`; allowed: row_number, rank, dense_rank, count, sum, avg, min, max, lag, lead"
                    ),
                ));
            }
        }
        let Expr::Closure(c) = &over_call.args[0] else {
            return Err(syn::Error::new(
                over_call.args[0].span(),
                "`.over(...)` argument must be a closure `|w| w.partition_by(...).order_by(...)`",
            ));
        };
        if c.inputs.len() != 1 {
            return Err(syn::Error::new(
                c.inputs.span(),
                "`.over(...)` closure must take one parameter",
            ));
        }
        let w_ident = c.inputs[0].closure_ident()?;
        parts.extend(self.render_window_chain(&c.body, &w_ident, scope)?);
        parts.push(self.dialect.kw("PAREN_CLOSE"));
        Ok(parts)
    }

    fn render_window_chain(
        &mut self,
        expr: &Expr,
        w_ident: &Ident,
        scope: &RowScope,
    ) -> syn::Result<Vec<proc_macro2::TokenStream>> {
        let mut chain: Vec<&syn::ExprMethodCall> = Vec::new();
        let mut current: &Expr = expr;
        loop {
            match current {
                Expr::MethodCall(mc) => {
                    chain.push(mc);
                    current = mc.receiver.as_ref();
                }
                Expr::Path(ExprPath { path, .. })
                    if path.segments.len() == 1 && path.segments[0].ident == *w_ident =>
                {
                    break;
                }
                other => {
                    return Err(syn::Error::new(
                        other.span(),
                        "window-spec chain must start at the `w` ident and use `.partition_by` / `.order_by` / `.order_by_desc`",
                    ));
                }
            }
        }
        chain.reverse();
        let mut partition_parts: Vec<Vec<proc_macro2::TokenStream>> = Vec::new();
        let mut order_parts: Vec<(Vec<proc_macro2::TokenStream>, OrderDir)> = Vec::new();
        for mc in chain {
            if mc.args.len() != 1 {
                return Err(syn::Error::new(
                    mc.args.span(),
                    format!("`.{}(...)` takes one argument", mc.method),
                ));
            }
            let elem = scope
                .parse_side_expr(
                    &mc.args[0],
                    Some((self.fn_params, &mut *self.captures)),
                    self.param_tys,
                    self.dialect,
                    self.cte_map,
                )
                .ok_or_else(|| {
                    syn::Error::new(
                        mc.args[0].span(),
                        format!(
                            "`.{}(...)` arg must be a column / arithmetic / function expression",
                            mc.method,
                        ),
                    )
                })?;
            match mc.method.to_string().as_str() {
                "partition_by" => partition_parts.push(elem),
                "order_by" => order_parts.push((elem, OrderDir::Asc)),
                "order_by_desc" => order_parts.push((elem, OrderDir::Desc)),
                other => {
                    return Err(syn::Error::new(
                        mc.method.span(),
                        format!(
                            "unsupported window-spec method `.{other}`; allowed: partition_by / order_by / order_by_desc"
                        ),
                    ));
                }
            }
        }
        let mut out: Vec<proc_macro2::TokenStream> = Vec::new();
        if !partition_parts.is_empty() {
            out.push(self.dialect.kw("PARTITION_BY"));
            for (i, p) in partition_parts.iter().enumerate() {
                if i > 0 {
                    out.push(self.dialect.kw("COMMA"));
                }
                out.extend(p.iter().cloned());
            }
        }
        if !order_parts.is_empty() {
            if !partition_parts.is_empty() {
                out.push(self.dialect.kw("SPACE"));
            }
            out.push(self.dialect.kw("WIN_ORDER_BY"));
            for (i, (parts, dir)) in order_parts.iter().enumerate() {
                if i > 0 {
                    out.push(self.dialect.kw("COMMA"));
                }
                out.extend(parts.iter().cloned());
                if matches!(dir, OrderDir::Desc) {
                    out.push(self.dialect.kw("DESC"));
                }
            }
        }
        Ok(out)
    }

    fn render_lateral_subquery(
        &mut self,
        j: &JoinSpec,
        join_index: usize,
        source: &QuerySource,
    ) -> syn::Result<Vec<proc_macro2::TokenStream>> {
        let outer_scope = RowScope::for_lateral_outer(source, &j.on_idents, join_index);
        let mut quals = ChainQualifiers::default();
        let body_after_quals = quals.peel(&j.cond)?;
        let (inner_source, inner_pred_idents, inner_predicate) =
            QuerySource::parse_chain(&body_after_quals, self.cte_map)?;
        if !inner_source.joins.is_empty() {
            return Err(syn::Error::new(
                j.cond.span(),
                "joined source not yet supported as a lateral subquery body",
            ));
        }
        let inner_table = inner_source.primary_path.clone();
        let inner_scope = RowScope::for_source(&inner_source, &inner_pred_idents)
            .with_outer(outer_scope.force_qualified())
            .force_qualified();
        let inner_where = WhereExpr::build(
            &inner_predicate,
            &inner_scope,
            self.fn_params,
            self.param_tys,
            &mut *self.captures,
            true,
            self.dialect,
            self.cte_map,
        )?;
        let mut where_parts = Vec::new();
        {
            let ctx = ParamCtx {
                captures: &*self.captures,
                param_ids: self.fn_params,
                param_tys: self.param_tys,
            };
            inner_where.render_parts(&mut where_parts, &ctx, self.dialect);
        }

        let mut sql = vec![
            self.dialect.kw("SUBQUERY_OPEN"),
            quote! { <#inner_table>::__CARTEL_SELECT_COLS_QUALIFIED },
            self.dialect.kw("FROM_KW"),
            quote! { <#inner_table>::__CARTEL_TABLE },
            self.dialect.kw("WHERE_KW"),
        ];
        sql.extend(where_parts);
        sql.extend(self.render_order_clauses(&quals.order_by, &inner_scope)?);
        sql.extend(self.render_limit_offset(&quals, None)?);
        sql.push(self.dialect.kw("PAREN_CLOSE_SPACE"));
        sql.push(quote! { <#inner_table>::__CARTEL_TABLE });
        sql.push(self.dialect.kw("ON_TRUE"));
        Ok(sql)
    }

    fn render_distinct_prefix(
        &mut self,
        qualifiers: &ChainQualifiers,
        scope: &RowScope,
    ) -> syn::Result<Vec<proc_macro2::TokenStream>> {
        match &qualifiers.distinct {
            DistinctKind::None => Ok(Vec::new()),
            DistinctKind::All => Ok(vec![self.dialect.kw("DISTINCT_KW")]),
            DistinctKind::On(arg, expr) => {
                let mut local_vars = Vec::with_capacity(scope.vars.len().max(1));
                if let Some(first) = scope.vars.first() {
                    local_vars.push(RowVar {
                        ident: arg.clone(),
                        table_const: first.table_const.clone(),
                    });
                } else {
                    local_vars.push(RowVar {
                        ident: arg.clone(),
                        table_const: None,
                    });
                }
                let local_scope = RowScope {
                    vars: local_vars,
                    agg_var: None,
                    outer: None,
                    primary_table_const: scope.primary_table_const.clone(),
                    unnest_cols: Vec::new(),
                };
                let inner = local_scope
                    .parse_side_expr(
                        expr,
                        Some((self.fn_params, &mut *self.captures)),
                        self.param_tys,
                        self.dialect,
                        self.cte_map,
                    )
                    .ok_or_else(|| {
                        syn::Error::new(
                            expr.span(),
                            "`.distinct_on(...)` body must be a column / arithmetic / function expression",
                        )
                    })?;
                let mut parts = vec![self.dialect.kw("DISTINCT_ON_OPEN")];
                parts.extend(inner);
                parts.push(self.dialect.kw("PAREN_CLOSE_SPACE"));
                Ok(parts)
            }
        }
    }

    fn build_update(
        &mut self,
        source: &QuerySource,
        pred_idents: &[Ident],
        predicate: &Expr,
        set_idents: &[Ident],
        set_body: &Expr,
        returning: ReturningKind,
    ) -> syn::Result<QueryPlan> {
        let primary_path = &source.primary_path;
        self.expect_mutation_return("UPDATE", returning, primary_path)?;

        let assigns = ColumnAssign::collect(
            set_body,
            &set_idents[0],
            self.fn_params,
            self.param_tys,
            &mut *self.captures,
            self.dialect,
            self.cte_map,
        )?;
        if assigns.is_empty() {
            return Err(syn::Error::new(
                set_body.span(),
                "`.update(...)` body must assign at least one column",
            ));
        }

        let scope = RowScope::for_source(source, pred_idents);

        let mut on_parts_acc: Vec<Vec<proc_macro2::TokenStream>> =
            Vec::with_capacity(source.joins.len());
        for (i, j) in source.joins.iter().enumerate() {
            let on_scope = RowScope::for_join_on(source, i);
            let mut throwaway: Vec<Ident> = Vec::new();
            let on_expr = WhereExpr::build(
                &j.cond,
                &on_scope,
                self.fn_params,
                self.param_tys,
                &mut throwaway,
                true,
                self.dialect,
                self.cte_map,
            )?;
            if !throwaway.is_empty() {
                return Err(syn::Error::new(
                    j.cond.span(),
                    "JOIN ON clause must compare columns only; bound parameters belong in `.filter(...)`",
                ));
            }
            let mut p = Vec::new();
            let ctx = ParamCtx {
                captures: &*self.captures,
                param_ids: self.fn_params,
                param_tys: self.param_tys,
            };
            on_expr.render_parts(&mut p, &ctx, self.dialect);
            on_parts_acc.push(p);
        }

        let where_expr = WhereExpr::build(
            predicate,
            &scope,
            self.fn_params,
            self.param_tys,
            &mut *self.captures,
            false,
            self.dialect,
            self.cte_map,
        )?;
        let mut where_parts = Vec::new();
        {
            let ctx = ParamCtx {
                captures: &*self.captures,
                param_ids: self.fn_params,
                param_tys: self.param_tys,
            };
            where_expr.render_parts(&mut where_parts, &ctx, self.dialect);
        }

        let mut sql_parts = vec![
            self.dialect.kw("UPDATE_KW"),
            quote! { <#primary_path>::__CARTEL_TABLE },
            self.dialect.kw("SET_KW"),
        ];
        for (i, a) in assigns.iter().enumerate() {
            if i > 0 {
                sql_parts.push(self.dialect.kw("COMMA"));
            }
            let col_eq = format!("{} = ", a.column);
            sql_parts.push(quote! { #col_eq });
            sql_parts.extend(a.value_parts.iter().cloned());
        }
        if !source.joins.is_empty() {
            sql_parts.push(self.dialect.kw("FROM_KW"));
            for (i, j) in source.joins.iter().enumerate() {
                if i > 0 {
                    sql_parts.push(self.dialect.kw("COMMA"));
                }
                let p = &j.path;
                sql_parts.push(quote! { <#p>::__CARTEL_TABLE });
            }
        }
        sql_parts.push(self.dialect.kw("WHERE_KW"));
        for on_parts in &on_parts_acc {
            sql_parts.extend(on_parts.iter().cloned());
            sql_parts.push(self.dialect.kw("AND_KW"));
        }
        sql_parts.extend(where_parts);

        self.mutation_plan(sql_parts, primary_path, returning)
    }

    fn build_delete(
        &mut self,
        source: &QuerySource,
        pred_idents: &[Ident],
        predicate: &Expr,
        returning: ReturningKind,
    ) -> syn::Result<QueryPlan> {
        let primary_path = &source.primary_path;
        self.expect_mutation_return("DELETE", returning, primary_path)?;

        let scope = RowScope::for_source(source, pred_idents);

        let mut on_parts_acc: Vec<Vec<proc_macro2::TokenStream>> =
            Vec::with_capacity(source.joins.len());
        for (i, j) in source.joins.iter().enumerate() {
            let on_scope = RowScope::for_join_on(source, i);
            let mut throwaway: Vec<Ident> = Vec::new();
            let on_expr = WhereExpr::build(
                &j.cond,
                &on_scope,
                self.fn_params,
                self.param_tys,
                &mut throwaway,
                true,
                self.dialect,
                self.cte_map,
            )?;
            if !throwaway.is_empty() {
                return Err(syn::Error::new(
                    j.cond.span(),
                    "JOIN ON clause must compare columns only; bound parameters belong in `.filter(...)`",
                ));
            }
            let mut p = Vec::new();
            let ctx = ParamCtx {
                captures: &*self.captures,
                param_ids: self.fn_params,
                param_tys: self.param_tys,
            };
            on_expr.render_parts(&mut p, &ctx, self.dialect);
            on_parts_acc.push(p);
        }

        let where_expr = WhereExpr::build(
            predicate,
            &scope,
            self.fn_params,
            self.param_tys,
            &mut *self.captures,
            false,
            self.dialect,
            self.cte_map,
        )?;
        let mut where_parts = Vec::new();
        {
            let ctx = ParamCtx {
                captures: &*self.captures,
                param_ids: self.fn_params,
                param_tys: self.param_tys,
            };
            where_expr.render_parts(&mut where_parts, &ctx, self.dialect);
        }

        let mut sql_parts = vec![
            self.dialect.kw("DELETE_FROM_KW"),
            quote! { <#primary_path>::__CARTEL_TABLE },
        ];
        if !source.joins.is_empty() {
            sql_parts.push(self.dialect.kw("USING_KW"));
            for (i, j) in source.joins.iter().enumerate() {
                if i > 0 {
                    sql_parts.push(self.dialect.kw("COMMA"));
                }
                let p = &j.path;
                sql_parts.push(quote! { <#p>::__CARTEL_TABLE });
            }
        }
        sql_parts.push(self.dialect.kw("WHERE_KW"));
        for on_parts in &on_parts_acc {
            sql_parts.extend(on_parts.iter().cloned());
            sql_parts.push(self.dialect.kw("AND_KW"));
        }
        sql_parts.extend(where_parts);

        self.mutation_plan(sql_parts, primary_path, returning)
    }

    fn build_update_each(
        &mut self,
        source: &QuerySource,
        data_sources: &[Expr],
        row_var: &Ident,
        col_vars: &[Ident],
        predicate: &Expr,
        set_body: &Expr,
        returning: ReturningKind,
    ) -> syn::Result<QueryPlan> {
        let primary_path = &source.primary_path;
        self.expect_mutation_return("UPDATE", returning, primary_path)?;

        let mut placeholders: Vec<proc_macro2::TokenStream> =
            Vec::with_capacity(data_sources.len());
        for ds in data_sources {
            let captured = self.fn_params.resolve_borrowed(ds)?;
            let idx = self.captures.intern(captured);
            let ctx = ParamCtx {
                captures: &*self.captures,
                param_ids: self.fn_params,
                param_tys: self.param_tys,
            };
            placeholders.push(self.dialect.placeholder(idx, &ctx));
        }

        let scope =
            RowScope::for_source(source, slice::from_ref(row_var)).with_unnest(col_vars.to_vec());

        let owned_single: Stmt;
        let stmts: Vec<&Stmt> = match set_body {
            Expr::Block(eb) => eb.block.stmts.iter().collect(),
            single_stmt @ Expr::Assign(_) => {
                owned_single = Stmt::Expr((*single_stmt).clone(), None);
                vec![&owned_single]
            }
            other => {
                return Err(syn::Error::new(
                    other.span(),
                    "`filter_each(...).update(|row, c0...| { ... })` body must be `row.col = <expr>` assignments",
                ));
            }
        };
        let mut assign_parts: Vec<proc_macro2::TokenStream> = Vec::new();
        for (i, stmt) in stmts.iter().enumerate() {
            let Stmt::Expr(assign_expr, _) = stmt else {
                return Err(syn::Error::new(
                    stmt.span(),
                    "update body may only contain `row.col = <expr>;` statements",
                ));
            };
            let Expr::Assign(syn::ExprAssign { left, right, .. }) = assign_expr else {
                return Err(syn::Error::new(
                    assign_expr.span(),
                    "expected `row.col = <expr>` assignment",
                ));
            };
            let col = left.as_column_ref(row_var).ok_or_else(|| {
                syn::Error::new(
                    left.span(),
                    "left side of assignment must be `<row_var>.<column>`",
                )
            })?;
            let rhs = scope
                .parse_side_expr(
                    right,
                    Some((self.fn_params, &mut *self.captures)),
                    self.param_tys,
                    self.dialect,
                    self.cte_map,
                )
                .ok_or_else(|| {
                    syn::Error::new(
                        right.span(),
                        "right side of assignment must be a column / fn-parameter / arithmetic / function expression",
                    )
                })?;
            if i > 0 {
                assign_parts.push(self.dialect.kw("COMMA"));
            }
            let col_eq = format!("{col} = ");
            assign_parts.push(quote! { #col_eq });
            assign_parts.extend(rhs);
        }
        if assign_parts.is_empty() {
            return Err(syn::Error::new(
                set_body.span(),
                "`.update(...)` body must assign at least one column",
            ));
        }

        let where_expr = WhereExpr::build(
            predicate,
            &scope,
            self.fn_params,
            self.param_tys,
            &mut *self.captures,
            true,
            self.dialect,
            self.cte_map,
        )?;
        let mut where_parts = Vec::new();
        {
            let ctx = ParamCtx {
                captures: &*self.captures,
                param_ids: self.fn_params,
                param_tys: self.param_tys,
            };
            where_expr.render_parts(&mut where_parts, &ctx, self.dialect);
        }

        let mut alias_cols = String::new();
        for i in 0..data_sources.len() {
            if i > 0 {
                alias_cols.push(',');
            }
            alias_cols.push('f');
            alias_cols.push_str(&i.to_string());
        }

        let mut sql_parts = vec![
            self.dialect.kw("UPDATE_KW"),
            quote! { <#primary_path>::__CARTEL_TABLE },
            self.dialect.kw("SET_KW"),
        ];
        sql_parts.extend(assign_parts);
        sql_parts.push(self.dialect.kw("UNNEST_FROM_OPEN"));
        for (i, ph) in placeholders.iter().enumerate() {
            if i > 0 {
                sql_parts.push(self.dialect.kw("COMMA_TIGHT"));
            }
            sql_parts.push(ph.clone());
        }
        sql_parts.push(self.dialect.kw("UNNEST_ALIAS_OPEN"));
        sql_parts.push(quote! { #alias_cols });
        sql_parts.push(self.dialect.kw("PAREN_CLOSE"));
        sql_parts.push(self.dialect.kw("WHERE_KW"));
        sql_parts.extend(where_parts);

        let probe = {
            let cols = data_sources;
            quote! {
                let __cartel_pred = |#row_var: #primary_path, #( #col_vars ),*| -> bool { #predicate };
                let __cartel_set = |mut #row_var: #primary_path, #( #col_vars ),*| { #set_body };
                let _ = <#primary_path>::filter_each(
                    ( #( #cols, )* ),
                    __cartel_pred,
                ).update(__cartel_set);
            }
        };

        let mut plan = self.mutation_plan(sql_parts, primary_path, returning)?;
        plan.probe_override = Some(probe);
        Ok(plan)
    }

    fn build_insert(
        &mut self,
        table_path: &Path,
        closure_arg: &Ident,
        body: &Expr,
        returning: ReturningKind,
        on_conflict: &OnConflictKind,
    ) -> syn::Result<QueryPlan> {
        self.expect_mutation_return("INSERT", returning, table_path)?;

        let assigns = ColumnAssign::collect(
            body,
            closure_arg,
            self.fn_params,
            self.param_tys,
            &mut *self.captures,
            self.dialect,
            self.cte_map,
        )?;
        if assigns.is_empty() {
            return Err(syn::Error::new(
                body.span(),
                "`Table::insert(...)` body must assign at least one column",
            ));
        }

        let mut cols_str = String::new();
        for (i, a) in assigns.iter().enumerate() {
            if i > 0 {
                cols_str.push(',');
            }
            cols_str.push_str(&a.column);
        }

        let mut sql_parts = vec![
            self.dialect.kw("INSERT_INTO_KW"),
            quote! { <#table_path>::__CARTEL_TABLE },
            self.dialect.kw("PAREN_OPEN_LEADING_SPACE"),
            quote! { #cols_str },
            self.dialect.kw("VALUES_OPEN"),
        ];
        for (i, a) in assigns.iter().enumerate() {
            if i > 0 {
                sql_parts.push(self.dialect.kw("COMMA_TIGHT"));
            }
            sql_parts.extend(a.value_parts.iter().cloned());
        }
        sql_parts.push(self.dialect.kw("PAREN_CLOSE"));
        sql_parts.extend(self.render_on_conflict(on_conflict)?);

        self.mutation_plan(sql_parts, table_path, returning)
    }

    fn build_insert_each(
        &mut self,
        table_path: &Path,
        closure_arg: &Ident,
        body: &Expr,
        returning: ReturningKind,
        on_conflict: &OnConflictKind,
    ) -> syn::Result<QueryPlan> {
        self.expect_mutation_return("INSERT", returning, table_path)?;

        let assigns = ColumnAssign::collect(
            body,
            closure_arg,
            self.fn_params,
            self.param_tys,
            &mut *self.captures,
            self.dialect,
            self.cte_map,
        )?;
        if assigns.is_empty() {
            return Err(syn::Error::new(
                body.span(),
                "`Table::insert_each(...)` body must assign at least one column",
            ));
        }

        let mut cols_str = String::new();
        for (i, a) in assigns.iter().enumerate() {
            if i > 0 {
                cols_str.push(',');
            }
            cols_str.push_str(&a.column);
        }

        let mut sql_parts = vec![
            self.dialect.kw("INSERT_INTO_KW"),
            quote! { <#table_path>::__CARTEL_TABLE },
            self.dialect.kw("PAREN_OPEN_LEADING_SPACE"),
            quote! { #cols_str },
            self.dialect.kw("UNNEST_OPEN"),
        ];
        for (i, a) in assigns.iter().enumerate() {
            if i > 0 {
                sql_parts.push(self.dialect.kw("COMMA_TIGHT"));
            }
            sql_parts.extend(a.value_parts.iter().cloned());
        }
        sql_parts.push(self.dialect.kw("PAREN_CLOSE"));
        sql_parts.extend(self.render_on_conflict(on_conflict)?);

        self.mutation_plan(sql_parts, table_path, returning)
    }

    fn build_insert_from(
        &mut self,
        table_path: &Path,
        source: &QuerySource,
        source_pred_idents: &[Ident],
        source_predicate: &Expr,
        target_arg: &Ident,
        source_idents: &[Ident],
        body: &Expr,
        returning: ReturningKind,
        on_conflict: &OnConflictKind,
    ) -> syn::Result<QueryPlan> {
        self.expect_mutation_return("INSERT", returning, table_path)?;

        let owned_single: Stmt;
        let stmts: Vec<&Stmt> = match body {
            Expr::Block(eb) => eb.block.stmts.iter().collect(),
            single_stmt @ Expr::Assign(_) => {
                owned_single = Stmt::Expr((*single_stmt).clone(), None);
                vec![&owned_single]
            }
            other => {
                return Err(syn::Error::new(
                    other.span(),
                    "`Table::insert_from(.., |t, src| { ... })` body must be a block of `t.col = <source expr>` assignments",
                ));
            }
        };

        let source_scope = RowScope::for_source(source, source_idents);
        let mut cols_str = String::new();
        let mut select_parts: Vec<proc_macro2::TokenStream> = Vec::new();
        for (i, stmt) in stmts.iter().enumerate() {
            let assign_expr = match stmt {
                Stmt::Expr(e, _) => e,
                other => {
                    return Err(syn::Error::new(
                        other.span(),
                        "insert_from body may only contain `t.col = <expr>;` statements",
                    ));
                }
            };
            let Expr::Assign(syn::ExprAssign { left, right, .. }) = assign_expr else {
                return Err(syn::Error::new(
                    assign_expr.span(),
                    "expected `t.col = <expr>` assignment",
                ));
            };
            let col = left.as_column_ref(target_arg).ok_or_else(|| {
                syn::Error::new(
                    left.span(),
                    "left side of assignment must be `<target>.<col>`",
                )
            })?;
            let rhs = source_scope
                .parse_side_expr(
                    right,
                    Some((self.fn_params, &mut *self.captures)),
                    self.param_tys,
                    self.dialect,
                    self.cte_map,
                )
                .ok_or_else(|| {
                    syn::Error::new(
                        right.span(),
                        "right side must be a column / arithmetic / function expression in the source row scope",
                    )
                })?;
            if i > 0 {
                cols_str.push(',');
                select_parts.push(self.dialect.kw("COMMA"));
            }
            cols_str.push_str(&col);
            select_parts.extend(rhs);
        }

        let where_scope = RowScope::for_source(source, source_pred_idents);
        let mut on_parts_acc: Vec<Vec<proc_macro2::TokenStream>> =
            Vec::with_capacity(source.joins.len());
        for (i, j) in source.joins.iter().enumerate() {
            let on_scope = RowScope::for_join_on(source, i);
            let mut throwaway: Vec<Ident> = Vec::new();
            let on_expr = WhereExpr::build(
                &j.cond,
                &on_scope,
                self.fn_params,
                self.param_tys,
                &mut throwaway,
                true,
                self.dialect,
                self.cte_map,
            )?;
            if !throwaway.is_empty() {
                return Err(syn::Error::new(
                    j.cond.span(),
                    "JOIN ON clause must compare columns only; bound parameters belong in `.filter(...)`",
                ));
            }
            let mut p = Vec::new();
            let ctx = ParamCtx {
                captures: &*self.captures,
                param_ids: self.fn_params,
                param_tys: self.param_tys,
            };
            on_expr.render_parts(&mut p, &ctx, self.dialect);
            on_parts_acc.push(p);
        }
        let where_expr = WhereExpr::build(
            source_predicate,
            &where_scope,
            self.fn_params,
            self.param_tys,
            &mut *self.captures,
            false,
            self.dialect,
            self.cte_map,
        )?;
        let mut where_parts = Vec::new();
        {
            let ctx = ParamCtx {
                captures: &*self.captures,
                param_ids: self.fn_params,
                param_tys: self.param_tys,
            };
            where_expr.render_parts(&mut where_parts, &ctx, self.dialect);
        }

        let primary = &source.primary_path;
        let mut sql_parts = vec![
            self.dialect.kw("INSERT_INTO_KW"),
            quote! { <#table_path>::__CARTEL_TABLE },
            self.dialect.kw("PAREN_OPEN_LEADING_SPACE"),
            quote! { #cols_str },
            self.dialect.kw("INSERT_FROM_SELECT"),
        ];
        sql_parts.extend(select_parts);
        sql_parts.push(self.dialect.kw("FROM_KW"));
        sql_parts.push(quote! { <#primary>::__CARTEL_TABLE });
        for (i, j) in source.joins.iter().enumerate() {
            let p = &j.path;
            sql_parts.push(j.kind.sql_ref(self.dialect));
            sql_parts.push(quote! { <#p>::__CARTEL_TABLE });
            sql_parts.push(self.dialect.kw("ON_KW"));
            sql_parts.extend(on_parts_acc[i].iter().cloned());
        }
        sql_parts.push(self.dialect.kw("WHERE_KW"));
        sql_parts.extend(where_parts);
        sql_parts.extend(self.render_on_conflict(on_conflict)?);

        self.mutation_plan(sql_parts, table_path, returning)
    }

    fn render_on_conflict(
        &mut self,
        on_conflict: &OnConflictKind,
    ) -> syn::Result<Vec<proc_macro2::TokenStream>> {
        match on_conflict {
            OnConflictKind::None => Ok(Vec::new()),
            OnConflictKind::DoNothing => Ok(vec![self.dialect.kw("ON_CONFLICT_DO_NOTHING")]),
            OnConflictKind::TargetDoNothing(target) => {
                let cols = target.cols.join(",");
                let s = format!(" ON CONFLICT ({cols}) DO NOTHING");
                Ok(vec![quote! { #s }])
            }
            OnConflictKind::TargetDoUpdate(target, upd) => {
                let cols = target.cols.join(",");
                let prefix = format!(" ON CONFLICT ({cols}) DO UPDATE SET ");
                let assigns = ColumnAssign::collect(
                    &upd.set_body,
                    &upd.set_arg,
                    self.fn_params,
                    self.param_tys,
                    &mut *self.captures,
                    self.dialect,
                    self.cte_map,
                )?;
                if assigns.is_empty() {
                    return Err(syn::Error::new(
                        upd.set_body.span(),
                        "`.do_update(...)` body must assign at least one column",
                    ));
                }
                let mut parts: Vec<proc_macro2::TokenStream> = vec![quote! { #prefix }];
                for (i, a) in assigns.iter().enumerate() {
                    if i > 0 {
                        parts.push(self.dialect.kw("COMMA"));
                    }
                    let col_eq = format!("{} = ", a.column);
                    parts.push(quote! { #col_eq });
                    parts.extend(a.value_parts.iter().cloned());
                }
                Ok(parts)
            }
        }
    }

    fn render_limit_offset(
        &mut self,
        qualifiers: &ChainQualifiers,
        terminator: Option<Terminator>,
    ) -> syn::Result<Vec<proc_macro2::TokenStream>> {
        let mut parts = Vec::new();

        let explicit_limit = match &qualifiers.limit {
            Some(expr) => Some(self.render_int_arg(expr)?),
            None => None,
        };
        let limit_part = match (explicit_limit, terminator) {
            (Some(p), _) => Some(p),
            (None, Some(Terminator::One | Terminator::First)) => Some(self.dialect.kw("LIMIT_ONE")),
            (None, _) => None,
        };
        if let Some(p) = limit_part {
            parts.push(self.dialect.kw("LIMIT_KW"));
            parts.push(p);
        }

        if let Some(expr) = &qualifiers.offset {
            let p = self.render_int_arg(expr)?;
            parts.push(self.dialect.kw("OFFSET_KW"));
            parts.push(p);
        }

        Ok(parts)
    }

    fn render_int_arg(&mut self, expr: &Expr) -> syn::Result<proc_macro2::TokenStream> {
        match expr {
            Expr::Lit(syn::ExprLit {
                lit: syn::Lit::Int(li),
                ..
            }) => {
                let n: i64 = li.base10_parse()?;
                if n < 0 {
                    return Err(syn::Error::new(
                        expr.span(),
                        "LIMIT / OFFSET cannot be negative",
                    ));
                }
                let s = n.to_string();
                Ok(quote! { #s })
            }
            Expr::Path(ExprPath { path, .. }) => {
                if path.segments.len() != 1 {
                    return Err(syn::Error::new(
                        expr.span(),
                        "LIMIT / OFFSET argument must be a literal int or a fn parameter ident",
                    ));
                }
                let id = &path.segments[0].ident;
                if !self.fn_params.iter().any(|p| p == id) {
                    return Err(syn::Error::new(
                        id.span(),
                        format!("`{id}` is not a fn parameter"),
                    ));
                }
                let idx = self.captures.intern(id.clone());
                let ctx = ParamCtx {
                    captures: &*self.captures,
                    param_ids: self.fn_params,
                    param_tys: self.param_tys,
                };
                Ok(self.dialect.placeholder(idx, &ctx))
            }
            _ => Err(syn::Error::new(
                expr.span(),
                "LIMIT / OFFSET argument must be a literal int or a fn parameter ident",
            )),
        }
    }

    fn render_order_clauses(
        &mut self,
        clauses: &[OrderClause],
        parent_scope: &RowScope,
    ) -> syn::Result<Vec<proc_macro2::TokenStream>> {
        if clauses.is_empty() {
            return Ok(Vec::new());
        }
        let mut parts = vec![self.dialect.kw("ORDER_BY_KW")];
        for (i, clause) in clauses.iter().enumerate() {
            if i > 0 {
                parts.push(self.dialect.kw("COMMA"));
            }
            let Expr::Closure(c) = &clause.closure else {
                return Err(syn::Error::new(
                    clause.closure.span(),
                    "`.order_by(...)` argument must be a closure",
                ));
            };
            if c.inputs.len() != parent_scope.vars.len() {
                return Err(syn::Error::new(
                    c.inputs.span(),
                    format!(
                        "`.order_by(|...|)` closure must take {} arg(s), matching the surrounding scope",
                        parent_scope.vars.len()
                    ),
                ));
            }
            let mut local_vars = Vec::with_capacity(c.inputs.len());
            for (input, parent) in c.inputs.iter().zip(&parent_scope.vars) {
                let ident = input.closure_ident()?;
                local_vars.push(RowVar {
                    ident,
                    table_const: parent.table_const.clone(),
                });
            }
            let scope = RowScope {
                vars: local_vars,
                agg_var: None,
                outer: None,
                primary_table_const: parent_scope.primary_table_const.clone(),
                unnest_cols: Vec::new(),
            };
            let rendered = scope
                .parse_side_expr(
                    &c.body,
                    Some((self.fn_params, &mut *self.captures)),
                    self.param_tys,
                    self.dialect,
                    self.cte_map,
                )
                .ok_or_else(|| {
                    syn::Error::new(
                        c.body.span(),
                        "`.order_by(...)` body must be a column / arithmetic / function expression",
                    )
                })?;
            parts.extend(rendered);
            if matches!(clause.dir, OrderDir::Desc) {
                parts.push(self.dialect.kw("DESC"));
            }
        }
        Ok(parts)
    }

    fn mutation_plan(
        &self,
        base_sql_parts: Vec<proc_macro2::TokenStream>,
        table_path: &Path,
        returning: ReturningKind,
    ) -> syn::Result<QueryPlan> {
        if returning == ReturningKind::None {
            return Ok(QueryPlan::no_rows(base_sql_parts));
        }
        let mut sql_parts = base_sql_parts;
        sql_parts.push(self.dialect.kw("RETURNING_KW"));
        sql_parts.push(quote! { <#table_path>::__CARTEL_SELECT_COLS });

        let row_ty: Type = syn::parse_quote! { #table_path };
        let dispatch = match returning {
            ReturningKind::One => DispatchKind::One(row_ty.clone()),
            ReturningKind::First => DispatchKind::First(row_ty.clone()),
            ReturningKind::All => DispatchKind::All(row_ty.clone()),
            ReturningKind::None => unreachable!(),
        };
        Ok(QueryPlan {
            row_ty: row_ty.clone(),
            sql_parts,
            n_result_cols: row_ty.n_cols_const(),
            decode: DecodeKind::Row(Box::new(row_ty.clone())),
            dispatch,
            probe_override: None,
        })
    }

    fn expect_mutation_return(
        &self,
        op: &str,
        returning: ReturningKind,
        table_path: &Path,
    ) -> syn::Result<()> {
        match returning {
            ReturningKind::None => match self.return_type {
                ReturnType::Default => Ok(()),
                ReturnType::Type(_, t) => match t.as_ref() {
                    Type::Tuple(tup) if tup.elems.is_empty() => Ok(()),
                    _ => Err(syn::Error::new(
                        self.return_type.span(),
                        format!(
                            "{op} without `.returning_*()` must return `()` (or omit the return type)"
                        ),
                    )),
                },
            },
            kind => {
                let outer = match self.return_type {
                    ReturnType::Type(_, t) => (**t).clone(),
                    ReturnType::Default => {
                        return Err(syn::Error::new(
                            self.return_type.span(),
                            format!(
                                "{op} with `.returning_{}()` requires an explicit return type ({})",
                                kind.name(),
                                kind.expected_return_type(),
                            ),
                        ));
                    }
                };
                let shape = ReturnShape::parse(&outer);
                let row_ty = shape.row_ty();
                let expected_path: Type = syn::parse_quote! { #table_path };
                let row_str = quote! { #row_ty }.to_string();
                let expected_str = quote! { #expected_path }.to_string();
                if row_str != expected_str {
                    return Err(syn::Error::new(
                        self.return_type.span(),
                        format!(
                            "RETURNING row type must be the table struct ({expected_str}); got {row_str}"
                        ),
                    ));
                }
                match (shape, kind) {
                    (ReturnShape::Plain(_), ReturningKind::One) => Ok(()),
                    (ReturnShape::Optional(_), ReturningKind::First) => Ok(()),
                    (ReturnShape::Many(_), ReturningKind::All) => Ok(()),
                    (s, k) => Err(syn::Error::new(
                        self.return_type.span(),
                        format!(
                            "return type {} does not match `.returning_{}()`; expected {}",
                            s.describe_actual(),
                            k.name(),
                            k.expected_return_type(),
                        ),
                    )),
                }
            }
        }
    }

    pub(super) fn compile_cte_body(
        inner_chain: &Expr,
        fn_params: &'a [Ident],
        param_tys: &'a [Type],
        captures: &'a mut Vec<Ident>,
        dialect: &'a Dialect,
        cte_map: &'a HashMap<String, Path>,
    ) -> syn::Result<Vec<proc_macro2::TokenStream>> {
        let dummy_return = ReturnType::Default;
        let mut pb = PlanBuilder {
            fn_params,
            param_tys,
            captures,
            return_type: &dummy_return,
            dialect,
            cte_map,
        };
        let mut quals = ChainQualifiers::default();
        let after = quals.peel(inner_chain)?;
        let (source, pred_idents, predicate) = QuerySource::parse_chain(&after, pb.cte_map)?;
        let primary_path = &source.primary_path;
        let scope = RowScope::for_source(&source, &pred_idents);

        let mut on_parts_per_join: Vec<Vec<proc_macro2::TokenStream>> =
            Vec::with_capacity(source.joins.len());
        for (i, j) in source.joins.iter().enumerate() {
            if j.kind.is_lateral() {
                let parts = pb.render_lateral_subquery(j, i, &source)?;
                on_parts_per_join.push(parts);
            } else {
                let on_scope = RowScope::for_join_on(&source, i);
                let mut throwaway: Vec<Ident> = Vec::new();
                let on_expr = WhereExpr::build(
                    &j.cond,
                    &on_scope,
                    pb.fn_params,
                    pb.param_tys,
                    &mut throwaway,
                    true,
                    pb.dialect,
                    pb.cte_map,
                )?;
                if !throwaway.is_empty() {
                    return Err(syn::Error::new(
                        j.cond.span(),
                        "JOIN ON clause must compare columns only; bound parameters belong in `.filter(...)`",
                    ));
                }
                let mut p = Vec::new();
                let ctx = ParamCtx {
                    captures: &*pb.captures,
                    param_ids: pb.fn_params,
                    param_tys: pb.param_tys,
                };
                on_expr.render_parts(&mut p, &ctx, pb.dialect);
                on_parts_per_join.push(p);
            }
        }

        let where_expr = WhereExpr::build(
            &predicate,
            &scope,
            pb.fn_params,
            pb.param_tys,
            &mut *pb.captures,
            false,
            pb.dialect,
            pb.cte_map,
        )?;
        let mut where_parts = Vec::new();
        {
            let ctx = ParamCtx {
                captures: &*pb.captures,
                param_ids: pb.fn_params,
                param_tys: pb.param_tys,
            };
            where_expr.render_parts(&mut where_parts, &ctx, pb.dialect);
        }

        let select_cols = if source.joins.is_empty() {
            quote! { <#primary_path>::__CARTEL_SELECT_COLS }
        } else {
            quote! { <#primary_path>::__CARTEL_SELECT_COLS_QUALIFIED }
        };

        let mut sql = vec![pb.dialect.kw("SELECT_KW"), select_cols];
        sql.extend(PlanBuilder::render_from_clause_with_on(
            &source,
            &on_parts_per_join,
            pb.dialect,
        ));
        sql.push(pb.dialect.kw("WHERE_KW"));
        sql.extend(where_parts);
        sql.extend(pb.render_order_clauses(&quals.order_by, &scope)?);
        sql.extend(pb.render_limit_offset(&quals, None)?);
        Ok(sql)
    }
}

impl QueryPlan {
    fn no_rows(sql_parts: Vec<proc_macro2::TokenStream>) -> Self {
        Self {
            row_ty: syn::parse_quote! { () },
            sql_parts,
            n_result_cols: quote! { 0u16 },
            decode: DecodeKind::Unit,
            dispatch: DispatchKind::NoRows,
            probe_override: None,
        }
    }
}

impl RowScope {
    fn for_lateral_outer(source: &QuerySource, idents: &[Ident], join_index: usize) -> Self {
        debug_assert_eq!(idents.len(), join_index + 1);
        let primary_path = &source.primary_path;
        let primary_const: proc_macro2::TokenStream = quote! { <#primary_path>::__CARTEL_TABLE };
        let mut vars = Vec::with_capacity(idents.len());
        vars.push(RowVar {
            ident: idents[0].clone(),
            table_const: Some(primary_const.clone()),
        });
        for (k, ident) in idents.iter().enumerate().skip(1) {
            let p = &source.joins[k - 1].path;
            vars.push(RowVar {
                ident: ident.clone(),
                table_const: Some(quote! { <#p>::__CARTEL_TABLE }),
            });
        }
        Self {
            vars,
            agg_var: None,
            outer: None,
            primary_table_const: Some(primary_const),
            unnest_cols: Vec::new(),
        }
    }
}

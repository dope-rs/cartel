use std::collections::HashMap;

use quote::quote;
use syn::spanned::Spanned;
use syn::{BinOp, Expr, ExprBinary, ExprField, ExprPath, Ident, Member, Path, Stmt, Type};

use crate::backend::{Dialect, ParamCtx};
use crate::shape::{AggCol, AggregateKind, QuerySource};
use crate::util::{CaptureSet, FnParamsExt};

#[derive(Clone, Copy)]
pub(super) enum Cmp {
    Eq,
    Ne,
    Lt,
    Gt,
    Le,
    Ge,
    Like,
    ILike,
    NotLike,
    NotILike,
    Glob,
    Contains,
    Overlaps,
    RegexMatch,
    RegexIMatch,
    NotRegexMatch,
    NotRegexIMatch,
    FtsMatch,
    LtreeDescOf,
    LtreeAncOf,
}

impl Cmp {
    fn sql_ref(self, dialect: &Dialect) -> proc_macro2::TokenStream {
        match self {
            Self::Eq => dialect.kw("EQ"),
            Self::Ne => dialect.kw("NE"),
            Self::Lt => dialect.kw("LT"),
            Self::Gt => dialect.kw("GT"),
            Self::Le => dialect.kw("LE"),
            Self::Ge => dialect.kw("GE"),
            Self::Like => dialect.kw("LIKE"),
            Self::ILike => dialect.kw("ILIKE"),
            Self::NotLike => dialect.kw("NOT_LIKE"),
            Self::NotILike => dialect.kw("NOT_ILIKE"),
            Self::Glob => dialect.kw("GLOB"),
            Self::Contains => dialect.kw("CONTAINS"),
            Self::Overlaps => dialect.kw("OVERLAPS"),
            Self::RegexMatch => dialect.kw("REGEX_MATCH"),
            Self::RegexIMatch => dialect.kw("REGEX_IMATCH"),
            Self::NotRegexMatch => dialect.kw("NOT_REGEX_MATCH"),
            Self::NotRegexIMatch => dialect.kw("NOT_REGEX_IMATCH"),
            Self::FtsMatch => dialect.kw("FTS_MATCH"),
            Self::LtreeDescOf => dialect.kw("LTREE_DESC_OF"),
            Self::LtreeAncOf => dialect.kw("LTREE_ANC_OF"),
        }
    }

    fn rhs_cast_suffix(self, dialect: &Dialect) -> Option<proc_macro2::TokenStream> {
        match self {
            Self::LtreeDescOf | Self::LtreeAncOf => Some(dialect.kw("CAST_LTREE_SUFFIX")),
            _ => None,
        }
    }

    fn flipped(self) -> Self {
        match self {
            Self::Eq => Self::Eq,
            Self::Ne => Self::Ne,
            Self::Lt => Self::Gt,
            Self::Gt => Self::Lt,
            Self::Le => Self::Ge,
            Self::Ge => Self::Le,
            Self::Like
            | Self::ILike
            | Self::NotLike
            | Self::NotILike
            | Self::Glob
            | Self::Contains
            | Self::Overlaps
            | Self::RegexMatch
            | Self::RegexIMatch
            | Self::NotRegexMatch
            | Self::NotRegexIMatch
            | Self::FtsMatch
            | Self::LtreeDescOf
            | Self::LtreeAncOf => self,
        }
    }
}

pub(super) enum WhereExpr {
    Compare {
        lhs: Vec<proc_macro2::TokenStream>,
        op: Cmp,
        rhs: ComparedRhs,
    },
    IsNull {
        col: Vec<proc_macro2::TokenStream>,
        negate: bool,
    },
    AnyArray {
        col: Vec<proc_macro2::TokenStream>,
        capture_idx: usize,
        negate: bool,
    },
    And(Box<Self>, Box<Self>),
    Or(Box<Self>, Box<Self>),
    Raw(Vec<proc_macro2::TokenStream>),
}

pub(super) enum ComparedRhs {
    Capture(usize),
    Column(Vec<proc_macro2::TokenStream>),
}

pub(super) enum SqlColRef {
    Bare(String),
    Qualified(proc_macro2::TokenStream, String),
}

impl SqlColRef {
    pub(super) fn append_to(&self, out: &mut Vec<proc_macro2::TokenStream>) {
        match self {
            Self::Bare(s) => {
                let lit = s.clone();
                out.push(quote! { #lit });
            }
            Self::Qualified(table_const, col) => {
                out.push(table_const.clone());
                let dot_col = format!(".{col}");
                out.push(quote! { #dot_col });
            }
        }
    }

    fn into_parts(self) -> Vec<proc_macro2::TokenStream> {
        let mut out = Vec::new();
        self.append_to(&mut out);
        out
    }
}

impl WhereExpr {
    pub(super) fn render_parts(
        &self,
        out: &mut Vec<proc_macro2::TokenStream>,
        ctx: &ParamCtx<'_>,
        dialect: &Dialect,
    ) {
        match self {
            Self::Compare { lhs, op, rhs } => {
                out.extend(lhs.iter().cloned());
                out.push(op.sql_ref(dialect));
                match rhs {
                    ComparedRhs::Capture(i) => {
                        out.push(dialect.placeholder(*i, ctx));
                        if let Some(suffix) = op.rhs_cast_suffix(dialect) {
                            out.push(suffix);
                        }
                    }
                    ComparedRhs::Column(c) => out.extend(c.iter().cloned()),
                }
            }
            Self::IsNull { col, negate } => {
                out.extend(col.iter().cloned());
                out.push(if *negate {
                    dialect.kw("IS_NOT_NULL")
                } else {
                    dialect.kw("IS_NULL")
                });
            }
            Self::AnyArray {
                col,
                capture_idx,
                negate,
            } => {
                out.extend(col.iter().cloned());

                if *negate {
                    out.push(dialect.kw("NE_ALL_OPEN"));
                } else {
                    out.push(dialect.kw("EQ_ANY_OPEN"));
                }
                out.push(dialect.placeholder(*capture_idx, ctx));
                out.push(dialect.kw("PAREN_CLOSE"));
            }
            Self::And(l, r) => {
                out.push(dialect.kw("PAREN_OPEN"));
                l.render_parts(out, ctx, dialect);
                out.push(dialect.kw("AND_PAREN_WRAP"));
                r.render_parts(out, ctx, dialect);
                out.push(dialect.kw("PAREN_CLOSE"));
            }
            Self::Or(l, r) => {
                out.push(dialect.kw("PAREN_OPEN"));
                l.render_parts(out, ctx, dialect);
                out.push(dialect.kw("OR_PAREN_WRAP"));
                r.render_parts(out, ctx, dialect);
                out.push(dialect.kw("PAREN_CLOSE"));
            }
            Self::Raw(parts) => {
                out.extend(parts.iter().cloned());
            }
        }
    }

    pub(super) fn build(
        expr: &Expr,
        scope: &RowScope,
        fn_params: &[Ident],
        param_tys: &[Type],
        captures: &mut Vec<Ident>,
        allow_col_to_col: bool,
        dialect: &Dialect,
        cte_map: &HashMap<String, Path>,
    ) -> syn::Result<Self> {
        if let Expr::Call(call) = expr
            && let Expr::Path(ExprPath { path, .. }) = call.func.as_ref()
            && path.segments.len() == 1
        {
            let name = path.segments[0].ident.to_string();
            if name == "exists" || name == "not_exists" {
                if call.args.len() != 1 {
                    return Err(syn::Error::new(
                        call.args.span(),
                        format!(
                            "`{name}(...)` takes one argument: a `Table::filter(|t| ...)` source",
                        ),
                    ));
                }
                let parts = scope.render_exists_subquery(
                    &call.args[0],
                    name == "not_exists",
                    fn_params,
                    param_tys,
                    captures,
                    dialect,
                    cte_map,
                )?;
                return Ok(Self::Raw(parts));
            }
        }

        match expr {
            Expr::Binary(ExprBinary {
                op, left, right, ..
            }) => match op {
                BinOp::And(_) => {
                    let l = Self::build(
                        left,
                        scope,
                        fn_params,
                        param_tys,
                        captures,
                        allow_col_to_col,
                        dialect,
                        cte_map,
                    )?;
                    let r = Self::build(
                        right,
                        scope,
                        fn_params,
                        param_tys,
                        captures,
                        allow_col_to_col,
                        dialect,
                        cte_map,
                    )?;
                    Ok(Self::And(Box::new(l), Box::new(r)))
                }
                BinOp::Or(_) => {
                    let l = Self::build(
                        left,
                        scope,
                        fn_params,
                        param_tys,
                        captures,
                        allow_col_to_col,
                        dialect,
                        cte_map,
                    )?;
                    let r = Self::build(
                        right,
                        scope,
                        fn_params,
                        param_tys,
                        captures,
                        allow_col_to_col,
                        dialect,
                        cte_map,
                    )?;
                    Ok(Self::Or(Box::new(l), Box::new(r)))
                }
                BinOp::Eq(_)
                | BinOp::Ne(_)
                | BinOp::Lt(_)
                | BinOp::Gt(_)
                | BinOp::Le(_)
                | BinOp::Ge(_) => {
                    let cmp = match op {
                        BinOp::Eq(_) => Cmp::Eq,
                        BinOp::Ne(_) => Cmp::Ne,
                        BinOp::Lt(_) => Cmp::Lt,
                        BinOp::Gt(_) => Cmp::Gt,
                        BinOp::Le(_) => Cmp::Le,
                        BinOp::Ge(_) => Cmp::Ge,
                        _ => unreachable!(),
                    };
                    let lhs = scope.parse_side_expr(
                        left,
                        Some((fn_params, captures)),
                        param_tys,
                        dialect,
                        cte_map,
                    );
                    if let Some(l_parts) = lhs {
                        let r_col = scope.parse_side_expr(right, None, param_tys, dialect, cte_map);
                        return match r_col {
                            Some(r_parts) => {
                                let rhs_is_col = scope.references_column(right);
                                if rhs_is_col && !allow_col_to_col {
                                    return Err(syn::Error::new(
                                        expr.span(),
                                        "both sides reference the row scope; column-to-column compare is only allowed in JOIN ON clauses",
                                    ));
                                }
                                Ok(Self::Compare {
                                    lhs: l_parts,
                                    op: cmp,
                                    rhs: ComparedRhs::Column(r_parts),
                                })
                            }
                            None => {
                                let captured = fn_params.resolve(right)?;
                                let idx = captures.intern(captured);
                                Ok(Self::Compare {
                                    lhs: l_parts,
                                    op: cmp,
                                    rhs: ComparedRhs::Capture(idx),
                                })
                            }
                        };
                    }
                    let rhs = scope.parse_side_expr(
                        right,
                        Some((fn_params, captures)),
                        param_tys,
                        dialect,
                        cte_map,
                    );
                    match rhs {
                        Some(parts) => {
                            let captured = fn_params.resolve(left)?;
                            let idx = captures.intern(captured);
                            Ok(Self::Compare {
                                lhs: parts,
                                op: cmp.flipped(),
                                rhs: ComparedRhs::Capture(idx),
                            })
                        }
                        None => Err(syn::Error::new(
                            expr.span(),
                            "expected at least one side of the comparison to be a column / arithmetic / function expression",
                        )),
                    }
                }
                _ => Err(syn::Error::new(
                    expr.span(),
                    "predicate operator not supported; use ==, !=, <, >, <=, >=, &&, ||",
                )),
            },
            Expr::Lit(syn::ExprLit {
                lit: syn::Lit::Bool(b),
                ..
            }) => {
                if b.value() {
                    Ok(Self::Raw(vec![dialect.kw("TRUE")]))
                } else {
                    Ok(Self::Raw(vec![dialect.kw("FALSE")]))
                }
            }
            Expr::Paren(p) => Self::build(
                &p.expr,
                scope,
                fn_params,
                param_tys,
                captures,
                allow_col_to_col,
                dialect,
                cte_map,
            ),
            Expr::Block(eb) => {
                let stmts = &eb.block.stmts;
                if stmts.len() != 1 {
                    return Err(syn::Error::new(
                        eb.span(),
                        "predicate block must contain exactly one tail expression",
                    ));
                }
                let Stmt::Expr(inner, _) = &stmts[0] else {
                    return Err(syn::Error::new(
                        stmts[0].span(),
                        "predicate block must contain a single tail expression",
                    ));
                };
                Self::build(
                    inner,
                    scope,
                    fn_params,
                    param_tys,
                    captures,
                    allow_col_to_col,
                    dialect,
                    cte_map,
                )
            }
            Expr::MethodCall(mc) if mc.args.is_empty() => {
                let name = mc.method.to_string();
                let negate = match name.as_str() {
                    "is_none" => false,
                    "is_some" => true,
                    _ => {
                        return Err(syn::Error::new(
                            mc.method.span(),
                            format!(
                                "method `.{name}()` not supported in predicates; allowed: is_none / is_some / in_ / not_in"
                            ),
                        ));
                    }
                };
                let col = scope.parse_side_expr(&mc.receiver, Some((fn_params, captures)), param_tys, dialect, cte_map)
                    .ok_or_else(|| {
                        syn::Error::new(
                            mc.receiver.span(),
                            "is_none/is_some receiver must be a column or column-derived expression",
                        )
                    })?;
                Ok(Self::IsNull { col, negate })
            }
            Expr::MethodCall(mc) if mc.args.len() == 1 => {
                let name = mc.method.to_string();
                let col = scope.parse_side_expr(&mc.receiver, Some((fn_params, captures)), param_tys, dialect, cte_map)
                    .ok_or_else(|| {
                        syn::Error::new(
                            mc.receiver.span(),
                            format!(
                                "`.{name}(arg)` receiver must be a column or column-derived expression"
                            ),
                        )
                    })?;
                match name.as_str() {
                    "in_" | "not_in" => {
                        dialect.reject_op(&name, mc.method.span())?;
                        let negate = name == "not_in";
                        let captured = fn_params.resolve_borrowed(&mc.args[0])?;
                        let capture_idx = captures.intern(captured);
                        Ok(Self::AnyArray {
                            col,
                            capture_idx,
                            negate,
                        })
                    }
                    "fts_match" => {
                        dialect.reject_op("fts_match", mc.method.span())?;
                        let rhs_parts = match scope.parse_side_expr(
                            &mc.args[0],
                            Some((fn_params, captures)),
                            param_tys,
                            dialect,
                            cte_map,
                        ) {
                            Some(p) => ComparedRhs::Column(p),
                            None => {
                                let captured = fn_params.resolve_borrowed(&mc.args[0])?;
                                let idx = captures.intern(captured);
                                ComparedRhs::Capture(idx)
                            }
                        };
                        Ok(Self::Compare {
                            lhs: col,
                            op: Cmp::FtsMatch,
                            rhs: rhs_parts,
                        })
                    }
                    "like" | "ilike" | "not_like" | "not_ilike" | "glob" | "pg_contains"
                    | "pg_overlaps" | "regex_match" | "regex_imatch" | "not_regex_match"
                    | "not_regex_imatch" | "is_descendant_of" | "is_ancestor_of" => {
                        dialect.reject_op(&name, mc.method.span())?;
                        let cmp = match name.as_str() {
                            "like" => Cmp::Like,
                            "ilike" => Cmp::ILike,
                            "not_like" => Cmp::NotLike,
                            "not_ilike" => Cmp::NotILike,
                            "glob" => Cmp::Glob,
                            "pg_contains" => Cmp::Contains,
                            "pg_overlaps" => Cmp::Overlaps,
                            "regex_match" => Cmp::RegexMatch,
                            "regex_imatch" => Cmp::RegexIMatch,
                            "not_regex_match" => Cmp::NotRegexMatch,
                            "not_regex_imatch" => Cmp::NotRegexIMatch,
                            "is_descendant_of" => Cmp::LtreeDescOf,
                            "is_ancestor_of" => Cmp::LtreeAncOf,
                            _ => unreachable!(),
                        };
                        let captured = fn_params.resolve_borrowed(&mc.args[0])?;
                        let idx = captures.intern(captured);
                        Ok(Self::Compare {
                            lhs: col,
                            op: cmp,
                            rhs: ComparedRhs::Capture(idx),
                        })
                    }
                    _ => Err(syn::Error::new(
                        mc.method.span(),
                        format!(
                            "method `.{name}(arg)` not supported in predicates; allowed: in_ / not_in / like / ilike / not_like / not_ilike / glob / pg_contains / pg_overlaps / regex_match / regex_imatch / not_regex_match / not_regex_imatch / is_descendant_of / is_ancestor_of"
                        ),
                    )),
                }
            }
            _ => Err(syn::Error::new(
                expr.span(),
                "predicate must be a comparison, `&& / ||` chain, or `<col>.is_none() / .is_some()`",
            )),
        }
    }
}

#[derive(Clone)]
pub(super) struct RowScope {
    pub(super) vars: Vec<RowVar>,
    pub(super) agg_var: Option<Ident>,
    pub(super) outer: Option<Box<Self>>,
    pub(super) primary_table_const: Option<proc_macro2::TokenStream>,
    pub(super) unnest_cols: Vec<Ident>,
}

#[derive(Clone)]
pub(super) struct RowVar {
    pub(super) ident: Ident,
    pub(super) table_const: Option<proc_macro2::TokenStream>,
}

impl RowScope {
    pub(super) fn single(ident: Ident) -> Self {
        Self {
            vars: vec![RowVar {
                ident,
                table_const: None,
            }],
            agg_var: None,
            outer: None,
            primary_table_const: None,
            unnest_cols: Vec::new(),
        }
    }

    pub(super) fn for_source(src: &QuerySource, idents: &[Ident]) -> Self {
        debug_assert_eq!(idents.len(), 1 + src.joins.len());
        let primary_path = &src.primary_path;
        let primary_const: proc_macro2::TokenStream = match &src.primary_alias {
            Some(alias) => {
                let alias_str = alias.to_string();
                quote! { #alias_str }
            }
            None => quote! { <#primary_path>::__CARTEL_TABLE },
        };
        if src.joins.is_empty() {
            return Self {
                vars: vec![RowVar {
                    ident: idents[0].clone(),
                    table_const: None,
                }],
                agg_var: None,
                outer: None,
                primary_table_const: Some(primary_const),
                unnest_cols: Vec::new(),
            };
        }
        let mut vars = Vec::with_capacity(idents.len());
        vars.push(RowVar {
            ident: idents[0].clone(),
            table_const: Some(primary_const.clone()),
        });
        for (j, ident) in src.joins.iter().zip(idents.iter().skip(1)) {
            let p = &j.path;
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

    pub(super) fn for_join_on(src: &QuerySource, join_index: usize) -> Self {
        let j = &src.joins[join_index];
        let idents = &j.on_idents;
        let primary_path = &src.primary_path;
        let primary_const: proc_macro2::TokenStream = quote! { <#primary_path>::__CARTEL_TABLE };
        let mut vars = Vec::with_capacity(idents.len());
        vars.push(RowVar {
            ident: idents[0].clone(),
            table_const: Some(primary_const.clone()),
        });
        for (k, ident) in idents.iter().enumerate().skip(1) {
            let p = &src.joins[k - 1].path;
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

    pub(super) fn with_agg(mut self, agg: Ident) -> Self {
        self.agg_var = Some(agg);
        self
    }

    pub(super) fn with_unnest(mut self, cols: Vec<Ident>) -> Self {
        self.unnest_cols = cols;
        self
    }

    fn unnest_ref(&self, expr: &Expr) -> Option<Vec<proc_macro2::TokenStream>> {
        let Expr::Path(ExprPath { path, .. }) = expr else {
            return None;
        };
        if path.segments.len() != 1 {
            return None;
        }
        let id = &path.segments[0].ident;
        let pos = self.unnest_cols.iter().position(|c| c == id)?;
        let col = format!("__d.f{pos}");
        Some(vec![quote! { #col }])
    }

    pub(super) fn with_outer(mut self, outer: Self) -> Self {
        self.outer = Some(Box::new(outer));
        self
    }

    pub(super) fn force_qualified(mut self) -> Self {
        let fallback = self.primary_table_const.clone();
        for v in &mut self.vars {
            if v.table_const.is_none() {
                v.table_const = fallback.clone();
            }
        }
        self
    }

    pub(super) fn column_ref(&self, expr: &Expr) -> Option<SqlColRef> {
        if let Some(c) = self.column_ref_local(expr) {
            return Some(c);
        }
        self.outer.as_ref().and_then(|o| o.column_ref(expr))
    }

    fn column_ref_local(&self, expr: &Expr) -> Option<SqlColRef> {
        let Expr::Field(ExprField { base, member, .. }) = expr else {
            return None;
        };
        let Expr::Path(ExprPath { path, .. }) = base.as_ref() else {
            return None;
        };
        if path.segments.len() != 1 {
            return None;
        }
        let var = self
            .vars
            .iter()
            .find(|v| v.ident == path.segments[0].ident)?;
        let Member::Named(col) = member else {
            return None;
        };
        let col_str = col.to_string();
        match &var.table_const {
            None => Some(SqlColRef::Bare(col_str)),
            Some(tc) => Some(SqlColRef::Qualified(tc.clone(), col_str)),
        }
    }

    pub(super) fn references_column(&self, expr: &Expr) -> bool {
        if self.column_ref(expr).is_some() || self.unnest_ref(expr).is_some() {
            return true;
        }
        match expr {
            Expr::Binary(b) => self.references_column(&b.left) || self.references_column(&b.right),
            Expr::Paren(p) => self.references_column(&p.expr),
            Expr::Call(c) => c.args.iter().any(|a| self.references_column(a)),
            Expr::MethodCall(mc) => {
                self.references_column(&mc.receiver)
                    || mc.args.iter().any(|a| self.references_column(a))
            }
            Expr::Index(idx) => {
                self.references_column(&idx.expr) || self.references_column(&idx.index)
            }
            Expr::Array(arr) => arr.elems.iter().any(|e| self.references_column(e)),
            Expr::Reference(r) => self.references_column(&r.expr),
            _ => false,
        }
    }

    pub(super) fn parse_side_expr(
        &self,
        expr: &Expr,
        mut bind_ctx: Option<(&[Ident], &mut Vec<Ident>)>,
        param_tys: &[Type],
        dialect: &Dialect,
        cte_map: &HashMap<String, Path>,
    ) -> Option<Vec<proc_macro2::TokenStream>> {
        if let Some(parts) = self.unnest_ref(expr) {
            return Some(parts);
        }
        if let Some(col) = self.column_ref(expr) {
            return Some(col.into_parts());
        }
        if let Expr::Path(ExprPath { path, .. }) = expr
            && path.segments.len() == 1
        {
            let id = &path.segments[0].ident;
            if let Some((fn_params, captures)) = bind_ctx.as_mut()
                && fn_params.iter().any(|p| p == id)
            {
                let idx = captures.intern(id.clone());
                let ctx = ParamCtx {
                    captures,
                    param_ids: fn_params,
                    param_tys,
                };
                return Some(vec![dialect.placeholder(idx, &ctx)]);
            }
        }
        match expr {
            Expr::Lit(syn::ExprLit {
                lit: syn::Lit::Int(li),
                ..
            }) => {
                let s = li.base10_digits().to_string();
                Some(vec![quote! { #s }])
            }
            Expr::Lit(syn::ExprLit {
                lit: syn::Lit::Float(li),
                ..
            }) => {
                let s = li.base10_digits().to_string();
                Some(vec![quote! { #s }])
            }
            Expr::Lit(syn::ExprLit {
                lit: syn::Lit::Bool(b),
                ..
            }) => {
                let kw = if b.value { "TRUE" } else { "FALSE" };
                Some(vec![dialect.kw(kw)])
            }
            Expr::Lit(syn::ExprLit {
                lit: syn::Lit::Str(s),
                ..
            }) => {
                let raw = s.value().replace('\'', "''");
                let sql = format!("'{raw}'");
                Some(vec![quote! { #sql }])
            }
            Expr::Binary(b) => {
                let op_str = match b.op {
                    BinOp::Add(_) => " + ",
                    BinOp::Sub(_) => " - ",
                    BinOp::Mul(_) => " * ",
                    BinOp::Div(_) => " / ",
                    BinOp::Rem(_) => " % ",
                    _ => return None,
                };
                let bc_l = bind_ctx.as_mut().map(|(fp, c)| (*fp, &mut **c));
                let l = self.parse_side_expr(&b.left, bc_l, param_tys, dialect, cte_map)?;
                let bc_r = bind_ctx.as_mut().map(|(fp, c)| (*fp, &mut **c));
                let r = self.parse_side_expr(&b.right, bc_r, param_tys, dialect, cte_map)?;
                let mut out = vec![dialect.kw("PAREN_OPEN")];
                out.extend(l);
                out.push(quote! { #op_str });
                out.extend(r);
                out.push(dialect.kw("PAREN_CLOSE"));
                Some(out)
            }
            Expr::Call(c) => {
                let Expr::Path(ExprPath { path, .. }) = c.func.as_ref() else {
                    return None;
                };
                if path.segments.len() != 1 {
                    return None;
                }
                let name = path.segments[0].ident.to_string();
                let allowed_nullary = matches!(
                    name.as_str(),
                    "now" | "current_timestamp" | "current_date" | "current_time"
                );
                let allowed_unary = matches!(
                    name.as_str(),
                    "lower"
                        | "upper"
                        | "length"
                        | "abs"
                        | "char_length"
                        | "trim"
                        | "floor"
                        | "ceil"
                        | "round"
                        | "sqrt"
                        | "to_tsvector"
                        | "to_tsquery"
                        | "plainto_tsquery"
                        | "phraseto_tsquery"
                        | "websearch_to_tsquery"
                        | "cardinality"
                );
                let allowed_binary = matches!(
                    name.as_str(),
                    "coalesce"
                        | "power"
                        | "position"
                        | "date_part"
                        | "date_trunc"
                        | "age"
                        | "ts_rank"
                        | "array_length"
                        | "regexp_match"
                );
                let allowed_ternary =
                    matches!(name.as_str(), "substring" | "replace" | "regexp_replace");
                if allowed_nullary && c.args.is_empty() {
                    let s = format!("{}()", name);
                    return Some(vec![quote! { #s }]);
                }
                if allowed_unary && c.args.len() == 1 {
                    let bc = bind_ctx.as_mut().map(|(fp, c)| (*fp, &mut **c));
                    let inner =
                        self.parse_side_expr(&c.args[0], bc, param_tys, dialect, cte_map)?;
                    let prefix = format!("{}(", name);
                    let mut out = vec![quote! { #prefix }];
                    out.extend(inner);
                    out.push(dialect.kw("PAREN_CLOSE"));
                    return Some(out);
                }
                if allowed_binary && c.args.len() == 2 {
                    let bc1 = bind_ctx.as_mut().map(|(fp, c)| (*fp, &mut **c));
                    let a = self.parse_side_expr(&c.args[0], bc1, param_tys, dialect, cte_map)?;
                    let bc2 = bind_ctx.as_mut().map(|(fp, c)| (*fp, &mut **c));
                    let b = self.parse_side_expr(&c.args[1], bc2, param_tys, dialect, cte_map)?;
                    let prefix = format!("{}(", name);
                    let mut out = vec![quote! { #prefix }];
                    out.extend(a);
                    out.push(dialect.kw("COMMA"));
                    out.extend(b);
                    out.push(dialect.kw("PAREN_CLOSE"));
                    return Some(out);
                }
                if allowed_ternary && c.args.len() == 3 {
                    let bc1 = bind_ctx.as_mut().map(|(fp, c)| (*fp, &mut **c));
                    let a = self.parse_side_expr(&c.args[0], bc1, param_tys, dialect, cte_map)?;
                    let bc2 = bind_ctx.as_mut().map(|(fp, c)| (*fp, &mut **c));
                    let b = self.parse_side_expr(&c.args[1], bc2, param_tys, dialect, cte_map)?;
                    let bc3 = bind_ctx.as_mut().map(|(fp, c)| (*fp, &mut **c));
                    let cc = self.parse_side_expr(&c.args[2], bc3, param_tys, dialect, cte_map)?;
                    let prefix = format!("{}(", name);
                    let mut out = vec![quote! { #prefix }];
                    out.extend(a);
                    out.push(dialect.kw("COMMA"));
                    out.extend(b);
                    out.push(dialect.kw("COMMA"));
                    out.extend(cc);
                    out.push(dialect.kw("PAREN_CLOSE"));
                    return Some(out);
                }
                None
            }
            Expr::Paren(p) => self.parse_side_expr(&p.expr, bind_ctx, param_tys, dialect, cte_map),
            Expr::Array(arr) => {
                let mut out: Vec<proc_macro2::TokenStream> = vec![quote! { "ARRAY[" }];
                for (i, elem) in arr.elems.iter().enumerate() {
                    if i > 0 {
                        out.push(dialect.kw("COMMA"));
                    }
                    let bc = bind_ctx.as_mut().map(|(fp, c)| (*fp, &mut **c));
                    let parts = self.parse_side_expr(elem, bc, param_tys, dialect, cte_map)?;
                    out.extend(parts);
                }
                out.push(quote! { "]" });
                Some(out)
            }
            Expr::Index(idx) => {
                let bc1 = bind_ctx.as_mut().map(|(fp, c)| (*fp, &mut **c));
                let recv = self.parse_side_expr(&idx.expr, bc1, param_tys, dialect, cte_map)?;
                let bc2 = bind_ctx.as_mut().map(|(fp, c)| (*fp, &mut **c));
                let i = self.parse_side_expr(&idx.index, bc2, param_tys, dialect, cte_map)?;
                let mut out = Vec::with_capacity(recv.len() + i.len() + 2);
                out.extend(recv);
                out.push(quote! { "[" });
                out.extend(i);
                out.push(quote! { "]" });
                Some(out)
            }
            Expr::MethodCall(mc) => {
                if let Expr::Call(_) = mc.receiver.as_ref() {
                    let mname = mc.method.to_string();
                    if matches!(mname.as_str(), "count" | "sum" | "avg" | "min" | "max") {
                        let agg_kind = match mname.as_str() {
                            "count" => {
                                if !mc.args.is_empty() {
                                    return None;
                                }
                                AggregateKind::Count
                            }
                            other => {
                                if mc.args.len() != 1 {
                                    return None;
                                }
                                let (arg, body) =
                                    crate::util::ExprExt::as_closure_single(&mc.args[0], other)
                                        .ok()?;
                                let agg_col = AggCol {
                                    args: vec![arg],
                                    body,
                                };
                                match other {
                                    "sum" => AggregateKind::Sum(agg_col),
                                    "avg" => AggregateKind::Avg(agg_col),
                                    "min" => AggregateKind::Min(agg_col),
                                    "max" => AggregateKind::Max(agg_col),
                                    _ => unreachable!(),
                                }
                            }
                        };
                        let (fn_params, captures) = bind_ctx?;
                        let parts = self
                            .render_scalar_aggregate_subquery(
                                &mc.receiver,
                                &agg_kind,
                                fn_params,
                                param_tys,
                                captures,
                                dialect,
                                cte_map,
                            )
                            .ok()?;
                        return Some(parts);
                    }
                }

                if let Some(agg_var) = &self.agg_var
                    && let Expr::Path(ExprPath { path, .. }) = mc.receiver.as_ref()
                    && path.segments.len() == 1
                    && path.segments[0].ident == *agg_var
                {
                    let mname = mc.method.to_string();
                    let agg_name: Option<&str> = match mname.as_str() {
                        "count" => Some("COUNT"),
                        "sum" => Some("SUM"),
                        "avg" => Some("AVG"),
                        "min" => Some("MIN"),
                        "max" => Some("MAX"),
                        _ => None,
                    };
                    if let Some(fname) = agg_name {
                        if mname == "count" {
                            if !mc.args.is_empty() {
                                return None;
                            }
                            return Some(vec![dialect.kw("COUNT_STAR")]);
                        }
                        if mc.args.len() != 1 {
                            return None;
                        }
                        let inner =
                            self.parse_side_expr(&mc.args[0], None, param_tys, dialect, cte_map)?;
                        let prefix = format!("{}(", fname);
                        let mut out = vec![quote! { #prefix }];
                        out.extend(inner);
                        out.push(dialect.kw("PAREN_CLOSE"));
                        return Some(out);
                    }
                }
                if mc.method == "concat" {
                    if mc.args.len() != 1 {
                        return None;
                    }
                    let recv_parts =
                        self.parse_side_expr(&mc.receiver, None, param_tys, dialect, cte_map)?;
                    let rhs_parts = if let Some(p) =
                        self.parse_side_expr(&mc.args[0], None, param_tys, dialect, cte_map)
                    {
                        p
                    } else {
                        let (fn_params, captures) = bind_ctx?;
                        let captured = fn_params.resolve_borrowed(&mc.args[0]).ok()?;
                        let idx = captures.intern(captured);
                        let ctx = ParamCtx {
                            captures: &*captures,
                            param_ids: fn_params,
                            param_tys,
                        };
                        vec![dialect.placeholder(idx, &ctx)]
                    };
                    let mut out = vec![dialect.kw("PAREN_OPEN")];
                    out.extend(recv_parts);
                    out.push(dialect.kw("CONCAT"));
                    out.extend(rhs_parts);
                    out.push(dialect.kw("PAREN_CLOSE"));
                    return Some(out);
                }
                let op = match mc.method.to_string().as_str() {
                    "json" => Some(" -> "),
                    "text" => Some(" ->> "),
                    "json_path" => Some(" #> "),
                    "text_path" => Some(" #>> "),
                    _ => None,
                }?;
                if mc.args.len() != 1 {
                    return None;
                }
                let recv_parts =
                    self.parse_side_expr(&mc.receiver, None, param_tys, dialect, cte_map)?;
                let key_parts = Self::render_json_key(&mc.args[0], bind_ctx, param_tys, dialect)?;
                let mut out = vec![dialect.kw("PAREN_OPEN")];
                out.extend(recv_parts);
                out.push(quote! { #op });
                out.extend(key_parts);
                out.push(dialect.kw("PAREN_CLOSE"));
                Some(out)
            }
            _ => None,
        }
    }

    fn render_json_key(
        expr: &Expr,
        bind_ctx: Option<(&[Ident], &mut Vec<Ident>)>,
        param_tys: &[Type],
        dialect: &Dialect,
    ) -> Option<Vec<proc_macro2::TokenStream>> {
        match expr {
            Expr::Lit(syn::ExprLit {
                lit: syn::Lit::Str(s),
                ..
            }) => {
                let raw = s.value().replace('\'', "''");
                let sql = format!("'{raw}'");
                Some(vec![quote! { #sql }])
            }
            Expr::Lit(syn::ExprLit {
                lit: syn::Lit::Int(li),
                ..
            }) => {
                let s = li.base10_digits().to_string();
                Some(vec![quote! { #s }])
            }
            Expr::Path(ExprPath { path, .. }) if path.segments.len() == 1 => {
                let id = &path.segments[0].ident;
                let (fn_params, captures) = bind_ctx?;
                if !fn_params.iter().any(|p| p == id) {
                    return None;
                }
                let idx = captures.intern(id.clone());
                let ctx = ParamCtx {
                    captures: &*captures,
                    param_ids: fn_params,
                    param_tys,
                };
                Some(vec![dialect.placeholder(idx, &ctx)])
            }
            Expr::Reference(r) => Self::render_json_key(&r.expr, bind_ctx, param_tys, dialect),
            _ => None,
        }
    }

    pub(super) fn render_exists_subquery(
        &self,
        inner_arg: &Expr,
        negate: bool,
        fn_params: &[Ident],
        param_tys: &[Type],
        captures: &mut Vec<Ident>,
        dialect: &Dialect,
        cte_map: &HashMap<String, Path>,
    ) -> syn::Result<Vec<proc_macro2::TokenStream>> {
        let (inner_source, inner_pred_idents, inner_pred) =
            QuerySource::parse_chain(inner_arg, cte_map)?;
        if !inner_source.joins.is_empty() {
            return Err(syn::Error::new(
                inner_arg.span(),
                "joined source is not yet supported inside `exists(...)` / `not_exists(...)`",
            ));
        }
        let inner_primary = inner_source.primary_path.clone();
        let outer_qualified = self.clone().force_qualified();
        let inner_scope = Self::for_source(&inner_source, &inner_pred_idents)
            .with_outer(outer_qualified)
            .force_qualified();
        let inner_where = WhereExpr::build(
            &inner_pred,
            &inner_scope,
            fn_params,
            param_tys,
            captures,
            true,
            dialect,
            cte_map,
        )?;
        let mut inner_parts = Vec::new();
        let ctx = ParamCtx {
            captures: &*captures,
            param_ids: fn_params,
            param_tys,
        };
        inner_where.render_parts(&mut inner_parts, &ctx, dialect);
        let head = if negate {
            dialect.kw("NOT_EXISTS_OPEN")
        } else {
            dialect.kw("EXISTS_OPEN")
        };
        let mut out = vec![head];
        out.push(quote! { <#inner_primary>::__CARTEL_TABLE });
        out.push(dialect.kw("WHERE_KW"));
        out.extend(inner_parts);
        out.push(dialect.kw("PAREN_CLOSE"));
        Ok(out)
    }

    pub(super) fn render_scalar_aggregate_subquery(
        &self,
        inner_chain: &Expr,
        agg_kind: &AggregateKind,
        fn_params: &[Ident],
        param_tys: &[Type],
        captures: &mut Vec<Ident>,
        dialect: &Dialect,
        cte_map: &HashMap<String, Path>,
    ) -> syn::Result<Vec<proc_macro2::TokenStream>> {
        let Expr::Call(call) = inner_chain else {
            return Err(syn::Error::new(
                inner_chain.span(),
                "scalar subquery must follow `Table::filter(|t| ...)`",
            ));
        };
        let (table_path, closure_arg, predicate) = QuerySource::parse_filter_call(call)?;
        let inner_source = QuerySource {
            primary_path: table_path.clone(),
            primary_alias: None,
            joins: Vec::new(),
        };
        let outer_qualified = self.clone().force_qualified();
        let inner_scope = Self::for_source(&inner_source, &[closure_arg])
            .with_outer(outer_qualified)
            .force_qualified();
        let inner_where = WhereExpr::build(
            &predicate,
            &inner_scope,
            fn_params,
            param_tys,
            captures,
            true,
            dialect,
            cte_map,
        )?;
        let mut inner_parts = Vec::new();
        {
            let ctx = ParamCtx {
                captures: &*captures,
                param_ids: fn_params,
                param_tys,
            };
            inner_where.render_parts(&mut inner_parts, &ctx, dialect);
        }

        let mut sql: Vec<proc_macro2::TokenStream> = vec![dialect.kw("SUBQUERY_OPEN")];
        match agg_kind.function() {
            None => sql.push(dialect.kw("COUNT_STAR")),
            Some((fn_name, agg)) => {
                let agg_inner_scope = Self::for_source(&inner_source, &agg.args)
                    .with_outer(self.clone().force_qualified())
                    .force_qualified();
                let parts = agg_inner_scope.parse_side_expr(&agg.body, Some((fn_params, captures)), param_tys, dialect, cte_map)
                    .ok_or_else(|| {
                        syn::Error::new(
                            agg.body.span(),
                            format!(
                                "scalar subquery `.{}(...)` body must be a column / arithmetic / function expression",
                                fn_name.to_lowercase(),
                            ),
                        )
                    })?;
                let prefix = format!("{fn_name}(");
                sql.push(quote! { #prefix });
                sql.extend(parts);
                sql.push(dialect.kw("PAREN_CLOSE"));
            }
        }
        sql.push(dialect.kw("FROM_KW"));
        sql.push(quote! { <#table_path>::__CARTEL_TABLE });
        sql.push(dialect.kw("WHERE_KW"));
        sql.extend(inner_parts);
        sql.push(dialect.kw("PAREN_CLOSE"));
        Ok(sql)
    }
}

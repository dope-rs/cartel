use crate::{
    backend,
    ext::{FnParamsExt as _, SqlIdent as _},
    shape,
};
use std::collections;

use syn::spanned::Spanned as _;

mod cmp;
pub(super) mod expr;
mod input;
pub(super) mod layouts;
mod literal;
pub(super) mod sql;
mod subquery;
pub(super) mod var;

use expr::Parsed;
use input::{Input, Method, Op};
use var::Var;

pub(super) struct WhereContext<'a> {
    pub(super) fn_params: &'a [syn::Ident],
    pub(super) param_tys: &'a [syn::Type],
    pub(super) dialect: &'a backend::Dialect,
    pub(super) cte_map: &'a collections::HashMap<String, syn::Path>,
}

pub(super) enum ComparedRhs {
    Capture(syn::Ident),
    Column(sql::Sql),
}

pub(super) enum SqlColRef {
    Bare(String),
    Qualified(proc_macro2::TokenStream, String),
}

impl SqlColRef {
    pub(super) fn append_to(&self, out: &mut Vec<proc_macro2::TokenStream>) {
        match self {
            Self::Bare(s) => {
                let lit = s.quoted();
                out.push(quote::quote! { #lit });
            }
            Self::Qualified(table, col) => {
                out.push(table.clone());
                let dot_col = format!(".{}", col.quoted());
                out.push(quote::quote! { #dot_col });
            }
        }
    }

    fn into_parts(self) -> Vec<proc_macro2::TokenStream> {
        let mut out = Vec::new();
        self.append_to(&mut out);
        out
    }
}

impl ComparedRhs {
    fn sql(self, lhs: sql::Sql, op: cmp::Cmp, dialect: &backend::Dialect) -> sql::Sql {
        let mut out = lhs.push(op.sql_ref(dialect));
        match self {
            Self::Capture(ident) => {
                out = out.append(sql::Sql::captured(ident));
                if op.is_ltree() {
                    out = out.push(dialect.kw("CAST_LTREE_SUFFIX"));
                }
            }
            Self::Column(column) => out = out.append(column),
        }
        out
    }
}

pub(super) fn build(
    expr: &syn::Expr,
    scope: &RowScope,
    columns: bool,
    context: &WhereContext<'_>,
) -> syn::Result<sql::Sql> {
    let params = context.fn_params;
    let types = context.param_tys;
    let dialect = context.dialect;
    let cte_map = context.cte_map;
    match Input::new(expr) {
        Input::Exists {
            syntax,
            name,
            negate,
        } => {
            if let Some(attr) = syntax.attrs.first() {
                return Err(syn::Error::new(
                    attr.span(),
                    "attributes are not supported on an existence predicate",
                ));
            }
            let mut args = syntax.args.iter();
            let Some(source) = args.next() else {
                return Err(syn::Error::new(
                    syntax.paren_token.span.join(),
                    format!("`{name}(...)` takes one argument: a `Table::filter(|t| ...)` source"),
                ));
            };
            if let Some(extra) = args.next() {
                return Err(syn::Error::new(
                    extra.span(),
                    format!("`{name}(...)` takes one argument: a `Table::filter(|t| ...)` source"),
                ));
            }
            scope.exists(source, negate, context)
        }
        Input::Binary { syntax, op } => {
            if let Some(attr) = syntax.attrs.first() {
                return Err(syn::Error::new(
                    attr.span(),
                    "attributes are not supported on a predicate operator",
                ));
            }
            let left = &syntax.left;
            let right = &syntax.right;
            match op {
                Op::And | Op::Or => {
                    let keyword = if let Op::And = op {
                        "AND_PAREN_WRAP"
                    } else {
                        "OR_PAREN_WRAP"
                    };
                    let left = build(left, scope, columns, context)?;
                    let right = build(right, scope, columns, context)?;
                    Ok(sql::Sql::infix(left, dialect.kw(keyword), right, dialect))
                }
                Op::Compare(op) => {
                    let Parsed::Sql(lhs) =
                        scope.parse(left, Some(params), types, dialect, cte_map)?
                    else {
                        return Err(syn::Error::new(
                            syntax.span(),
                            "expected at least one side of the comparison to be a column / arithmetic / function expression",
                        ));
                    };
                    let rhs = match scope.parse(right, None, types, dialect, cte_map)? {
                        Parsed::Sql(parts) => {
                            if !columns && scope.has_column(left) && scope.has_column(right) {
                                return Err(syn::Error::new(
                                    syntax.span(),
                                    "both sides reference the row scope; column-to-column compare is only allowed in JOIN ON clauses",
                                ));
                            }
                            ComparedRhs::Column(parts)
                        }
                        Parsed::Unsupported => ComparedRhs::Capture(params.resolve(right)?),
                    };
                    Ok(rhs.sql(lhs, op, dialect))
                }
                Op::Unsupported(operator) => Err(syn::Error::new(
                    operator.span(),
                    "predicate operator not supported; use ==, !=, <, >, <=, >=, &&, ||",
                )),
            }
        }
        Input::Boolean { syntax, value } => {
            if let Some(attr) = syntax.attrs.first() {
                return Err(syn::Error::new(
                    attr.span(),
                    "attributes are not supported on a boolean predicate",
                ));
            }
            let keyword = if value.value() { "TRUE" } else { "FALSE" };
            Ok(sql::Sql::tokens(vec![dialect.kw(keyword)]))
        }
        Input::Field { expr, syntax } => {
            if let Some(attr) = syntax.attrs.first() {
                return Err(syn::Error::new(
                    attr.span(),
                    "attributes are not supported on a boolean column predicate",
                ));
            }
            let col =
                scope
                    .parse(expr, None, types, dialect, cte_map)?
                    .required(syn::Error::new(
                        syntax.span(),
                        "boolean predicate must reference a column",
                    ))?;
            Ok(col.push(dialect.kw("EQ")).push(dialect.kw("TRUE")))
        }
        Input::Not(syntax) => {
            if let Some(attr) = syntax.attrs.first() {
                return Err(syn::Error::new(
                    attr.span(),
                    "attributes are not supported on a negated predicate",
                ));
            }
            let col = scope
                .parse(&syntax.expr, None, types, dialect, cte_map)?
                .required(syn::Error::new(
                    syntax.expr.span(),
                    "negated boolean predicate must reference a column",
                ))?;
            Ok(col.push(dialect.kw("EQ")).push(dialect.kw("FALSE")))
        }
        Input::Paren(syntax) => {
            if let Some(attr) = syntax.attrs.first() {
                return Err(syn::Error::new(
                    attr.span(),
                    "attributes are not supported on a parenthesized predicate",
                ));
            }
            build(&syntax.expr, scope, columns, context)
        }
        Input::Block(syntax) => {
            if let Some(attr) = syntax.attrs.first() {
                return Err(syn::Error::new(
                    attr.span(),
                    "attributes are not supported on a predicate block",
                ));
            }
            if let Some(label) = &syntax.label {
                return Err(syn::Error::new(
                    label.span(),
                    "labels are not supported on a predicate block",
                ));
            }
            let stmts = &syntax.block.stmts;
            if stmts.len() != 1 {
                return Err(syn::Error::new(
                    syntax.span(),
                    "predicate block must contain exactly one tail expression",
                ));
            }
            let stmt = &stmts[0];
            let syn::Stmt::Expr(inner, semi) = stmt else {
                return Err(syn::Error::new(
                    stmt.span(),
                    "predicate block must contain a single tail expression",
                ));
            };
            if let Some(semi) = semi {
                return Err(syn::Error::new(
                    semi.span(),
                    "predicate block must end with a tail expression, not a semicolon",
                ));
            }
            build(inner, scope, columns, context)
        }
        Input::Null(null) => null.parse(scope, context),
        Input::Method { syntax, kind } => {
            if let Some(attr) = syntax.attrs.first() {
                return Err(syn::Error::new(
                    attr.span(),
                    "attributes are not supported on a predicate method",
                ));
            }
            if let Some(generic) = &syntax.turbofish {
                return Err(syn::Error::new(
                    generic.span(),
                    "predicate methods do not take generic arguments",
                ));
            }
            let name = syntax.method.to_string();
            let mut args = syntax.args.iter();
            let Some(arg) = args.next() else {
                return Err(syn::Error::new(
                    syntax.paren_token.span.join(),
                    format!("method `.{name}(arg)` requires exactly one argument"),
                ));
            };
            if let Some(extra) = args.next() {
                return Err(syn::Error::new(
                    extra.span(),
                    format!("method `.{name}(arg)` requires exactly one argument"),
                ));
            }
            let col = scope
                .parse(&syntax.receiver, Some(params), types, dialect, cte_map)?
                .required(syn::Error::new(
                    syntax.receiver.span(),
                    format!(
                        "`.{name}(arg)` receiver must be a column or column-derived expression"
                    ),
                ))?;
            dialect.reject_op(&name, syntax.method.span())?;
            match kind {
                Method::In | Method::NotIn => {
                    let capture = params.resolve_ref(arg)?;
                    let keyword = if matches!(kind, Method::In) {
                        "EQ_ANY_OPEN"
                    } else {
                        "NE_ALL_OPEN"
                    };
                    Ok(col
                        .push(dialect.kw(keyword))
                        .append(sql::Sql::captured(capture))
                        .push(dialect.kw("PAREN_CLOSE")))
                }
                Method::Fts => {
                    let rhs = match scope.parse(arg, Some(params), types, dialect, cte_map)? {
                        Parsed::Sql(parts) => ComparedRhs::Column(parts),
                        Parsed::Unsupported => ComparedRhs::Capture(params.resolve_ref(arg)?),
                    };
                    Ok(rhs.sql(col, cmp::Cmp::FtsMatch, dialect))
                }
                Method::Compare(op) => {
                    Ok(ComparedRhs::Capture(params.resolve_ref(arg)?).sql(col, op, dialect))
                }
            }
        }
        Input::UnsupportedMethod(syntax) => Err(syn::Error::new(
            syntax.method.span(),
            format!(
                "method `.{}` not supported in predicates; allowed: is_some / is_none / in_ / not_in / fts_match / like / ilike / not_like / not_ilike / glob / pg_contains / pg_overlaps / regex_match / regex_imatch / not_regex_match / not_regex_imatch / descendant_of / ancestor_of",
                syntax.method
            ),
        )),
        Input::Unsupported(expr) => Err(syn::Error::new(
            expr.span(),
            "predicate must be a boolean column, comparison, or `&& / ||` chain",
        )),
    }
}

#[derive(Clone)]
pub(super) struct RowScope {
    pub(super) vars: Vec<Var>,
    pub(super) agg_var: Option<syn::Ident>,
    pub(super) outer: Option<Box<Self>>,
    pub(super) alias_depth: Option<usize>,
    pub(super) unnest_cols: Vec<syn::Ident>,
}

impl RowScope {
    fn for_bindings(source: &shape::QuerySource, idents: &[syn::Ident]) -> Self {
        let vars = idents
            .iter()
            .enumerate()
            .map(|(index, ident)| Var {
                ident: ident.clone(),
                table_const: Some(source.qualifier(index)),
            })
            .collect();
        Self {
            vars,
            agg_var: None,
            outer: None,
            alias_depth: Some(source.alias_depth),
            unnest_cols: Vec::new(),
        }
    }

    pub(super) fn for_outer(
        source: &shape::QuerySource,
        idents: &[syn::Ident],
        index: usize,
    ) -> syn::Result<Self> {
        let primary = idents.first().ok_or_else(|| {
            syn::Error::new(
                source.primary_path.span(),
                "outer scope needs a row binding",
            )
        })?;
        if idents.len() != index + 1 || index >= source.joins.len() {
            return Err(syn::Error::new(
                primary.span(),
                "outer row bindings do not match the lateral join position",
            ));
        }
        Ok(Self::for_bindings(source, idents))
    }
}

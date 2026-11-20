use crate::emit::token::{EQ, PAREN_CLOSE, PAREN_OPEN, fragment};
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

use input::{Input, Method, Op};
use var::Var;

pub(super) struct WhereContext<'a> {
    pub(super) params: &'a [backend::Param],
    pub(super) dialect: &'a backend::Dialect,
    pub(super) cte_map: &'a collections::BTreeMap<String, syn::Path>,
}

pub(super) enum SqlColRef {
    Bare(String),
    Qualified(proc_macro2::TokenStream, String),
}

impl SqlColRef {
    pub(super) fn into_parts(self) -> impl Iterator<Item = proc_macro2::TokenStream> {
        let (first, second) = match self {
            Self::Bare(s) => {
                let lit = s.quoted();
                (quote::quote! { #lit }, None)
            }
            Self::Qualified(table, col) => {
                let dot_col = format!(".{}", col.quoted());
                (table, Some(quote::quote! { #dot_col }))
            }
        };
        std::iter::once(first).chain(second)
    }
}

pub(super) fn build(
    expr: Option<&syn::Expr>,
    scope: &RowScope<'_>,
    columns: bool,
    context: &WhereContext<'_>,
) -> syn::Result<sql::Operand> {
    let mut sql = sql::Sql::new();
    let rust = filter_into(expr, scope, columns, context, &mut sql)?;
    Ok(sql::Operand { sql, rust })
}

fn filter_into(
    expr: Option<&syn::Expr>,
    scope: &RowScope<'_>,
    columns: bool,
    context: &WhereContext<'_>,
    sql: &mut sql::Sql,
) -> syn::Result<proc_macro2::TokenStream> {
    if let Some(expr) = expr {
        return build_into(expr, scope, columns, context, sql);
    }
    sql.push(context.dialect.boolean(true));
    Ok(quote::quote!(true))
}

fn build_into(
    expr: &syn::Expr,
    scope: &RowScope<'_>,
    columns: bool,
    context: &WhereContext<'_>,
    sql: &mut sql::Sql,
) -> syn::Result<proc_macro2::TokenStream> {
    let params = context.params;
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
            scope.exists(source, negate, context, sql)
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
                        ") AND ("
                    } else {
                        ") OR ("
                    };
                    sql.push(fragment(PAREN_OPEN));
                    let lhs = build_into(left, scope, columns, context, sql)?;
                    sql.push(fragment(keyword));
                    let rhs = build_into(right, scope, columns, context, sql)?;
                    sql.push(fragment(PAREN_CLOSE));
                    let operator = &syntax.op;
                    Ok(quote::quote! { (#lhs #operator #rhs) })
                }
                Op::Compare(op) => {
                    let Some(lhs) = scope.parse_into(left, Some(params), dialect, cte_map, sql)?
                    else {
                        return Err(syn::Error::new(
                            syntax.span(),
                            "expected at least one side of the comparison to be a column / arithmetic / function expression",
                        ));
                    };
                    sql.push(op.sql_ref());
                    let rhs = if let Some(value) =
                        scope.parse_into(right, None, dialect, cte_map, sql)?
                    {
                        if !columns && scope.has_column(left) && scope.has_column(right) {
                            return Err(syn::Error::new(
                                syntax.span(),
                                "both sides reference the row scope; column-to-column compare is only allowed in JOIN ON clauses",
                            ));
                        }
                        value
                    } else {
                        op.capture(params.resolve(right)?, sql);
                        quote::quote!(#right)
                    };
                    let operator = &syntax.op;
                    Ok(quote::quote! { (#lhs #operator #rhs) })
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
            sql.push(dialect.boolean(value.value()));
            Ok(quote::quote!(#value))
        }
        Input::Field { expr, syntax } => {
            if let Some(attr) = syntax.attrs.first() {
                return Err(syn::Error::new(
                    attr.span(),
                    "attributes are not supported on a boolean column predicate",
                ));
            }
            let Some(value) = scope.parse_into(expr, None, dialect, cte_map, sql)? else {
                return Err(syn::Error::new(
                    syntax.span(),
                    "boolean predicate must reference a column",
                ));
            };
            sql.push(fragment(EQ));
            sql.push(dialect.boolean(true));
            Ok(value)
        }
        Input::Not(syntax) => {
            if let Some(attr) = syntax.attrs.first() {
                return Err(syn::Error::new(
                    attr.span(),
                    "attributes are not supported on a negated predicate",
                ));
            }
            let Some(value) = scope.parse_into(&syntax.expr, None, dialect, cte_map, sql)? else {
                return Err(syn::Error::new(
                    syntax.expr.span(),
                    "negated boolean predicate must reference a column",
                ));
            };
            sql.push(fragment(EQ));
            sql.push(dialect.boolean(false));
            Ok(quote::quote! { !(#value) })
        }
        Input::Paren(syntax) => {
            if let Some(attr) = syntax.attrs.first() {
                return Err(syn::Error::new(
                    attr.span(),
                    "attributes are not supported on a parenthesized predicate",
                ));
            }
            build_into(&syntax.expr, scope, columns, context, sql)
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
            build_into(inner, scope, columns, context, sql)
        }
        Input::Null(null) => null.parse(scope, context, sql),
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
            let Some(receiver) =
                scope.parse_into(&syntax.receiver, Some(params), dialect, cte_map, sql)?
            else {
                return Err(syn::Error::new(
                    syntax.receiver.span(),
                    format!(
                        "`.{name}(arg)` receiver must be a column or column-derived expression"
                    ),
                ));
            };
            dialect.reject_op(&name, syntax.method.span())?;
            let (method, value) = match kind {
                Method::In | Method::NotIn => {
                    let capture = params.resolve_ref(arg)?;
                    let keyword = if matches!(kind, Method::In) {
                        " = ANY("
                    } else {
                        " <> ALL("
                    };
                    sql.push(fragment(keyword));
                    sql.capture(capture);
                    sql.push(fragment(PAREN_CLOSE));
                    ("is_in", quote::quote!(#arg))
                }
                Method::Fts => {
                    sql.push(cmp::Cmp::FtsMatch.sql_ref());
                    let value = match scope.parse_into(arg, Some(params), dialect, cte_map, sql)? {
                        Some(value) => value,
                        None => {
                            sql.capture(params.resolve_ref(arg)?);
                            quote::quote!(#arg)
                        }
                    };
                    ("is_match", value)
                }
                Method::Compare(op) => {
                    let capture = params.resolve_ref(arg)?;
                    sql.push(op.sql_ref());
                    op.capture(capture, sql);
                    let method = match op {
                        cmp::Cmp::Contains | cmp::Cmp::Overlaps => "has_all",
                        cmp::Cmp::LtreeDescOf | cmp::Cmp::LtreeAncOf => "is_below",
                        _ => "is_like",
                    };
                    (method, quote::quote!(#arg))
                }
            };
            let method = syn::Ident::new(method, syntax.method.span());
            let scope = backend::scope();
            Ok(quote::quote! { (#receiver).#method(#scope, #value) })
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

pub(super) struct RowScope<'a> {
    pub(super) vars: Vec<Var>,
    pub(super) agg_var: Option<syn::Ident>,
    pub(super) outer: Option<&'a Self>,
    pub(super) alias_depth: Option<usize>,
    pub(super) unnest_cols: &'a [syn::Ident],
    pub(super) unnest_data: &'a [syn::Ident],
}

impl RowScope<'_> {
    fn for_bindings(source: &shape::QuerySource<'_>, idents: &[syn::Ident]) -> Self {
        let tables =
            std::iter::once(&source.primary_path).chain(source.joins.iter().map(|j| j.path));
        let vars = idents
            .iter()
            .zip(tables)
            .enumerate()
            .map(|(index, (ident, table))| Var {
                referenced: Default::default(),
                ident: ident.clone(),
                ty: quote::quote!(#table),
                table_const: Some(source.qualifier(index)),
            })
            .collect();
        Self {
            vars,
            agg_var: None,
            outer: None,
            alias_depth: Some(source.alias_depth),
            unnest_cols: &[],
            unnest_data: &[],
        }
    }

    pub(super) fn for_outer(
        source: &shape::QuerySource<'_>,
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

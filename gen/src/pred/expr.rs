use crate::backend;
use crate::pred;
use crate::pred::literal::Literal;
use crate::pred::{layouts, sql, subquery};
use crate::shape;
use std::collections;

use crate::ext::ExprExt as _;
use layouts::ScopeLayout as _;
use subquery::Subquery as _;
use syn::ext::IdentExt as _;

enum Operator {
    Add(proc_macro2::Span),
    Mul(proc_macro2::Span),
}

impl Operator {
    fn new(operator: &syn::BinOp) -> Option<Self> {
        if let syn::BinOp::Add(token) = operator {
            return Some(Self::Add(token.span));
        }
        if let syn::BinOp::Mul(token) = operator {
            return Some(Self::Mul(token.span));
        }
        None
    }

    fn tokens(self) -> proc_macro2::TokenStream {
        let literal = match self {
            Self::Add(span) => syn::LitStr::new(" + ", span),
            Self::Mul(span) => syn::LitStr::new(" * ", span),
        };
        quote::quote! { #literal }
    }
}

struct Binary<'a> {
    syntax: &'a syn::ExprBinary,
    operator: Operator,
}

impl<'a> Binary<'a> {
    fn new(expr: &'a syn::Expr) -> Option<Self> {
        let syn::Expr::Binary(syntax) = expr else {
            return None;
        };
        if !syntax.attrs.is_empty() {
            return None;
        }
        Some(Self {
            syntax,
            operator: Operator::new(&syntax.op)?,
        })
    }
}

pub(super) struct Path<'a>(&'a syn::ExprPath);

impl<'a> Path<'a> {
    pub(super) fn new(expr: &'a syn::Expr) -> Option<Self> {
        let syn::Expr::Path(syntax) = expr else {
            return None;
        };
        if !syntax.attrs.is_empty()
            || syntax.qself.is_some()
            || syntax.path.leading_colon.is_some()
            || syntax
                .path
                .segments
                .iter()
                .any(|segment| segment.arguments != syn::PathArguments::None)
        {
            return None;
        }
        Some(Self(syntax))
    }

    pub(super) fn single(&self) -> Option<&'a syn::Ident> {
        let mut segments = self.0.path.segments.iter();
        let segment = segments.next()?;
        if segments.next().is_some() {
            return None;
        }
        Some(&segment.ident)
    }

    pub(super) fn function(&self) -> Option<&'a syn::Ident> {
        let mut segments = self.0.path.segments.iter().rev();
        let function = segments.next()?;
        let owner = segments.next()?;
        if owner.ident != "Dialect" {
            return None;
        }
        Some(&function.ident)
    }
}

#[derive(Clone, Copy)]
enum Function {
    Vector,
    Query,
    PlainQuery,
    Rank,
    ArrayLength,
    Cardinality,
}

impl Function {
    fn new(ident: &syn::Ident) -> Option<Self> {
        if ident == "to_tsvector" {
            Some(Self::Vector)
        } else if ident == "to_tsquery" {
            Some(Self::Query)
        } else if ident == "plainto_tsquery" {
            Some(Self::PlainQuery)
        } else if ident == "ts_rank" {
            Some(Self::Rank)
        } else if ident == "array_length" {
            Some(Self::ArrayLength)
        } else if ident == "cardinality" {
            Some(Self::Cardinality)
        } else {
            None
        }
    }

    fn name(&self) -> &str {
        match self {
            Self::Vector => "to_tsvector",
            Self::Query => "to_tsquery",
            Self::PlainQuery => "plainto_tsquery",
            Self::Rank => "ts_rank",
            Self::ArrayLength => "array_length",
            Self::Cardinality => "cardinality",
        }
    }
}

struct Call<'a> {
    syntax: &'a syn::ExprCall,
    function: Function,
}

impl<'a> Call<'a> {
    fn new(expr: &'a syn::Expr) -> Option<Self> {
        let syn::Expr::Call(syntax) = expr else {
            return None;
        };
        if !syntax.attrs.is_empty() {
            return None;
        }
        let path = Path::new(&syntax.func)?;
        let function = Function::new(path.function()?)?;
        Some(Self { syntax, function })
    }

    fn one(&self) -> Option<&'a syn::Expr> {
        let mut args = self.syntax.args.iter();
        let first = args.next()?;
        if args.next().is_some() {
            return None;
        }
        Some(first)
    }

    fn two(&self) -> Option<(&'a syn::Expr, &'a syn::Expr)> {
        let mut args = self.syntax.args.iter();
        let first = args.next()?;
        let second = args.next()?;
        if args.next().is_some() {
            return None;
        }
        Some((first, second))
    }
}

#[derive(Clone, Copy)]
enum Aggregate {
    Count,
    Column(shape::AggregateFn),
}

impl Aggregate {
    fn new(ident: &syn::Ident) -> Option<Self> {
        if ident == "count" {
            Some(Self::Count)
        } else {
            shape::AggregateFn::new(ident).map(Self::Column)
        }
    }
}

struct Method<'a> {
    syntax: &'a syn::ExprMethodCall,
    aggregate: Aggregate,
}

impl<'a> Method<'a> {
    fn new(expr: &'a syn::Expr) -> Option<Self> {
        let syn::Expr::MethodCall(syntax) = expr else {
            return None;
        };
        if !syntax.attrs.is_empty() || syntax.turbofish.is_some() {
            return None;
        }
        Some(Self {
            syntax,
            aggregate: Aggregate::new(&syntax.method)?,
        })
    }

    fn one(&self) -> Option<&'a syn::Expr> {
        let mut args = self.syntax.args.iter();
        let first = args.next()?;
        if args.next().is_some() {
            return None;
        }
        Some(first)
    }

    fn shape(&self) -> syn::Result<Option<shape::Aggregation>> {
        let function = match self.aggregate {
            Aggregate::Count => {
                return Ok(self
                    .syntax
                    .args
                    .is_empty()
                    .then_some(shape::Aggregation::Count));
            }
            Aggregate::Column(function) => function,
        };
        let Some(expr) = self.one() else {
            return Ok(None);
        };
        let (arg, body) = expr.closure_one(function.lower())?;
        Ok(Some(shape::Aggregation::Column(
            function,
            shape::AggCol {
                args: vec![arg],
                body,
            },
        )))
    }
}

pub(crate) enum Parsed {
    Sql(sql::Sql),
    Unsupported,
}

impl Parsed {
    pub(crate) fn required(self, error: syn::Error) -> syn::Result<sql::Sql> {
        match self {
            Self::Sql(sql) => Ok(sql),
            Self::Unsupported => Err(error),
        }
    }
}

pub(crate) trait Expr {
    fn has_column(&self, expr: &syn::Expr) -> bool;

    fn parse(
        &self,
        expr: &syn::Expr,
        params: Option<&[syn::Ident]>,
        types: &[syn::Type],
        dialect: &backend::Dialect,
        cte_map: &collections::HashMap<String, syn::Path>,
    ) -> syn::Result<Parsed>;
}

impl Expr for pred::RowScope {
    fn has_column(&self, expr: &syn::Expr) -> bool {
        if self.column_ref(expr).is_some() || self.unnest_ref(expr).is_some() {
            return true;
        }
        if let Some(binary) = Binary::new(expr) {
            return self.has_column(&binary.syntax.left) || self.has_column(&binary.syntax.right);
        }
        if let syn::Expr::Paren(paren) = expr
            && paren.attrs.is_empty()
        {
            return self.has_column(&paren.expr);
        }
        if let Some(call) = Call::new(expr) {
            return call.syntax.args.iter().any(|arg| self.has_column(arg));
        }
        if let Some(method) = Method::new(expr) {
            return self.has_column(&method.syntax.receiver)
                || method.syntax.args.iter().any(|arg| self.has_column(arg));
        }
        false
    }

    fn parse(
        &self,
        expr: &syn::Expr,
        params: Option<&[syn::Ident]>,
        types: &[syn::Type],
        dialect: &backend::Dialect,
        cte_map: &collections::HashMap<String, syn::Path>,
    ) -> syn::Result<Parsed> {
        if let Some(parts) = self.unnest_ref(expr) {
            return Ok(Parsed::Sql(sql::Sql::tokens(parts)));
        }
        if let Some(column) = self.column_ref(expr) {
            return Ok(Parsed::Sql(sql::Sql::tokens(column.into_parts())));
        }
        if let Some(params) = params
            && let Some(ident) = Path::new(expr).and_then(|path| path.single())
            && params.iter().any(|param| param.unraw() == ident.unraw())
        {
            return Ok(Parsed::Sql(sql::Sql::captured(ident.clone())));
        }
        if let Some(binary) = Binary::new(expr) {
            let Parsed::Sql(left) =
                self.parse(&binary.syntax.left, params, types, dialect, cte_map)?
            else {
                return Ok(Parsed::Unsupported);
            };
            let Parsed::Sql(right) =
                self.parse(&binary.syntax.right, params, types, dialect, cte_map)?
            else {
                return Ok(Parsed::Unsupported);
            };
            return Ok(Parsed::Sql(sql::Sql::infix(
                left,
                binary.operator.tokens(),
                right,
                dialect,
            )));
        }
        if let Some(call) = Call::new(expr) {
            let prefix = format!("{}(", call.function.name());
            return match call.function {
                Function::Vector
                | Function::Query
                | Function::PlainQuery
                | Function::Cardinality => {
                    let Some(arg) = call.one() else {
                        return Ok(Parsed::Unsupported);
                    };
                    let Parsed::Sql(arg) = self.parse(arg, params, types, dialect, cte_map)? else {
                        return Ok(Parsed::Unsupported);
                    };
                    Ok(Parsed::Sql(sql::Sql::call(
                        quote::quote! { #prefix },
                        [arg],
                        dialect,
                    )))
                }
                Function::Rank | Function::ArrayLength => {
                    let Some((first, second)) = call.two() else {
                        return Ok(Parsed::Unsupported);
                    };
                    let Parsed::Sql(first) = self.parse(first, params, types, dialect, cte_map)?
                    else {
                        return Ok(Parsed::Unsupported);
                    };
                    let Parsed::Sql(second) =
                        self.parse(second, params, types, dialect, cte_map)?
                    else {
                        return Ok(Parsed::Unsupported);
                    };
                    Ok(Parsed::Sql(sql::Sql::call(
                        quote::quote! { #prefix },
                        [first, second],
                        dialect,
                    )))
                }
            };
        }
        if let syn::Expr::Paren(paren) = expr {
            if !paren.attrs.is_empty() {
                return Ok(Parsed::Unsupported);
            }
            return self.parse(&paren.expr, params, types, dialect, cte_map);
        }
        if let Some(method) = Method::new(expr) {
            if let syn::Expr::Call(receiver) = method.syntax.receiver.as_ref() {
                let Some(kind) = method.shape()? else {
                    return Ok(Parsed::Unsupported);
                };
                let Some(params) = params else {
                    return Ok(Parsed::Unsupported);
                };
                let context = pred::WhereContext {
                    fn_params: params,
                    param_tys: types,
                    dialect,
                    cte_map,
                };
                return Ok(Parsed::Sql(self.scalar_agg(receiver, &kind, &context)?));
            }
            if let Some(agg_var) = &self.agg_var
                && let Some(receiver) =
                    Path::new(&method.syntax.receiver).and_then(|path| path.single())
                && receiver.unraw() == agg_var.unraw()
            {
                let function = match method.aggregate {
                    Aggregate::Count => {
                        if !method.syntax.args.is_empty() {
                            return Ok(Parsed::Unsupported);
                        }
                        return Ok(Parsed::Sql(sql::Sql::tokens(vec![
                            dialect.kw("COUNT_STAR"),
                        ])));
                    }
                    Aggregate::Column(function) => function,
                };
                let Some(arg) = method.one() else {
                    return Ok(Parsed::Unsupported);
                };
                let Parsed::Sql(inner) = self.parse(arg, None, types, dialect, cte_map)? else {
                    return Ok(Parsed::Unsupported);
                };
                let prefix = format!("{}(", function.upper());
                return Ok(Parsed::Sql(sql::Sql::call(
                    quote::quote! { #prefix },
                    [inner],
                    dialect,
                )));
            }
            return Ok(Parsed::Unsupported);
        }
        match Literal::new(expr) {
            Some(literal) => Ok(Parsed::Sql(literal.render(dialect))),
            None => Ok(Parsed::Unsupported),
        }
    }
}

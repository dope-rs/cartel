use crate::emit::token::{COMMA, COUNT_STAR, PAREN_CLOSE, PAREN_OPEN, fragment};
use crate::backend;
use crate::pred;
use crate::pred::literal::Literal;
use crate::pred::sql;
use crate::shape;
use std::collections;

use crate::ext::ExprExt as _;
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
}

pub(super) enum Aggregate<'a> {
    Count,
    Column(shape::AggregateFn, &'a syn::Ident, &'a syn::Expr),
}

struct Method<'a> {
    syntax: &'a syn::ExprMethodCall,
    function: Option<shape::AggregateFn>,
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
            function: if syntax.method == "count" {
                None
            } else {
                Some(shape::AggregateFn::new(&syntax.method)?)
            },
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

    fn shape(&self) -> syn::Result<Option<Aggregate<'a>>> {
        let function = match self.function {
            None => {
                return Ok(self.syntax.args.is_empty().then_some(Aggregate::Count));
            }
            Some(function) => function,
        };
        let Some(expr) = self.one() else {
            return Ok(None);
        };
        let (arg, body) = expr.closure_one(function.lower())?;
        Ok(Some(Aggregate::Column(function, arg, body)))
    }
}

impl pred::RowScope<'_> {
    pub(super) fn has_column(&self, expr: &syn::Expr) -> bool {
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

    pub(crate) fn parse(
        &self,
        expr: &syn::Expr,
        params: Option<&[backend::Param]>,
        dialect: &backend::Dialect,
        cte_map: &collections::BTreeMap<String, syn::Path>,
    ) -> syn::Result<Option<sql::Operand>> {
        let mut sql = sql::Sql::new();
        Ok(self
            .parse_into(expr, params, dialect, cte_map, &mut sql)?
            .map(|rust| sql::Operand { sql, rust }))
    }

    pub(crate) fn parse_into(
        &self,
        expr: &syn::Expr,
        params: Option<&[backend::Param]>,
        dialect: &backend::Dialect,
        cte_map: &collections::BTreeMap<String, syn::Path>,
        sql: &mut sql::Sql,
    ) -> syn::Result<Option<proc_macro2::TokenStream>> {
        let start = sql.len();
        let rust = self.write_expr(expr, params, dialect, cte_map, sql)?;
        if rust.is_none() {
            sql.truncate(start);
        }
        Ok(rust)
    }

    fn write_expr(
        &self,
        expr: &syn::Expr,
        params: Option<&[backend::Param]>,
        dialect: &backend::Dialect,
        cte_map: &collections::BTreeMap<String, syn::Path>,
        sql: &mut sql::Sql,
    ) -> syn::Result<Option<proc_macro2::TokenStream>> {
        if let Some((token, column)) = self.unnest_ref(expr) {
            sql.push(token);
            return Ok(Some(quote::quote! { #column }));
        } else if let Some(column) = self.column_ref(expr) {
            sql.extend(column.into_parts());
        } else if let Some(params) = params
            && let Some(ident) = Path::new(expr).and_then(|path| path.single())
            && let Some(index) = params
                .iter()
                .position(|(param, _)| param.unraw() == ident.unraw())
        {
            sql.capture((index, ident.span()));
        } else if let Some(binary) = Binary::new(expr) {
            sql.push(fragment(PAREN_OPEN));
            let Some(left) = self.write_expr(&binary.syntax.left, params, dialect, cte_map, sql)?
            else {
                return Ok(None);
            };
            let op = &binary.syntax.op;
            sql.push(binary.operator.tokens());
            let Some(right) =
                self.write_expr(&binary.syntax.right, params, dialect, cte_map, sql)?
            else {
                return Ok(None);
            };
            sql.push(fragment(PAREN_CLOSE));
            return Ok(Some(quote::quote! { (#left #op #right) }));
        } else if let Some(call) = Call::new(expr) {
            let arity = match call.function {
                Function::Vector
                | Function::Query
                | Function::PlainQuery
                | Function::Cardinality => 1,
                Function::Rank | Function::ArrayLength => 2,
            };
            if call.syntax.args.len() != arity {
                return Ok(None);
            }
            let prefix = format!("{}(", call.function.name());
            sql.push(quote::quote! { #prefix });
            let mut args = Vec::with_capacity(arity);
            for arg in &call.syntax.args {
                if !args.is_empty() {
                    sql.push(fragment(COMMA));
                }
                let Some(value) = self.write_expr(arg, params, dialect, cte_map, sql)? else {
                    return Ok(None);
                };
                args.push(value);
            }
            sql.push(fragment(PAREN_CLOSE));
            let method = syn::Ident::new(
                match call.function {
                    Function::Vector => "vector",
                    Function::Query | Function::PlainQuery => "query",
                    Function::Rank => "rank",
                    Function::ArrayLength => "array_len",
                    Function::Cardinality => "cardinality",
                },
                call.syntax.paren_token.span.open(),
            );
            let scope = backend::scope();
            let rt = &dialect.rt_crate;
            return Ok(Some(
                quote::quote! { #rt::dsl::expr::#method(#scope, #(#args),*) },
            ));
        } else if let syn::Expr::Paren(paren) = expr {
            if !paren.attrs.is_empty() {
                return Ok(None);
            }
            return self.write_expr(&paren.expr, params, dialect, cte_map, sql);
        } else if let Some(method) = Method::new(expr) {
            if let syn::Expr::Call(receiver) = method.syntax.receiver.as_ref() {
                let (Some(kind), Some(params)) = (method.shape()?, params) else {
                    return Ok(None);
                };
                let context = pred::WhereContext {
                    params,
                    dialect,
                    cte_map,
                };
                return self.scalar_agg(receiver, kind, &context, sql).map(Some);
            }
            if let Some(agg) = &self.agg_var
                && let Some(receiver) =
                    Path::new(&method.syntax.receiver).and_then(|path| path.single())
                && receiver.unraw() == agg.unraw()
                && method.function.is_none()
                && method.syntax.args.is_empty()
            {
                sql.push(fragment(COUNT_STAR));
                return Ok(Some(quote::quote! { 0i64 }));
            }
            return Ok(None);
        } else if let Some(literal) = Literal::new(expr) {
            sql.push(literal.render(dialect));
        } else {
            return Ok(None);
        }
        Ok(Some(quote::quote! { #expr }))
    }
}

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
    ) -> syn::Result<Parsed> {
        let mut sql = sql::Sql::new();
        if self.parse_into(expr, params, dialect, cte_map, &mut sql)? {
            Ok(Parsed::Sql(sql))
        } else {
            Ok(Parsed::Unsupported)
        }
    }

    pub(super) fn parse_into(
        &self,
        expr: &syn::Expr,
        params: Option<&[backend::Param]>,
        dialect: &backend::Dialect,
        cte_map: &collections::BTreeMap<String, syn::Path>,
        sql: &mut sql::Sql,
    ) -> syn::Result<bool> {
        let start = sql.len();
        let parsed = self.write_expr(expr, params, dialect, cte_map, sql)?;
        if !parsed {
            sql.truncate(start);
        }
        Ok(parsed)
    }

    fn write_expr(
        &self,
        expr: &syn::Expr,
        params: Option<&[backend::Param]>,
        dialect: &backend::Dialect,
        cte_map: &collections::BTreeMap<String, syn::Path>,
        sql: &mut sql::Sql,
    ) -> syn::Result<bool> {
        if let Some(token) = self.unnest_ref(expr) {
            sql.push(token);
            return Ok(true);
        }
        if let Some(column) = self.column_ref(expr) {
            sql.extend(column.into_parts());
            return Ok(true);
        }
        if let Some(params) = params
            && let Some(ident) = Path::new(expr).and_then(|path| path.single())
            && let Some(index) = params
                .iter()
                .position(|(param, _)| param.unraw() == ident.unraw())
        {
            sql.capture((index, ident.span()));
            return Ok(true);
        }
        if let Some(binary) = Binary::new(expr) {
            sql.push(dialect.kw("PAREN_OPEN"));
            if !self.write_expr(&binary.syntax.left, params, dialect, cte_map, sql)? {
                return Ok(false);
            }
            sql.push(binary.operator.tokens());
            if !self.write_expr(&binary.syntax.right, params, dialect, cte_map, sql)? {
                return Ok(false);
            }
            sql.push(dialect.kw("PAREN_CLOSE"));
            return Ok(true);
        }
        if let Some(call) = Call::new(expr) {
            let arity = match call.function {
                Function::Vector
                | Function::Query
                | Function::PlainQuery
                | Function::Cardinality => 1,
                Function::Rank | Function::ArrayLength => 2,
            };
            if call.syntax.args.len() != arity {
                return Ok(false);
            }
            let prefix = format!("{}(", call.function.name());
            sql.push(quote::quote! { #prefix });
            for (index, arg) in call.syntax.args.iter().enumerate() {
                if index > 0 {
                    sql.push(dialect.kw("COMMA"));
                }
                if !self.write_expr(arg, params, dialect, cte_map, sql)? {
                    return Ok(false);
                }
            }
            sql.push(dialect.kw("PAREN_CLOSE"));
            return Ok(true);
        }
        if let syn::Expr::Paren(paren) = expr {
            if !paren.attrs.is_empty() {
                return Ok(false);
            }
            return self.write_expr(&paren.expr, params, dialect, cte_map, sql);
        }
        if let Some(method) = Method::new(expr) {
            if let syn::Expr::Call(receiver) = method.syntax.receiver.as_ref() {
                let Some(kind) = method.shape()? else {
                    return Ok(false);
                };
                let Some(params) = params else {
                    return Ok(false);
                };
                let context = pred::WhereContext {
                    params,
                    dialect,
                    cte_map,
                };
                self.scalar_agg(receiver, kind, &context, sql)?;
                return Ok(true);
            }
            if let Some(agg_var) = &self.agg_var
                && let Some(receiver) =
                    Path::new(&method.syntax.receiver).and_then(|path| path.single())
                && receiver.unraw() == agg_var.unraw()
            {
                let function = match method.function {
                    None => {
                        if !method.syntax.args.is_empty() {
                            return Ok(false);
                        }
                        sql.push(dialect.kw("COUNT_STAR"));
                        return Ok(true);
                    }
                    Some(function) => function,
                };
                let Some(arg) = method.one() else {
                    return Ok(false);
                };
                let prefix = format!("{}(", function.upper());
                sql.push(quote::quote! { #prefix });
                if !self.write_expr(arg, None, dialect, cte_map, sql)? {
                    return Ok(false);
                }
                sql.push(dialect.kw("PAREN_CLOSE"));
                return Ok(true);
            }
            return Ok(false);
        }
        match Literal::new(expr) {
            Some(literal) => {
                sql.push(literal.render(dialect));
                Ok(true)
            }
            None => Ok(false),
        }
    }
}

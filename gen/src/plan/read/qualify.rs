use crate::backend;
use crate::plan;
use crate::pred::{self, sql::Sql};
use crate::shape::{self, bound, qualify};

use syn::{ext::IdentExt as _, spanned::Spanned as _};

pub(in crate::plan) struct Qualify {
    clauses: Vec<Clause>,
    limit: Limit,
    offset: Offset,
    lock: Option<proc_macro2::TokenStream>,
}

pub(in crate::plan) enum Integer {
    Literal(proc_macro2::TokenStream),
    Param(backend::Capture),
}

struct Clause {
    sql: Sql,
    dir: qualify::Dir,
}

enum Limit {
    None,
    One,
    Value(Integer),
}

enum Offset {
    None,
    Value(Integer),
}

impl Qualify {
    pub(in crate::plan) fn new(
        quals: &qualify::State,
        end: Option<shape::Terminator>,
        parent: &pred::RowScope<'_>,
        state: &plan::State<'_, '_>,
    ) -> syn::Result<Self> {
        if !matches!(quals.distinct, qualify::Distinct::None) {
            quals
                .lock
                .reject("row locking is not supported with DISTINCT")?;
        }
        let lock = quals.lock.try_render(state.dialect)?;
        let limit = Limit::new(&quals.limit, end, state.params)?;
        let offset = Offset::new(&quals.offset, state.params)?;
        let mut clauses = Vec::with_capacity(quals.order_by.len());
        for clause in &quals.order_by {
            clauses.push(Clause::new(state, clause, parent)?);
        }
        Ok(Self {
            clauses,
            limit,
            offset,
            lock,
        })
    }

    pub(in crate::plan) fn bounds(
        quals: &qualify::State,
        end: shape::Terminator,
        state: &plan::State<'_, '_>,
    ) -> syn::Result<Self> {
        quals
            .lock
            .reject("row locking is not supported on set-operation queries")?;
        if !matches!(quals.distinct, qualify::Distinct::None) {
            return Err(syn::Error::new(
                state.return_type.span(),
                "DISTINCT qualifiers are not supported on set-operation queries",
            ));
        }
        let limit = Limit::new(&quals.limit, Some(end), state.params)?;
        let offset = Offset::new(&quals.offset, state.params)?;
        Ok(Self {
            clauses: Vec::new(),
            limit,
            offset,
            lock: None,
        })
    }

    pub(in crate::plan) fn render(
        self,
        state: &mut plan::State<'_, '_>,
    ) -> Vec<proc_macro2::TokenStream> {
        let mut value = if self.clauses.is_empty() {
            Vec::new()
        } else {
            vec![state.dialect.kw("ORDER_BY_KW")]
        };
        for (index, clause) in self.clauses.into_iter().enumerate() {
            if index > 0 {
                value.push(state.dialect.kw("COMMA"));
            }
            value.extend(state.render(clause.sql));
            if matches!(clause.dir, qualify::Dir::Desc) {
                value.push(state.dialect.kw("DESC"));
            }
        }
        value.extend(self.limit.render(state, &self.offset));
        value.extend(self.offset.render(state));
        value.extend(self.lock);
        value
    }
}

impl Integer {
    fn invalid(span: proc_macro2::Span) -> syn::Error {
        syn::Error::new(
            span,
            "LIMIT / OFFSET argument must be a literal int or a fn parameter ident",
        )
    }

    pub(in crate::plan) fn parse(expr: &syn::Expr, params: &[backend::Param]) -> syn::Result<Self> {
        if let syn::Expr::Lit(literal) = expr {
            let syn::ExprLit { attrs, lit } = literal;
            if let Some(attr) = attrs.first() {
                return Err(Self::invalid(attr.span()));
            }
            if let syn::Lit::Int(literal) = lit {
                let value: i64 = literal.base10_parse()?;
                let text = value.to_string();
                return Ok(Self::Literal(quote::quote! { #text }));
            }
            return Err(Self::invalid(lit.span()));
        }
        if let syn::Expr::Path(node) = expr {
            let syn::ExprPath { attrs, qself, path } = node;
            if let Some(attr) = attrs.first() {
                return Err(Self::invalid(attr.span()));
            }
            if let Some(qself) = qself {
                return Err(Self::invalid(qself.span()));
            }
            if let Some(colon) = path.leading_colon {
                return Err(Self::invalid(colon.span()));
            }
            if path.segments.len() != 1 {
                return Err(Self::invalid(path.span()));
            }
            let segment = &path.segments[0];
            match &segment.arguments {
                syn::PathArguments::None => {}
                syn::PathArguments::AngleBracketed(args) => {
                    return Err(Self::invalid(args.span()));
                }
                syn::PathArguments::Parenthesized(args) => {
                    return Err(Self::invalid(args.span()));
                }
            }
            let id = &segment.ident;
            let Some(index) = params
                .iter()
                .position(|(param, _)| param.unraw() == id.unraw())
            else {
                return Err(syn::Error::new(
                    id.span(),
                    format!("`{id}` is not a fn parameter"),
                ));
            };
            return Ok(Self::Param((index, id.span())));
        }
        Err(Self::invalid(expr.span()))
    }

    pub(in crate::plan) fn render(
        self,
        state: &mut plan::State<'_, '_>,
        bound: plan::Bound,
    ) -> proc_macro2::TokenStream {
        match self {
            Self::Literal(value) => value,
            Self::Param(capture) => {
                let idx = pred::sql::intern(&mut state.captures, capture);
                state.mark_nonneg(capture.0, bound);
                state.dialect.placeholder(idx, &state.params[capture.0].1)
            }
        }
    }
}

impl Limit {
    fn new(
        bound: &bound::Bound,
        end: Option<shape::Terminator>,
        params: &[backend::Param],
    ) -> syn::Result<Self> {
        match bound {
            bound::Bound::Expr(expr) => Ok(Self::Value(Integer::parse(expr, params)?)),
            bound::Bound::None => Ok(match end {
                Some(shape::Terminator::One | shape::Terminator::First) => Self::One,
                Some(shape::Terminator::All | shape::Terminator::Stream) | None => Self::None,
            }),
        }
    }

    fn render(
        self,
        state: &mut plan::State<'_, '_>,
        offset: &Offset,
    ) -> Vec<proc_macro2::TokenStream> {
        match self {
            Self::None
                if matches!(state.dialect.kind, backend::Kind::Sqlite)
                    && matches!(offset, Offset::Value(_)) =>
            {
                vec![state.dialect.kw("LIMIT_KW"), quote::quote! { "-1" }]
            }
            Self::None => Vec::new(),
            Self::One => vec![state.dialect.kw("LIMIT_KW"), state.dialect.kw("LIMIT_ONE")],
            Self::Value(value) => vec![
                state.dialect.kw("LIMIT_KW"),
                value.render(state, plan::Bound::Limit),
            ],
        }
    }
}

impl Offset {
    fn new(bound: &bound::Bound, params: &[backend::Param]) -> syn::Result<Self> {
        match bound {
            bound::Bound::None => Ok(Self::None),
            bound::Bound::Expr(expr) => Ok(Self::Value(Integer::parse(expr, params)?)),
        }
    }

    fn render(self, state: &mut plan::State<'_, '_>) -> Vec<proc_macro2::TokenStream> {
        match self {
            Self::None => Vec::new(),
            Self::Value(value) => vec![
                state.dialect.kw("OFFSET_KW"),
                value.render(state, plan::Bound::Offset),
            ],
        }
    }
}

impl Clause {
    fn new(
        state: &plan::State<'_, '_>,
        clause: &qualify::Order,
        parent: &pred::RowScope<'_>,
    ) -> syn::Result<Self> {
        let syn::Expr::Closure(closure) = &clause.closure else {
            return Err(syn::Error::new(
                clause.closure.span(),
                "`.order_by(...)` argument must be a closure",
            ));
        };
        let scope = pred::RowScope::for_order(parent, &closure.inputs)?;
        let sql = state.parse(&scope, &closure.body, || {
            syn::Error::new(
                closure.body.span(),
                "`.order_by(...)` body must be a column / arithmetic / function expression",
            )
        })?;
        Ok(Self {
            sql,
            dir: clause.dir,
        })
    }
}

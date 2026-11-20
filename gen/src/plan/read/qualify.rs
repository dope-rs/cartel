use crate::emit::token::{COMMA, DESC, ORDER_BY, fragment};
use crate::backend;
use crate::plan;
use crate::pred::{self, sql::Sql};
use crate::shape::{self, qualify};

use syn::{ext::IdentExt as _, spanned::Spanned as _};

pub(in crate::plan) struct Qualify {
    order: Sql,
    bounds: Bounds,
    lock: Option<proc_macro2::TokenStream>,
}

pub(in crate::plan) enum Integer {
    Literal(proc_macro2::TokenStream),
    Param(backend::Capture),
}

pub(in crate::plan) struct Bounds {
    limit: Option<Integer>,
    offset: Option<Integer>,
}

impl Qualify {
    pub(in crate::plan) fn new(
        quals: &qualify::State<'_>,
        end: Option<shape::Terminator>,
        parent: &pred::RowScope<'_>,
        state: &mut plan::State<'_, '_>,
    ) -> syn::Result<Self> {
        if !matches!(quals.distinct, qualify::Distinct::None) {
            quals
                .lock
                .reject("row locking is not supported with DISTINCT")?;
        }
        let lock = quals.lock.try_render(state.dialect)?;
        let bounds = Bounds::new(quals, end, state)?;
        let mut order = Sql::new();
        for (index, clause) in quals.order_by.iter().enumerate() {
            let syn::Expr::Closure(closure) = clause.closure else {
                return Err(syn::Error::new(
                    clause.closure.span(),
                    "`.order_by(...)` argument must be a closure",
                ));
            };
            let scope = pred::RowScope::for_order(parent, &closure.inputs)?;
            order.push(fragment(if index == 0 { ORDER_BY } else { COMMA }));
            let Some(value) = scope.parse_into(
                &closure.body,
                Some(state.params),
                state.dialect,
                &state.cte_map,
                &mut order,
            )?
            else {
                return Err(syn::Error::new(
                    closure.body.span(),
                    "`.order_by(...)` body must be a column / arithmetic / function expression",
                ));
            };
            let rust = scope.bound(value);
            state.checks.extend(quote::quote! { let _ = #rust; });
            if matches!(clause.dir, qualify::Dir::Desc) {
                order.push(fragment(DESC));
            }
        }
        Ok(Self {
            order,
            bounds,
            lock,
        })
    }

    pub(in crate::plan) fn render_into(
        self,
        state: &mut plan::State<'_, '_>,
        output: &mut Vec<proc_macro2::TokenStream>,
    ) {
        state.render_into(self.order, output);
        self.bounds.render_into(state, output);
        output.extend(self.lock);
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
                if !matches!(literal.suffix(), "" | "i64") {
                    return Err(Self::invalid(literal.span()));
                }
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
                let id = &state.params[capture.0].0;
                state.checks.extend(quote::quote! { let _: i64 = #id; });
                state.mark_nonneg(capture.0, bound);
                state.dialect.placeholder(idx, &state.params[capture.0].1)
            }
        }
    }
}

impl Bounds {
    pub(in crate::plan) fn new(
        quals: &qualify::State<'_>,
        end: Option<shape::Terminator>,
        state: &plan::State<'_, '_>,
    ) -> syn::Result<Self> {
        let limit = match quals.limit {
            Some(expr) => Some(Integer::parse(expr, state.params)?),
            None if matches!(end, Some(shape::Terminator::One | shape::Terminator::First)) => {
                Some(Integer::Literal(fragment("1")))
            }
            None if matches!(state.dialect.kind, backend::Kind::Sqlite)
                && quals.offset.is_some() =>
            {
                Some(Integer::Literal(quote::quote! { "-1" }))
            }
            None => None,
        };
        let offset = quals
            .offset
            .map(|expr| Integer::parse(expr, state.params))
            .transpose()?;
        Ok(Self { limit, offset })
    }

    pub(in crate::plan) fn render_into(
        self,
        state: &mut plan::State<'_, '_>,
        output: &mut Vec<proc_macro2::TokenStream>,
    ) {
        if let Some(limit) = self.limit {
            output.push(fragment(" LIMIT "));
            output.push(limit.render(state, plan::Bound::Limit));
        }
        if let Some(offset) = self.offset {
            output.push(fragment(" OFFSET "));
            output.push(offset.render(state, plan::Bound::Offset));
        }
    }
}

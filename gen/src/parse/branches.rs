use crate::shape;
use crate::shape::{lock, qualify};
use std::collections;

use syn::spanned::Spanned as _;

use crate::ext::ExprExt as _;

impl shape::SelectBranch {
    pub(super) fn parse_set(
        expr: &syn::Expr,
        cte_map: &collections::HashMap<String, syn::Path>,
    ) -> syn::Result<shape::SetQuery> {
        let mut links = Vec::new();
        let mut current = expr;
        while let syn::Expr::MethodCall(call) = current {
            let Some(op) = shape::SetOpKind::from_method(&call.method.to_string()) else {
                break;
            };
            if call.args.len() != 1 {
                return Err(syn::Error::new(
                    call.args.span(),
                    format!(
                        "`.{}(...)` takes one argument: another `Table::filter(|t| ...)` chain",
                        call.method,
                    ),
                ));
            }
            links.push((op, &call.args[0]));
            current = call.receiver.as_ref();
        }

        links.reverse();
        let (source, idents, pred) = shape::QuerySource::parse_chain(current, cte_map)?;
        let first = Self {
            source,
            pred_idents: idents,
            predicate: pred,
        };
        let mut parsed = Vec::with_capacity(links.len());
        for (op, operand) in links {
            let (source, idents, pred) = shape::QuerySource::parse_chain(operand, cte_map)?;
            parsed.push((
                op,
                Self {
                    source,
                    pred_idents: idents,
                    predicate: pred,
                },
            ));
        }
        Ok(shape::SetQuery {
            first,
            links: parsed,
        })
    }
}

impl qualify::State {
    pub(crate) fn peel<'a>(&mut self, expr: &'a syn::Expr) -> syn::Result<&'a syn::Expr> {
        enum Method {
            Rows,
            Order(qualify::Dir),
            Limit,
            Offset,
            Distinct,
            DistinctOn,
            Lock(lock::Lock),
        }

        let mut parsed = Self::default();
        let mut current = expr.unbrace();
        while let syn::Expr::MethodCall(mc) = current {
            let method = if mc.method == "rows" {
                Method::Rows
            } else if mc.method == "order_by" {
                Method::Order(qualify::Dir::Asc)
            } else if mc.method == "order_by_desc" {
                Method::Order(qualify::Dir::Desc)
            } else if mc.method == "limit" {
                Method::Limit
            } else if mc.method == "offset" {
                Method::Offset
            } else if mc.method == "distinct" {
                Method::Distinct
            } else if mc.method == "distinct_on" {
                Method::DistinctOn
            } else if mc.method == "for_update" {
                Method::Lock(lock::Lock::ForUpdate(mc.method.span()))
            } else if mc.method == "for_share" {
                Method::Lock(lock::Lock::ForShare(mc.method.span()))
            } else {
                break;
            };
            match method {
                Method::Rows => {
                    if !mc.args.is_empty() {
                        return Err(syn::Error::new(
                            mc.args.span(),
                            "`.rows()` takes no arguments",
                        ));
                    }
                }
                Method::Order(dir) => {
                    if mc.args.len() != 1 {
                        return Err(syn::Error::new(
                            mc.args.span(),
                            format!("`.{}(...)` takes exactly one closure argument", mc.method),
                        ));
                    }
                    parsed.order_by.push(qualify::Order {
                        closure: mc.args[0].clone(),
                        dir,
                    });
                }
                Method::Limit => {
                    if mc.args.len() != 1 {
                        return Err(syn::Error::new(
                            mc.args.span(),
                            "`.limit(n)` takes exactly one int / fn-param argument",
                        ));
                    }
                    parsed.limit = parsed.limit.add(
                        &self.limit,
                        mc.args[0].clone(),
                        mc.method.span(),
                        "limit",
                    )?;
                }
                Method::Offset => {
                    if mc.args.len() != 1 {
                        return Err(syn::Error::new(
                            mc.args.span(),
                            "`.offset(n)` takes exactly one int / fn-param argument",
                        ));
                    }
                    parsed.offset = parsed.offset.add(
                        &self.offset,
                        mc.args[0].clone(),
                        mc.method.span(),
                        "offset",
                    )?;
                }
                Method::Distinct => {
                    if !mc.args.is_empty() {
                        return Err(syn::Error::new(
                            mc.args.span(),
                            "`.distinct()` takes no arguments",
                        ));
                    }
                    if !matches!(self.distinct, qualify::Distinct::None)
                        || !matches!(parsed.distinct, qualify::Distinct::None)
                    {
                        return Err(syn::Error::new(
                            mc.method.span(),
                            "duplicate `.distinct(...)`",
                        ));
                    }
                    parsed.distinct = qualify::Distinct::All;
                }
                Method::DistinctOn => {
                    if mc.args.len() != 1 {
                        return Err(syn::Error::new(
                            mc.args.span(),
                            "`.distinct_on(...)` takes one closure argument",
                        ));
                    }
                    if !matches!(self.distinct, qualify::Distinct::None)
                        || !matches!(parsed.distinct, qualify::Distinct::None)
                    {
                        return Err(syn::Error::new(
                            mc.method.span(),
                            "duplicate `.distinct(...)`",
                        ));
                    }
                    let (arg, body) = mc.args[0].closure_one("distinct_on")?;
                    parsed.distinct = qualify::Distinct::On(arg, body);
                }
                Method::Lock(lock) => {
                    if !mc.args.is_empty() {
                        return Err(syn::Error::new(
                            mc.args.span(),
                            format!("`.{}()` takes no arguments", mc.method),
                        ));
                    }
                    if !matches!(self.lock, lock::Lock::None)
                        || !matches!(parsed.lock, lock::Lock::None)
                    {
                        return Err(syn::Error::new(
                            mc.method.span(),
                            "duplicate row-lock qualifier",
                        ));
                    }
                    parsed.lock = lock;
                }
            }
            current = mc.receiver.as_ref();
        }
        parsed.order_by.reverse();
        self.order_by.extend(parsed.order_by);
        parsed.limit.merge(&mut self.limit);
        parsed.offset.merge(&mut self.offset);
        if !matches!(parsed.distinct, qualify::Distinct::None) {
            self.distinct = parsed.distinct;
        }
        if !matches!(parsed.lock, lock::Lock::None) {
            self.lock = parsed.lock;
        }
        Ok(current)
    }
}

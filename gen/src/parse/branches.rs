use crate::shape;
use crate::shape::{lock, qualify};
use std::collections;

use syn::spanned::Spanned as _;

use crate::ext::ExprExt as _;

impl<'a> shape::SetQuery<'a> {
    pub(super) fn parse(
        expr: &'a syn::Expr,
        cte_map: &collections::BTreeMap<String, syn::Path>,
    ) -> syn::Result<Self> {
        let mut links = Vec::new();
        let mut current = expr;
        while let syn::Expr::MethodCall(call) = current {
            let Some(op) = shape::SetOpKind::from_method(&call.method.to_string()) else {
                break;
            };
            crate::ext::inferred(call)?;
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

        let first = shape::Selection::parse(current, cte_map)?;
        let links = links
            .into_iter()
            .rev()
            .map(|(op, operand)| Ok((op, shape::Selection::parse(operand, cte_map)?)))
            .collect::<syn::Result<_>>()?;
        Ok(Self { first, links })
    }
}

impl<'a> qualify::State<'a> {
    pub(crate) fn peel(&mut self, expr: &'a syn::Expr) -> syn::Result<&'a syn::Expr> {
        enum Method {
            Order(qualify::Dir),
            Limit,
            Offset,
            Distinct,
            DistinctOn,
            Lock(lock::Lock),
        }

        let order_start = self.order_by.len();
        let mut current = expr.unbrace();
        while let syn::Expr::MethodCall(mc) = current {
            let method = if mc.method == "order_by" {
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
            crate::ext::inferred(mc)?;
            match method {
                Method::Order(dir) => {
                    if mc.args.len() != 1 {
                        return Err(syn::Error::new(
                            mc.args.span(),
                            format!("`.{}(...)` takes exactly one closure argument", mc.method),
                        ));
                    }
                    self.order_by.push(qualify::Order {
                        closure: &mc.args[0],
                        dir,
                    });
                }
                Method::Limit => set_bound(&mut self.limit, mc, "limit")?,
                Method::Offset => set_bound(&mut self.offset, mc, "offset")?,
                Method::Distinct => {
                    if !mc.args.is_empty() {
                        return Err(syn::Error::new(
                            mc.args.span(),
                            "`.distinct()` takes no arguments",
                        ));
                    }
                    if !matches!(self.distinct, qualify::Distinct::None) {
                        return Err(syn::Error::new(
                            mc.method.span(),
                            "duplicate `.distinct(...)`",
                        ));
                    }
                    self.distinct = qualify::Distinct::All;
                }
                Method::DistinctOn => {
                    if mc.args.len() != 1 {
                        return Err(syn::Error::new(
                            mc.args.span(),
                            "`.distinct_on(...)` takes one closure argument",
                        ));
                    }
                    if !matches!(self.distinct, qualify::Distinct::None) {
                        return Err(syn::Error::new(
                            mc.method.span(),
                            "duplicate `.distinct(...)`",
                        ));
                    }
                    let (arg, body) = mc.args[0].closure_one("distinct_on")?;
                    self.distinct = qualify::Distinct::On(arg, body);
                }
                Method::Lock(lock) => {
                    if !mc.args.is_empty() {
                        return Err(syn::Error::new(
                            mc.args.span(),
                            format!("`.{}()` takes no arguments", mc.method),
                        ));
                    }
                    if !matches!(self.lock, lock::Lock::None) {
                        return Err(syn::Error::new(
                            mc.method.span(),
                            "duplicate row-lock qualifier",
                        ));
                    }
                    self.lock = lock;
                }
            }
            current = mc.receiver.as_ref();
        }
        self.order_by[order_start..].reverse();
        Ok(current)
    }
}

fn set_bound<'a>(
    bound: &mut Option<&'a syn::Expr>,
    call: &'a syn::ExprMethodCall,
    name: &str,
) -> syn::Result<()> {
    if call.args.len() != 1 {
        return Err(syn::Error::new(
            call.args.span(),
            format!("`.{name}(n)` takes exactly one int / fn-param argument"),
        ));
    }
    if let Some(prior) = bound {
        let mut error = syn::Error::new(call.method.span(), format!("duplicate `.{name}(...)`"));
        error.combine(syn::Error::new(prior.span(), "first qualifier is here"));
        return Err(error);
    }
    *bound = Some(&call.args[0]);
    Ok(())
}

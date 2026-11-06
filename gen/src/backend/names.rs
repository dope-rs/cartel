use crate::ext::PathExt as _;
use std::collections::BTreeSet;
use syn::ext::IdentExt as _;
use syn::visit::{self, Visit as _};
use syn::visit_mut::{self, VisitMut as _};

/// Both SQL planning and the Rust proof consume this same lowered block.
pub(super) fn lower(block: &syn::Block, params: &[syn::Ident]) -> syn::Result<syn::Block> {
    let mut names = Names(params.iter().map(crate::ext::name).collect());
    names.visit_block(block);
    let mut block = block.clone();
    for stmt in &mut block.stmts {
        let expr = match stmt {
            syn::Stmt::Expr(expr, _) => expr,
            syn::Stmt::Local(local) => match &mut local.init {
                Some(init) => &mut init.expr,
                None => continue,
            },
            _ => continue,
        };
        if let Some(chain) = Chain::parse(expr)? {
            for argument in chain.arguments {
                if matches!(argument, syn::Expr::Closure(_)) {
                    continue;
                }
                let row = names.fresh("row");
                Fields {
                    table: &chain.table,
                    row: &row,
                }
                .visit_expr_mut(argument);
                *argument = syn::parse_quote!(|#row| #argument);
            }
        }
    }
    Ok(block)
}

/// A complete chain whose row arguments all have one known table owner.
/// Joins, CTE references, sets, bulk input and windows retain explicit row bindings.
struct Chain<'a> {
    table: syn::Path,
    arguments: Vec<&'a mut syn::Expr>,
}

impl<'a> Chain<'a> {
    fn parse(expr: &'a mut syn::Expr) -> syn::Result<Option<Self>> {
        match expr {
            syn::Expr::Call(call) => {
                let syn::Expr::Path(function) = call.func.as_ref() else {
                    return Ok(None);
                };
                if function.qself.is_some()
                    || !function.path.segments.last().is_some_and(|segment| {
                        matches!(segment.ident.to_string().as_str(), "filter" | "insert")
                    })
                    || call.args.len() != 1
                {
                    return Ok(None);
                }
                Ok(Some(Self {
                    table: function.path.without_last()?,
                    arguments: call.args.iter_mut().collect(),
                }))
            }
            syn::Expr::MethodCall(call) => {
                let row_arg = match call.method.to_string().as_str() {
                    "select" | "order_by" | "order_by_desc" | "update" | "sum" | "avg" | "min"
                    | "max" | "group_by" => true,
                    "one"
                    | "first"
                    | "all"
                    | "stream"
                    | "rows"
                    | "limit"
                    | "offset"
                    | "distinct"
                    | "for_update"
                    | "for_share"
                    | "count"
                    | "cte"
                    | "delete"
                    | "returning_one"
                    | "returning_first"
                    | "returning_all"
                    | "on_conflict_do_nothing" => false,
                    _ => return Ok(None),
                };
                let Some(mut chain) = Self::parse(&mut call.receiver)? else {
                    return Ok(None);
                };
                if row_arg && call.args.len() == 1 {
                    chain.arguments.extend(call.args.iter_mut());
                }
                Ok(Some(chain))
            }
            _ => Ok(None),
        }
    }
}

#[derive(Default)]
pub(crate) struct Names(BTreeSet<String>);

impl Names {
    pub(crate) fn fresh(&mut self, prefix: &str) -> syn::Ident {
        let mut name = format!("{prefix}{}", self.0.len());
        while !self.0.insert(name.clone()) {
            name.push('_');
        }
        syn::Ident::new(&name, proc_macro2::Span::mixed_site())
    }

    pub(crate) fn lifetime(&mut self, prefix: &str) -> syn::Lifetime {
        let ident = self.fresh(prefix);
        syn::Lifetime::new(&format!("'{ident}"), ident.span())
    }
}

impl<'ast> visit::Visit<'ast> for Names {
    fn visit_ident(&mut self, ident: &'ast syn::Ident) {
        self.0.insert(crate::ext::name(ident));
    }
}

struct Fields<'a> {
    table: &'a syn::Path,
    row: &'a syn::Ident,
}

impl visit_mut::VisitMut for Fields<'_> {
    fn visit_expr_mut(&mut self, expr: &mut syn::Expr) {
        if matches!(expr, syn::Expr::Closure(_)) {
            return;
        }
        if let syn::Expr::Path(path) = expr
            && path.qself.is_none()
            && path.path.leading_colon.is_some() == self.table.leading_colon.is_some()
            && path.path.segments.len() == self.table.segments.len() + 1
            && path
                .path
                .segments
                .iter()
                .zip(&self.table.segments)
                .all(|(field, table)| {
                    field.ident.unraw() == table.ident.unraw() && field.arguments == table.arguments
                })
            && let Some(field) = path.path.segments.last()
            && field.arguments.is_empty()
        {
            let row = self.row;
            *expr = syn::Expr::Field(syn::ExprField {
                attrs: path.attrs.clone(),
                base: Box::new(syn::parse_quote!(#row)),
                dot_token: Default::default(),
                member: syn::Member::Named(field.ident.clone()),
            });
            return;
        }
        visit_mut::visit_expr_mut(self, expr);
    }

    /// A call's callee is not a row field; visit only its arguments.
    fn visit_expr_call_mut(&mut self, call: &mut syn::ExprCall) {
        for argument in &mut call.args {
            self.visit_expr_mut(argument);
        }
    }
}

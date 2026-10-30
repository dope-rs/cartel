use crate::shape;

use syn::spanned::Spanned as _;

pub(crate) trait Binding: Sized {
    fn extract_from(block: &syn::Block) -> syn::Result<(Vec<Self>, syn::Expr)>;
}

impl Binding for shape::CteBinding {
    fn extract_from(block: &syn::Block) -> syn::Result<(Vec<Self>, syn::Expr)> {
        let stmts = &block.stmts;
        let Some((body, bindings)) = stmts.split_last() else {
            return Err(syn::Error::new(block.span(), "#[query] body is empty"));
        };
        let mut ctes = Vec::new();
        for stmt in bindings {
            let syn::Stmt::Local(local) = stmt else {
                return Err(syn::Error::new(
                    stmt.span(),
                    "#[query] body may only contain `let X = <chain>.cte();` bindings before the main query",
                ));
            };
            let syn::Pat::Ident(pi) = &local.pat else {
                return Err(syn::Error::new(
                    local.pat.span(),
                    "CTE let-binding pattern must be a plain identifier",
                ));
            };
            let init = local.init.as_ref().ok_or_else(|| {
                syn::Error::new(local.span(), "CTE let-binding requires `= <chain>.cte();`")
            })?;
            let rhs = init.expr.as_ref();
            let syn::Expr::MethodCall(mc) = rhs else {
                return Err(syn::Error::new(
                    rhs.span(),
                    "CTE let-binding rhs must end in `.cte()`",
                ));
            };
            if mc.method != "cte" || !mc.args.is_empty() {
                return Err(syn::Error::new(
                    mc.method.span(),
                    "CTE let-binding rhs must end in `.cte()` (no arguments)",
                ));
            }
            ctes.push(Self {
                name: pi.ident.clone(),
                inner_chain: (*mc.receiver).clone(),
            });
        }
        match body {
            syn::Stmt::Expr(expr, semi) => match semi {
                None => Ok((ctes, expr.clone())),
                Some(semi) => Err(syn::Error::new(
                    semi.span,
                    "#[query] body's main query expression must not have a trailing semicolon",
                )),
            },
            stmt => Err(syn::Error::new(
                stmt.span(),
                "#[query] body's last statement must be the main query expression",
            )),
        }
    }
}

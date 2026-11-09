use crate::backend;
use crate::pred;
use crate::pred::sql;
use std::collections;
use syn::spanned::Spanned as _;

use crate::ext::ExprExt as _;

pub(crate) struct Assign {
    pub(crate) column: String,
    pub(crate) value: sql::Sql,
}

impl Assign {
    pub(crate) fn collect(
        body: &syn::Expr,
        row_var: &syn::Ident,
        scope: &pred::RowScope<'_>,
        params: &[backend::Param],
        dialect: &backend::Dialect,
        cte_map: &collections::BTreeMap<String, syn::Path>,
    ) -> syn::Result<Vec<Self>> {
        match body {
            syn::Expr::Block(block) => {
                if let Some(attr) = block.attrs.first() {
                    return Err(syn::Error::new(
                        attr.span(),
                        "attributes are not supported on an assignment block",
                    ));
                }
                if let Some(label) = &block.label {
                    return Err(syn::Error::new(
                        label.span(),
                        "labels are not supported on an assignment block",
                    ));
                }
                let mut assigns = Vec::with_capacity(block.block.stmts.len());
                for stmt in &block.block.stmts {
                    let syn::Stmt::Expr(expr, semi) = stmt else {
                        return Err(syn::Error::new(
                            stmt.span(),
                            "assign block may only contain `t.col = expr;` statements",
                        ));
                    };
                    if semi.is_none() {
                        return Err(syn::Error::new(
                            expr.span(),
                            "an assignment block statement must end with `;`",
                        ));
                    }
                    assigns.push(Self::parse_single(
                        expr, row_var, scope, params, dialect, cte_map,
                    )?);
                }
                Ok(assigns)
            }
            single => Ok(vec![Self::parse_single(
                single, row_var, scope, params, dialect, cte_map,
            )?]),
        }
    }

    fn parse_single(
        expr: &syn::Expr,
        row_var: &syn::Ident,
        scope: &pred::RowScope<'_>,
        params: &[backend::Param],
        dialect: &backend::Dialect,
        cte_map: &collections::BTreeMap<String, syn::Path>,
    ) -> syn::Result<Self> {
        let syn::Expr::Assign(assign) = expr else {
            return Err(syn::Error::new(
                expr.span(),
                "expected `t.col = <expr>` assignment",
            ));
        };
        if let Some(attr) = assign.attrs.first() {
            return Err(syn::Error::new(
                attr.span(),
                "attributes are not supported on an assignment",
            ));
        }
        let left = &assign.left;
        let right = &assign.right;
        let column = left.column(row_var).ok_or_else(|| {
            syn::Error::new(
                left.span(),
                "left side of assignment must be `<row_var>.<column>`",
            )
        })?;
        let value = scope
            .parse(right, Some(params), dialect, cte_map)?
            .ok_or_else(|| syn::Error::new(
                right.span(),
                "right side of assignment must be a fn-parameter, literal, or arithmetic/function expression",
            ))?;
        Ok(Self { column, value })
    }
}

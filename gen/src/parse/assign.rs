use crate::emit::token::{PAREN_CLOSE, PAREN_OPEN, fragment};
use crate::pred;
use crate::pred::sql;
use syn::spanned::Spanned as _;

use crate::ext::ExprExt as _;

pub(crate) struct Assign {
    pub(crate) column: String,
    pub(crate) value: sql::Sql,
}

impl Assign {
    pub(crate) fn collect<const UPDATE: bool>(
        body: &syn::Expr,
        row_var: &syn::Ident,
        scope: &pred::RowScope<'_>,
        target: proc_macro2::TokenStream,
        state: &mut crate::plan::State<'_, '_>,
        empty: &'static str,
    ) -> syn::Result<Vec<Self>> {
        let context = state.context();
        let mut checks = proc_macro2::TokenStream::new();

        let values = match body {
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
                if block.block.stmts.is_empty() {
                    return Err(syn::Error::new(body.span(), empty));
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
                        expr,
                        row_var,
                        scope,
                        &context,
                        &mut checks,
                        false,
                    )?);
                }
                assigns
            }
            single => vec![Self::parse_single(
                single,
                row_var,
                scope,
                &context,
                &mut checks,
                UPDATE,
            )?],
        };
        let bindings = scope.bindings(Some(row_var));
        let witness = crate::backend::scope();
        state.checks.extend(quote::quote! {{
            #bindings
            let #row_var: &mut #target = #witness.value();
            #checks
        }});
        Ok(values)
    }

    fn parse_single(
        expr: &syn::Expr,
        row_var: &syn::Ident,
        scope: &pred::RowScope<'_>,
        context: &pred::WhereContext<'_>,
        checks: &mut proc_macro2::TokenStream,
        compound: bool,
    ) -> syn::Result<Self> {
        let (attrs, left, right, increment) = match expr {
            syn::Expr::Assign(a) => (&a.attrs, &a.left, &a.right, false),
            syn::Expr::Binary(a) if compound && matches!(a.op, syn::BinOp::AddAssign(_)) => {
                (&a.attrs, &a.left, &a.right, true)
            }
            _ => {
                return Err(syn::Error::new(
                    expr.span(),
                    "expected a row field assignment",
                ));
            }
        };
        if let Some(attr) = attrs.first() {
            return Err(syn::Error::new(
                attr.span(),
                "attributes are not supported on an assignment",
            ));
        }
        let column = left.column(row_var).ok_or_else(|| {
            syn::Error::new(
                left.span(),
                "left side of assignment must be `<row_var>.<column>`",
            )
        })?;
        let mut value = sql::Sql::new();
        if increment {
            value.push(fragment(PAREN_OPEN));
            let dest = scope
                .column_ref(left)
                .ok_or_else(|| syn::Error::new(left.span(), "unknown update column"))?;
            for token in dest.into_parts() {
                value.push(token);
            }
            value.push(quote::quote! { " + " });
        }
        let rhs = scope.parse_into(right, Some(context.params), context.dialect, context.cte_map, &mut value)?
            .ok_or_else(|| syn::Error::new(
                right.span(),
                "right side of assignment must be a fn-parameter, literal, or arithmetic/function expression",
            ))?;
        if increment {
            value.push(fragment(PAREN_CLOSE));
            checks.extend(quote::quote! { #left += #rhs; });
        } else {
            let rt = &context.dialect.rt_crate;
            let witness = crate::backend::scope();
            checks.extend(
                quote::quote! { #rt::sql::Assign::assign(#witness.target(&mut #left), #rhs); },
            );
        }
        Ok(Self {
            column: crate::ext::name(column),
            value,
        })
    }
}

use crate::ext::PatExt as _;
use crate::plan;
use crate::plan::read::qualify::Integer;
use crate::pred;
use syn::{ext::IdentExt as _, spanned::Spanned as _};

enum Function {
    Nullary(&'static str),
    Count,
    Unary(&'static str),
    Offset(&'static str),
}

fn head(
    call: &syn::ExprCall,
    scope: &pred::RowScope<'_>,
    state: &mut plan::State<'_, '_>,
    output: &mut Vec<proc_macro2::TokenStream>,
) -> syn::Result<()> {
    let expr = call.func.as_ref();
    let syn::Expr::Path(syn::ExprPath { attrs, qself, path }) = expr else {
        return Err(syn::Error::new(
            expr.span(),
            "window function must be a path ending in `row_number` / `rank` / `dense_rank` / `count` / `sum` / `avg` / `min` / `max` / `lag` / `lead`",
        ));
    };
    if let Some(attr) = attrs.first() {
        return Err(syn::Error::new(
            attr.span(),
            "window function path must not have outer attributes",
        ));
    }
    if let Some(qself) = qself {
        return Err(syn::Error::new(
            qself.span(),
            "window function path must not use qualified-self syntax",
        ));
    }
    let segment = path
        .segments
        .last()
        .ok_or_else(|| syn::Error::new(path.span(), "window function path is empty"))?;
    let name = segment.ident.to_string();
    let function = match name.as_str() {
        "row_number" => Function::Nullary("ROW_NUMBER_OVER"),
        "rank" => Function::Nullary("RANK_OVER"),
        "dense_rank" => Function::Nullary("DENSE_RANK_OVER"),
        "count" => Function::Count,
        "sum" => Function::Unary("SUM_OPEN"),
        "avg" => Function::Unary("AVG_OPEN"),
        "min" => Function::Unary("MIN_OPEN"),
        "max" => Function::Unary("MAX_OPEN"),
        "lag" => Function::Offset("lag("),
        "lead" => Function::Offset("lead("),
        other => {
            return Err(syn::Error::new(
                path.span(),
                format!(
                    "unsupported window function `{other}`; allowed: row_number, rank, dense_rank, count, sum, avg, min, max, lag, lead"
                ),
            ));
        }
    };
    let open = match function {
        Function::Nullary(keyword) => {
            if !call.args.is_empty() {
                return Err(syn::Error::new(
                    call.args.span(),
                    format!("`{name}()` takes no arguments"),
                ));
            }
            output.push(state.dialect.kw(keyword));
            return Ok(());
        }
        Function::Count if call.args.is_empty() => {
            output.push(state.dialect.kw("COUNT_STAR_OVER"));
            return Ok(());
        }
        Function::Count => {
            if call.args.len() != 1 {
                return Err(syn::Error::new(
                    call.args.span(),
                    "`count(...)` window takes 0 or 1 args",
                ));
            }
            state.dialect.kw("COUNT_OPEN")
        }
        Function::Unary(keyword) => {
            if call.args.len() != 1 {
                return Err(syn::Error::new(
                    call.args.span(),
                    format!("`{name}(...)` window takes one column / expression argument"),
                ));
            }
            state.dialect.kw(keyword)
        }
        Function::Offset(prefix) => {
            if call.args.len() != 2 {
                return Err(syn::Error::new(
                    call.args.span(),
                    format!("`{name}(col, offset)` window takes two arguments"),
                ));
            }
            quote::quote! { #prefix }
        }
    };
    let expr = &call.args[0];
    let sql = state.parse(scope, expr, || {
        let arg = if matches!(function, Function::Offset(_)) {
            "first arg"
        } else {
            "window arg"
        };
        syn::Error::new(
            expr.span(),
            format!("`{name}(...)` {arg} must be a column / expression"),
        )
    })?;
    output.push(open);
    state.render_into(sql, output);
    if matches!(function, Function::Offset(_)) {
        let offset = Integer::parse(&call.args[1], state.params)?;
        output.push(state.dialect.kw("COMMA"));
        output.push(offset.render(state, plan::Bound::Offset));
    }
    output.push(state.dialect.kw("OVER_OPEN"));
    Ok(())
}

enum Clause {
    Partition,
    Order,
    Descending,
    Unknown,
}

fn root(expr: &syn::Expr, root: &syn::Ident) -> syn::Result<()> {
    match expr {
        syn::Expr::Path(syn::ExprPath { attrs, qself, path }) => {
            if let Some(attr) = attrs.first() {
                return Err(syn::Error::new(
                    attr.span(),
                    "window-spec root identifier must not have outer attributes",
                ));
            }
            if let Some(qself) = qself {
                return Err(syn::Error::new(
                    qself.span(),
                    "window-spec root identifier must not use qualified-self syntax",
                ));
            }
            if path.segments.len() == 1 && path.segments[0].ident.unraw() == root.unraw() {
                return Ok(());
            }
            Err(syn::Error::new(
                path.span(),
                "window-spec chain must start at the closure parameter identifier",
            ))
        }
        other => Err(syn::Error::new(
            other.span(),
            "window-spec chain must start at the `w` ident and use `.partition_by` / `.order_by` / `.order_by_desc`",
        )),
    }
}

fn specification(
    mut expr: &syn::Expr,
    root_ident: &syn::Ident,
    scope: &pred::RowScope<'_>,
    state: &mut plan::State<'_, '_>,
    output: &mut Vec<proc_macro2::TokenStream>,
) -> syn::Result<()> {
    let mut calls = Vec::new();
    let mut last_partition = None;
    while let syn::Expr::MethodCall(call) = expr {
        let kind = match call.method.to_string().as_str() {
            "partition_by" => {
                last_partition.get_or_insert(calls.len());
                Clause::Partition
            }
            "order_by" => Clause::Order,
            "order_by_desc" => Clause::Descending,
            _ => Clause::Unknown,
        };
        calls.push((call, kind));
        expr = &call.receiver;
    }
    root(expr, root_ident)?;

    let mut partition = false;
    let mut order = false;
    let mut deferred = Vec::new();
    for (index, (call, kind)) in calls.into_iter().enumerate().rev() {
        if call.args.len() != 1 {
            return Err(syn::Error::new(
                call.args.span(),
                format!("`.{}(...)` takes one argument", call.method),
            ));
        }
        let dst = match kind {
            Clause::Partition => {
                let prefix = if partition { "COMMA" } else { "PARTITION_BY" };
                partition = true;
                output.push(state.dialect.kw(prefix));
                &mut *output
            }
            Clause::Order | Clause::Descending => {
                let dst = if last_partition.is_some_and(|last| index > last) {
                    &mut deferred
                } else {
                    &mut *output
                };
                if order {
                    dst.push(state.dialect.kw("COMMA"));
                } else {
                    if last_partition.is_some() {
                        dst.push(state.dialect.kw("SPACE"));
                    }
                    dst.push(state.dialect.kw("WIN_ORDER_BY"));
                    order = true;
                }
                dst
            }
            Clause::Unknown => {
                return Err(syn::Error::new(
                    call.method.span(),
                    format!(
                        "unsupported window-spec method `.{}`; allowed: partition_by / order_by / order_by_desc",
                        call.method
                    ),
                ));
            }
        };
        let expr = &call.args[0];
        let sql = state.parse(scope, expr, || {
            syn::Error::new(
                expr.span(),
                format!(
                    "`.{}(...)` arg must be a column / arithmetic / function expression",
                    call.method
                ),
            )
        })?;
        state.render_into(sql, dst);
        if matches!(kind, Clause::Descending) {
            dst.push(state.dialect.kw("DESC"));
        }
        if last_partition == Some(index) {
            output.append(&mut deferred);
        }
    }
    Ok(())
}

pub(in crate::plan) fn build(
    state: &mut plan::State<'_, '_>,
    call: &syn::ExprMethodCall,
    scope: &pred::RowScope<'_>,
    output: &mut Vec<proc_macro2::TokenStream>,
) -> syn::Result<()> {
    if call.args.len() != 1 {
        return Err(syn::Error::new(
            call.args.span(),
            "`.over(...)` takes exactly one closure argument",
        ));
    }
    let function = match call.receiver.as_ref() {
        syn::Expr::Call(function) => function,
        other => {
            return Err(syn::Error::new(
                other.span(),
                "window receiver must be a function call like `row_number()` / `rank()` / `count()` / `sum(col)` / etc.",
            ));
        }
    };
    head(function, scope, state, output)?;
    let syn::Expr::Closure(closure) = &call.args[0] else {
        return Err(syn::Error::new(
            call.args[0].span(),
            "`.over(...)` argument must be a closure `|w| w.partition_by(...).order_by(...)`",
        ));
    };
    if closure.inputs.len() != 1 {
        return Err(syn::Error::new(
            closure.inputs.span(),
            "`.over(...)` closure must take one parameter",
        ));
    }
    let root = closure.inputs[0].ident()?;
    specification(&closure.body, root, scope, state, output)?;
    output.push(state.dialect.kw("PAREN_CLOSE"));
    Ok(())
}

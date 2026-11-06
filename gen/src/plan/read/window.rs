use crate::plan;
use crate::pred::{self, sql};
use crate::shape::qualify;

use syn::{ext::IdentExt as _, spanned::Spanned as _};

use crate::ext::PatExt as _;
use crate::plan::read::qualify::Integer;

enum Function {
    Nullary(Nullary),
    Count,
    Unary(Unary),
    Offset(Offset),
}

enum Nullary {
    RowNumber,
    Rank,
    DenseRank,
}

enum Unary {
    Sum,
    Avg,
    Min,
    Max,
}

enum Offset {
    Lag,
    Lead,
}

enum Keyword {
    RowNumber,
    Rank,
    DenseRank,
    CountStar,
    Count,
    Sum,
    Avg,
    Min,
    Max,
}

enum Head {
    Keyword(Keyword),
    Expr(Keyword, sql::Sql),
    Offset(&'static str, sql::Sql, Integer),
}

struct Chain<'a>(Vec<&'a syn::ExprMethodCall>);

struct Part {
    sql: sql::Sql,
    kind: Kind,
}

enum Kind {
    Group,
    Order(qualify::Dir),
}

struct Spec(Vec<Part>);

impl Nullary {
    fn syntax(&self) -> (&str, Keyword) {
        match self {
            Self::RowNumber => ("row_number", Keyword::RowNumber),
            Self::Rank => ("rank", Keyword::Rank),
            Self::DenseRank => ("dense_rank", Keyword::DenseRank),
        }
    }
}

impl Unary {
    fn syntax(&self) -> (&str, Keyword) {
        match self {
            Self::Sum => ("sum", Keyword::Sum),
            Self::Avg => ("avg", Keyword::Avg),
            Self::Min => ("min", Keyword::Min),
            Self::Max => ("max", Keyword::Max),
        }
    }
}

impl Keyword {
    fn render(self, state: &plan::State<'_>) -> proc_macro2::TokenStream {
        state.dialect.kw(match self {
            Self::RowNumber => "ROW_NUMBER_OVER",
            Self::Rank => "RANK_OVER",
            Self::DenseRank => "DENSE_RANK_OVER",
            Self::CountStar => "COUNT_STAR_OVER",
            Self::Count => "COUNT_OPEN",
            Self::Sum => "SUM_OPEN",
            Self::Avg => "AVG_OPEN",
            Self::Min => "MIN_OPEN",
            Self::Max => "MAX_OPEN",
        })
    }
}

impl Offset {
    fn syntax(&self) -> (&'static str, &'static str) {
        match self {
            Self::Lag => ("lag", "lag("),
            Self::Lead => ("lead", "lead("),
        }
    }
}

impl Function {
    fn parse(expr: &syn::Expr) -> syn::Result<Self> {
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
        match segment.ident.to_string().as_str() {
            "row_number" => Ok(Self::Nullary(Nullary::RowNumber)),
            "rank" => Ok(Self::Nullary(Nullary::Rank)),
            "dense_rank" => Ok(Self::Nullary(Nullary::DenseRank)),
            "count" => Ok(Self::Count),
            "sum" => Ok(Self::Unary(Unary::Sum)),
            "avg" => Ok(Self::Unary(Unary::Avg)),
            "min" => Ok(Self::Unary(Unary::Min)),
            "max" => Ok(Self::Unary(Unary::Max)),
            "lag" => Ok(Self::Offset(Offset::Lag)),
            "lead" => Ok(Self::Offset(Offset::Lead)),
            other => Err(syn::Error::new(
                path.span(),
                format!(
                    "unsupported window function `{other}`; allowed: row_number, rank, dense_rank, count, sum, avg, min, max, lag, lead"
                ),
            )),
        }
    }
}

impl Head {
    fn new(
        call: &syn::ExprCall,
        scope: &pred::RowScope<'_>,
        state: &plan::State<'_>,
    ) -> syn::Result<Self> {
        match Function::parse(call.func.as_ref())? {
            Function::Nullary(function) => {
                let (name, keyword) = function.syntax();
                if !call.args.is_empty() {
                    return Err(syn::Error::new(
                        call.args.span(),
                        format!("`{name}()` takes no arguments"),
                    ));
                }
                Ok(Self::Keyword(keyword))
            }
            Function::Count if call.args.is_empty() => Ok(Self::Keyword(Keyword::CountStar)),
            Function::Count if call.args.len() == 1 => {
                let expr = &call.args[0];
                let sql = state.parse(
                    scope,
                    expr,
                    syn::Error::new(
                        expr.span(),
                        "`count(...)` window arg must be a column / expression",
                    ),
                )?;
                Ok(Self::Expr(Keyword::Count, sql))
            }
            Function::Count => Err(syn::Error::new(
                call.args.span(),
                "`count(...)` window takes 0 or 1 args",
            )),
            Function::Unary(function) => {
                let (name, keyword) = function.syntax();
                if call.args.len() != 1 {
                    return Err(syn::Error::new(
                        call.args.span(),
                        format!("`{name}(...)` window takes one column / expression argument"),
                    ));
                }
                let expr = &call.args[0];
                let sql = state.parse(
                    scope,
                    expr,
                    syn::Error::new(
                        expr.span(),
                        format!("`{name}(...)` window arg must be a column / expression"),
                    ),
                )?;
                Ok(Self::Expr(keyword, sql))
            }
            Function::Offset(function) => {
                let (name, open) = function.syntax();
                if call.args.len() != 2 {
                    return Err(syn::Error::new(
                        call.args.span(),
                        format!("`{name}(col, offset)` window takes two arguments"),
                    ));
                }
                let expr = &call.args[0];
                let sql = state.parse(
                    scope,
                    expr,
                    syn::Error::new(
                        expr.span(),
                        format!("`{name}(...)` first arg must be a column / expression"),
                    ),
                )?;
                let offset = Integer::parse(&call.args[1], state.fn_params)?;
                Ok(Self::Offset(open, sql, offset))
            }
        }
    }

    fn render(self, state: &mut plan::State<'_>) -> Vec<proc_macro2::TokenStream> {
        match self {
            Self::Keyword(keyword) => vec![keyword.render(state)],
            Self::Expr(keyword, sql) => {
                let mut parts = vec![keyword.render(state)];
                parts.extend(state.render(sql));
                parts.push(state.dialect.kw("OVER_OPEN"));
                parts
            }
            Self::Offset(open, sql, offset) => {
                let mut parts = vec![quote::quote! { #open }];
                parts.extend(state.render(sql));
                parts.push(state.dialect.kw("COMMA"));
                parts.push(offset.render(state, plan::Bound::Offset));
                parts.push(state.dialect.kw("OVER_OPEN"));
                parts
            }
        }
    }
}

impl<'a> Chain<'a> {
    fn new(expr: &'a syn::Expr, root: &syn::Ident) -> syn::Result<Self> {
        Ok(Self(Self::collect(expr, root)?))
    }

    fn collect(
        expr: &'a syn::Expr,
        root: &syn::Ident,
    ) -> syn::Result<Vec<&'a syn::ExprMethodCall>> {
        match expr {
            syn::Expr::MethodCall(call) => {
                let mut chain = Self::collect(call.receiver.as_ref(), root)?;
                chain.push(call);
                Ok(chain)
            }
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
                    return Ok(Vec::new());
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
}

impl Part {
    fn new(
        call: &syn::ExprMethodCall,
        scope: &pred::RowScope<'_>,
        state: &plan::State<'_>,
    ) -> syn::Result<Self> {
        if call.args.len() != 1 {
            return Err(syn::Error::new(
                call.args.span(),
                format!("`.{}(...)` takes one argument", call.method),
            ));
        }
        let kind = match call.method.to_string().as_str() {
            "partition_by" => Kind::Group,
            "order_by" => Kind::Order(qualify::Dir::Asc),
            "order_by_desc" => Kind::Order(qualify::Dir::Desc),
            other => {
                return Err(syn::Error::new(
                    call.method.span(),
                    format!(
                        "unsupported window-spec method `.{other}`; allowed: partition_by / order_by / order_by_desc"
                    ),
                ));
            }
        };
        let expr = &call.args[0];
        let sql = state.parse(
            scope,
            expr,
            syn::Error::new(
                expr.span(),
                format!(
                    "`.{}(...)` arg must be a column / arithmetic / function expression",
                    call.method,
                ),
            ),
        )?;
        Ok(Self { sql, kind })
    }
}

impl Spec {
    fn new(
        chain: Chain<'_>,
        scope: &pred::RowScope<'_>,
        state: &plan::State<'_>,
    ) -> syn::Result<Self> {
        let mut parts = Vec::new();
        for call in chain.0 {
            parts.push(Part::new(call, scope, state)?);
        }
        Ok(Self(parts))
    }

    fn render(self, state: &mut plan::State<'_>) -> Vec<proc_macro2::TokenStream> {
        let mut groups = Vec::new();
        let mut orders = Vec::new();
        for part in self.0 {
            let sql = state.render(part.sql);
            match part.kind {
                Kind::Group => groups.push(sql),
                Kind::Order(dir) => orders.push((sql, dir)),
            }
        }
        let mut output = Vec::new();
        if !groups.is_empty() {
            output.push(state.dialect.kw("PARTITION_BY"));
            for (index, parts) in groups.into_iter().enumerate() {
                if index > 0 {
                    output.push(state.dialect.kw("COMMA"));
                }
                output.extend(parts);
            }
        }
        if !orders.is_empty() {
            if !output.is_empty() {
                output.push(state.dialect.kw("SPACE"));
            }
            output.push(state.dialect.kw("WIN_ORDER_BY"));
            for (index, (parts, dir)) in orders.into_iter().enumerate() {
                if index > 0 {
                    output.push(state.dialect.kw("COMMA"));
                }
                output.extend(parts);
                if matches!(dir, qualify::Dir::Desc) {
                    output.push(state.dialect.kw("DESC"));
                }
            }
        }
        output
    }
}

pub(in crate::plan) fn build(
    state: &mut plan::State<'_>,
    call: &syn::ExprMethodCall,
    scope: &pred::RowScope<'_>,
) -> syn::Result<Vec<proc_macro2::TokenStream>> {
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
    let head = Head::new(function, scope, state)?;
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
    let chain = Chain::new(&closure.body, root)?;
    let spec = Spec::new(chain, scope, state)?;

    let mut value = head.render(state);
    value.extend(spec.render(state));
    value.push(state.dialect.kw("PAREN_CLOSE"));
    Ok(value)
}

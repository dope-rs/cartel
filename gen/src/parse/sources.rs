use crate::shape;
use std::collections;

use syn::spanned::Spanned as _;

use crate::ext::ExprExt as _;
use crate::ext::PathExt as _;
use shape::join;

impl shape::QuerySource {
    pub(crate) fn parse_chain(
        expr: &syn::Expr,
        cte_map: &collections::BTreeMap<String, syn::Path>,
    ) -> syn::Result<(Self, Vec<syn::Ident>, syn::Expr)> {
        let expr = expr.unbrace();

        if let syn::Expr::Path(source) = expr {
            if let Some(attr) = source.attrs.first() {
                return Err(syn::Error::new(
                    attr.span(),
                    "attributes are not supported on a query source path",
                ));
            }
            if let Some(qself) = &source.qself {
                return Err(syn::Error::new(
                    qself.span(),
                    "a query source must be an unqualified CTE name or Table path",
                ));
            }
            if let Some(id) = source.path.get_ident()
                && let Some(table) = cte_map.get(crate::ext::unraw(&id.to_string()))
            {
                let dummy = syn::Ident::new("__cte", id.span());
                let pred: syn::Expr = syn::parse_quote!(true);
                return Ok((
                    Self {
                        primary_path: table.clone(),
                        primary_alias: Some(id.clone()),
                        joins: Vec::new(),
                        alias_depth: 0,
                    },
                    vec![dummy],
                    pred,
                ));
            }
        }

        if let syn::Expr::Call(call) = expr {
            let (table, arg, pred) = Self::parse_call(call)?;
            let source = Self::new(table);
            return Ok((source, vec![arg.clone()], pred.clone()));
        }
        if let syn::Expr::MethodCall(mc) = expr
            && mc.method == "filter"
        {
            if mc.args.len() != 1 {
                return Err(syn::Error::new(
                    mc.args.span(),
                    "`.filter(...)` takes one closure argument",
                ));
            }
            if let syn::Expr::Path(source) = mc.receiver.as_ref() {
                if let Some(attr) = source.attrs.first() {
                    return Err(syn::Error::new(
                        attr.span(),
                        "attributes are not supported on a filtered query source",
                    ));
                }
                if let Some(qself) = &source.qself {
                    return Err(syn::Error::new(
                        qself.span(),
                        "a filtered query source must be an unqualified CTE name or Table path",
                    ));
                }
                if let Some(id) = source.path.get_ident()
                    && let Some(table) = cte_map.get(crate::ext::unraw(&id.to_string()))
                {
                    let (ids, body) = mc.args[0].closure_n(1, "filter")?;
                    return Ok((
                        Self {
                            primary_path: table.clone(),
                            primary_alias: Some(id.clone()),
                            joins: Vec::new(),
                            alias_depth: 0,
                        },
                        ids,
                        body,
                    ));
                }
            }
            let source = Self::parse_joins(mc.receiver.as_ref())?;
            let n = 1 + source.joins.len();
            let (ids, body) = mc.args[0].closure_n(n, "filter")?;
            return Ok((source, ids, body));
        }
        Err(syn::Error::new(
            expr.span(),
            "expected `Table::filter(|t| ...)`, `Table::join::<X>(|p,u| ...).filter(|p,u| ...)`, or a CTE name",
        ))
    }

    fn parse_joins(expr: &syn::Expr) -> syn::Result<Self> {
        let mut chain = Vec::new();
        let mut current = expr;
        loop {
            match current {
                syn::Expr::MethodCall(mc) => {
                    chain.push(mc);
                    current = mc.receiver.as_ref();
                }
                syn::Expr::Call(call) => {
                    let (primary, joined, kind, ids, cond) = join::Join::parse_call(call)?;
                    let mut joins = vec![join::Join {
                        kind,
                        path: joined,
                        on_idents: ids,
                        cond,
                    }];
                    chain.reverse();
                    for (count, mc) in (2..).zip(chain) {
                        let kind = join::Kind::from_method(&mc.method)?;
                        let joined = join::Join::parse_type(mc)?;
                        if mc.args.len() != 1 {
                            return Err(syn::Error::new(
                                mc.args.span(),
                                format!("`.{}(...)` takes one closure argument", mc.method),
                            ));
                        }
                        let arity = if kind.is_lateral() { count } else { count + 1 };
                        let (idents, body) = mc.args[0].closure_n(arity, &mc.method.to_string())?;
                        joins.push(join::Join {
                            kind,
                            path: joined,
                            on_idents: idents,
                            cond: body,
                        });
                    }
                    return Ok(Self {
                        primary_path: primary,
                        primary_alias: None,
                        joins,
                        alias_depth: 0,
                    });
                }
                other => {
                    return Err(syn::Error::new(
                        other.span(),
                        "join chain must start with `Table::join::<X>(...)` (or `left_join` / `right_join` / `full_join`)",
                    ));
                }
            }
        }
    }

    pub(crate) fn parse_call(
        call: &syn::ExprCall,
    ) -> syn::Result<(syn::Path, &syn::Ident, &syn::Expr)> {
        let syn::Expr::Path(function) = call.func.as_ref() else {
            return Err(syn::Error::new(
                call.func.span(),
                "expected path `Table::filter`",
            ));
        };
        if let Some(attr) = function.attrs.first() {
            return Err(syn::Error::new(
                attr.span(),
                "attributes are not supported on a `Table::filter` path",
            ));
        }
        if let Some(qself) = &function.qself {
            return Err(syn::Error::new(
                qself.span(),
                "`filter` must be called through an unqualified Table path",
            ));
        }
        let filter = &function.path;
        if filter
            .segments
            .last()
            .map(|s| s.ident.to_string())
            .as_deref()
            != Some("filter")
        {
            return Err(syn::Error::new(
                filter.span(),
                "expected `Table::filter` (last segment must be `filter`)",
            ));
        }
        let table = filter.without_last()?;
        if call.args.len() != 1 {
            return Err(syn::Error::new(
                call.args.span(),
                "`filter` takes exactly one closure argument",
            ));
        }
        let (arg, body) = call.args[0].closure_one("filter")?;
        Ok((table, arg, body))
    }
}

impl shape::Target {
    pub(super) fn parse_shape(
        expr: &syn::Expr,
        cte_map: &collections::BTreeMap<String, syn::Path>,
    ) -> syn::Result<(Self, shape::QueryShape)> {
        let syn::Expr::MethodCall(mc) = expr else {
            return Err(syn::Error::new(
                expr.span(),
                "`.do_nothing()` / `.do_update(...)` must follow `.on_conflict(|u| u.col)`",
            ));
        };
        if mc.method != "on_conflict" {
            return Err(syn::Error::new(
                mc.method.span(),
                "expected `.on_conflict(|u| u.col)` before `.do_nothing()` / `.do_update(...)`",
            ));
        }
        if mc.args.len() != 1 {
            return Err(syn::Error::new(
                mc.args.span(),
                "`.on_conflict(...)` takes one closure argument",
            ));
        }
        let (arg, body) = mc.args[0].closure_one("on_conflict")?;
        let cols = Self::parse_cols(arg, body)?;
        let inner = shape::QueryShape::parse(mc.receiver.as_ref(), cte_map)?;
        Ok((Self { cols }, inner))
    }

    fn parse_cols(arg: &syn::Ident, body: &syn::Expr) -> syn::Result<Vec<String>> {
        match body {
            syn::Expr::Tuple(t) => {
                let mut columns = Vec::with_capacity(t.elems.len());
                for expr in &t.elems {
                    let Some(column) = expr.column(arg) else {
                        return Err(syn::Error::new(
                            expr.span(),
                            "`.on_conflict` body element must be `<arg>.<col>`",
                        ));
                    };
                    columns.push(column);
                }
                Ok(columns)
            }
            single => match single.column(arg) {
                Some(column) => Ok(vec![column]),
                None => Err(syn::Error::new(
                    single.span(),
                    "`.on_conflict` body element must be `<arg>.<col>`",
                )),
            },
        }
    }
}

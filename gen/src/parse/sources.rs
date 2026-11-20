use crate::shape;
use std::collections;

use syn::spanned::Spanned as _;

use crate::ext::ExprExt as _;
use crate::ext::PathExt as _;
use shape::join;

impl shape::QuerySource<'_> {
    pub(crate) fn single_table(&self, method: &str) -> syn::Result<()> {
        if let Some(join) = self.joins.first() {
            return Err(syn::Error::new(
                join.path.span(),
                format!("{method} requires a single source table"),
            ));
        }
        Ok(())
    }

    fn cte(path: &syn::Path, tables: &collections::BTreeMap<String, syn::Path>) -> Option<Self> {
        let id = path.get_ident()?;
        let table = tables.get(crate::ext::unraw(&id.to_string()))?;
        Some(Self {
            primary_path: table.clone(),
            primary_alias: Some(id.clone()),
            joins: Vec::new(),
            alias_depth: 0,
        })
    }
}

impl<'a> shape::Selection<'a> {
    pub(crate) fn parse(
        expr: &'a syn::Expr,
        cte_map: &collections::BTreeMap<String, syn::Path>,
    ) -> syn::Result<Self> {
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
            if let Some(cte) = shape::QuerySource::cte(&source.path, cte_map) {
                let dummy = syn::Ident::new("__cte", source.path.span());
                return Ok(Self {
                    source: cte,
                    idents: vec![dummy],
                    predicate: None,
                });
            }
        }

        if let syn::Expr::Call(call) = expr {
            let (table, arg, pred) = shape::QuerySource::parse_call(call)?;
            return Ok(Self {
                source: shape::QuerySource::new(table),
                idents: vec![arg.clone()],
                predicate: Some(pred),
            });
        }
        if let syn::Expr::MethodCall(mc) = expr
            && mc.method == "filter"
        {
            crate::ext::inferred(mc)?;
            if mc.args.len() != 1 {
                return Err(syn::Error::new(
                    mc.args.span(),
                    "`.filter(...)` takes one closure argument",
                ));
            }
            let cte = if let syn::Expr::Path(source) = mc.receiver.as_ref() {
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
                shape::QuerySource::cte(&source.path, cte_map)
            } else {
                None
            };
            let source = match cte {
                Some(source) => source,
                None => shape::QuerySource::parse_joins(&mc.receiver)?,
            };
            let n = 1 + source.joins.len();
            let binding = mc.args[0].binding(Some(n), "filter")?;
            return Ok(Self {
                source,
                idents: binding.idents,
                predicate: Some(binding.body),
            });
        }
        Err(syn::Error::new(
            expr.span(),
            "expected `Table::filter(|t| ...)`, `Table::join::<X>(|p,u| ...).filter(|p,u| ...)`, or a CTE name",
        ))
    }
}

impl<'a> shape::QuerySource<'a> {
    fn parse_joins(expr: &'a syn::Expr) -> syn::Result<Self> {
        let mut chain = Vec::new();
        let mut current = expr;
        loop {
            match current {
                syn::Expr::MethodCall(mc) => {
                    chain.push(mc);
                    current = mc.receiver.as_ref();
                }
                syn::Expr::Call(call) => {
                    let (primary, join) = join::Join::parse_call(call)?;
                    let mut joins = vec![join];
                    chain.reverse();
                    for (count, mc) in (2..).zip(chain) {
                        if mc.method != "join" || count >= 4 {
                            return Err(syn::Error::new(
                                mc.method.span(),
                                "only inner joins through four tables can be chained",
                            ));
                        }
                        let joined = join::Join::parse_type(mc)?;
                        if mc.args.len() != 1 {
                            return Err(syn::Error::new(
                                mc.args.span(),
                                format!("`.{}(...)` takes one closure argument", mc.method),
                            ));
                        }
                        let arity = count + 1;
                        let on = mc.args[0].binding(Some(arity), &mc.method.to_string())?;
                        joins.push(join::Join {
                            kind: join::Kind::Inner,
                            path: joined,
                            on,
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
                        "join chain must start with `Table::join::<X>(...)` (or `left_join`)",
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
        let table = filter.table()?;
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

impl<'a> shape::Target<'a> {
    pub(super) fn parse_shape(
        expr: &'a syn::Expr,
        cte_map: &collections::BTreeMap<String, syn::Path>,
    ) -> syn::Result<(Self, shape::QueryShape<'a>)> {
        let syn::Expr::MethodCall(mc) = expr else {
            return Err(syn::Error::new(
                expr.span(),
                "`.do_nothing()` / `.do_update(...)` must follow `.on_conflict(|u| u.col)`",
            ));
        };
        crate::ext::inferred(mc)?;
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

    fn parse_cols(arg: &syn::Ident, body: &'a syn::Expr) -> syn::Result<Vec<&'a syn::Ident>> {
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

use crate::shape;
use shape::{mutation, qualify};
use std::collections;

use syn::spanned::Spanned as _;

use crate::ext::{ExprExt as _, PathExt as _};

pub(super) mod assign;
pub(super) mod branches;
pub(super) mod ctes;
pub(super) mod sources;

impl shape::QueryShape {
    pub(crate) fn parse(
        expr: &syn::Expr,
        cte_map: &collections::BTreeMap<String, syn::Path>,
    ) -> syn::Result<Self> {
        use mutation::{Delete, From, Input, Insert, Mutation, Query, Update, Values};

        if let syn::Expr::MethodCall(top) = expr {
            let method = top.method.to_string();

            if let Some(ret) = shape::Returning::from_method(&method) {
                if !top.args.is_empty() {
                    return Err(syn::Error::new(
                        top.args.span(),
                        format!("`.{method}()` takes no arguments"),
                    ));
                }
                return Self::parse(top.receiver.as_ref(), cte_map)?
                    .returning(ret, top.method.span());
            }
            if method == "on_conflict_do_nothing" {
                if !top.args.is_empty() {
                    return Err(syn::Error::new(
                        top.args.span(),
                        "`.on_conflict_do_nothing()` takes no arguments",
                    ));
                }
                return Self::parse(top.receiver.as_ref(), cte_map)?
                    .conflict(shape::Conflict::DoNothing, top.method.span());
            }
            if method == "do_nothing" {
                if !top.args.is_empty() {
                    return Err(syn::Error::new(
                        top.args.span(),
                        "`.do_nothing()` takes no arguments",
                    ));
                }
                let (target, inner) = shape::Target::parse_shape(top.receiver.as_ref(), cte_map)?;
                return inner.conflict(shape::Conflict::TargetDoNothing(target), top.method.span());
            }
            if method == "do_update" {
                if top.args.len() != 1 {
                    return Err(syn::Error::new(
                        top.args.span(),
                        "`.do_update(...)` takes exactly one closure argument",
                    ));
                }
                let (set_arg, set_body) = top.args[0].closure_one("do_update")?;
                let (target, inner) = shape::Target::parse_shape(top.receiver.as_ref(), cte_map)?;
                let upd = shape::Update {
                    set_arg: set_arg.clone(),
                    set_body: set_body.clone(),
                };
                return inner.conflict(
                    shape::Conflict::TargetDoUpdate(target, upd),
                    top.method.span(),
                );
            }
        }

        if let syn::Expr::Call(call) = expr
            && let syn::Expr::Path(function) = call.func.as_ref()
        {
            if let Some(attr) = call.attrs.first() {
                return Err(syn::Error::new(
                    attr.span(),
                    "attributes are not supported on an insert call",
                ));
            }
            if let Some(attr) = function.attrs.first() {
                return Err(syn::Error::new(
                    attr.span(),
                    "attributes are not supported on an insert path",
                ));
            }
            if let Some(qself) = &function.qself {
                return Err(syn::Error::new(
                    qself.span(),
                    "insert must be called through an unqualified Table path",
                ));
            }
            let path = &function.path;
            let args = &call.args;
            let last = path.segments.last().map(|s| s.ident.to_string());
            if matches!(last.as_deref(), Some("insert" | "insert_each")) {
                let each = last.as_deref() == Some("insert_each");
                let method = if each { "insert_each" } else { "insert" };
                if args.len() != 1 {
                    return Err(syn::Error::new(
                        args.span(),
                        format!("`{method}` takes exactly one closure argument"),
                    ));
                }
                let table = path.without_last()?;
                let (arg, body) = args[0].closure_one(method)?;
                let values = Values {
                    table,
                    arg: arg.clone(),
                    body: Box::new(body.clone()),
                };
                let input = if each {
                    Input::Each(values)
                } else {
                    Input::One(values)
                };
                return Ok(Self::Mutation(Query::new(Mutation::Insert(Insert {
                    input,
                    conflict: shape::Conflict::None,
                }))));
            }
            if last.as_deref() == Some("insert_from") {
                if args.len() != 2 {
                    return Err(syn::Error::new(
                        args.span(),
                        "`Table::insert_from(source, |t, src| { ... })` takes exactly two arguments",
                    ));
                }
                let table = path.without_last()?;
                let (source, ids, pred) = shape::QuerySource::parse_chain(&args[0], cte_map)?;
                let arity = 2 + source.joins.len();
                let (idents, body) = args[1].closure_n(arity, "insert_from")?;
                let target = idents[0].clone();
                let sources = idents[1..].to_vec();
                return Ok(Self::Mutation(Query::new(Mutation::Insert(Insert {
                    input: Input::From(Box::new(From {
                        table,
                        source,
                        pred_idents: ids,
                        predicate: Box::new(pred),
                        target,
                        source_idents: sources,
                        body: Box::new(body),
                    })),
                    conflict: shape::Conflict::None,
                }))));
            }
        }

        let syn::Expr::MethodCall(top) = expr else {
            return Err(syn::Error::new(
                expr.span(),
                "expected `Table::filter(|t| ...).<term>()`, `Table::insert(|t| ...)`, or join chain",
            ));
        };
        let method = top.method.to_string();

        if method == "update" {
            if top.args.len() != 1 {
                return Err(syn::Error::new(
                    top.args.span(),
                    "`.update(...)` takes exactly one closure argument",
                ));
            }
            if let Some(query) = Query::try_each(top.receiver.as_ref(), &top.args[0])? {
                return Ok(Self::Mutation(query));
            }
            let (source, ids, pred) =
                shape::QuerySource::parse_chain(top.receiver.as_ref(), cte_map)?;
            let n = 1 + source.joins.len();
            let (set_ids, set_body) = top.args[0].closure_n(n, "update")?;
            let set_arg = set_ids.into_iter().next().ok_or_else(|| {
                syn::Error::new(top.args[0].span(), "update closure needs a row parameter")
            })?;
            return Ok(Self::Mutation(Query::new(Mutation::Update(Update {
                source,
                pred_idents: ids,
                predicate: Box::new(pred),
                set_arg,
                body: Box::new(set_body),
            }))));
        }

        if method == "delete" {
            if !top.args.is_empty() {
                return Err(syn::Error::new(
                    top.args.span(),
                    "`.delete()` takes no arguments",
                ));
            }
            let (source, ids, pred) =
                shape::QuerySource::parse_chain(top.receiver.as_ref(), cte_map)?;
            return Ok(Self::Mutation(Query::new(Mutation::Delete(Delete {
                source,
                pred_idents: ids,
                predicate: Box::new(pred),
            }))));
        }

        if matches!(method.as_str(), "count" | "sum" | "avg" | "min" | "max") {
            let kind = shape::Aggregation::parse(top)?;
            let mut quals = qualify::State::default();
            let mut current = quals.peel(top.receiver.as_ref())?;
            let mut having: Option<shape::HavingClause> = None;
            if let syn::Expr::MethodCall(mc) = current
                && mc.method == "having"
            {
                if mc.args.len() != 1 {
                    return Err(syn::Error::new(
                        mc.args.span(),
                        "`.having(...)` takes one closure argument",
                    ));
                }
                let (idents, body) = mc.args[0].closure_args("having")?;
                let Some((agg_arg, row_args)) = idents.split_last() else {
                    return Err(syn::Error::new(
                        mc.args[0].span(),
                        "`.having(...)` closure must take at least 2 args (row vars + agg handle)",
                    ));
                };
                having = Some(shape::HavingClause {
                    row_args: row_args.to_vec(),
                    agg_arg: agg_arg.clone(),
                    pred: body,
                });
                current = mc.receiver.as_ref();
            }
            let mut group_by: Option<shape::AggCol> = None;
            if let syn::Expr::MethodCall(mc) = current
                && mc.method == "group_by"
            {
                if mc.args.len() != 1 {
                    return Err(syn::Error::new(
                        mc.args.span(),
                        "`.group_by(...)` takes one closure argument",
                    ));
                }
                let (args, body) = mc.args[0].closure_args("group_by")?;
                group_by = Some(shape::AggCol { args, body });
                current = mc.receiver.as_ref();
            }
            let (source, ids, pred) = shape::QuerySource::parse_chain(current, cte_map)?;
            let n = 1 + source.joins.len();
            if let Some(gb) = &group_by
                && gb.args.len() != n
            {
                return Err(syn::Error::new(
                    gb.body.span(),
                    format!(
                        "`.group_by(...)` closure must take {n} parameter(s) to match the source"
                    ),
                ));
            }
            if let Some(h) = &having
                && h.row_args.len() != n
            {
                return Err(syn::Error::new(
                    h.pred.span(),
                    format!(
                        "`.having(...)` closure must take {} parameter(s) — {n} row var(s) + 1 agg handle",
                        n + 1
                    ),
                ));
            }
            if let Some((_, column)) = kind.function()
                && column.args.len() != n
            {
                return Err(syn::Error::new(
                    column.body.span(),
                    format!(
                        "aggregate column closure must take {n} parameter(s) to match the source"
                    ),
                ));
            }
            return Ok(Self::Aggregate(Box::new(shape::Aggregate {
                source,
                pred_idents: ids,
                predicate: pred,
                kind,
                group_by,
                having,
                qualifiers: quals,
            })));
        }

        let term = match method.as_str() {
            "one" => shape::Terminator::One,
            "first" => shape::Terminator::First,
            "all" => shape::Terminator::All,
            "stream" => shape::Terminator::Stream,
            other => {
                return Err(syn::Error::new(
                    top.method.span(),
                    format!(
                        "unknown method `.{other}()`; expected one/first/all/stream/count/update/delete"
                    ),
                ));
            }
        };
        if !top.args.is_empty() {
            return Err(syn::Error::new(
                top.args.span(),
                "terminator method must take no arguments",
            ));
        }
        let mut quals = qualify::State::default();
        let input = quals.peel(top.receiver.as_ref())?;

        if let syn::Expr::MethodCall(mc) = input
            && shape::SetOpKind::from_method(&mc.method.to_string()).is_some()
        {
            let set = shape::SelectBranch::parse_set(input, cte_map)?;
            return Ok(Self::SetOp {
                set,
                terminator: term,
                outer_qualifiers: quals,
            });
        }

        if let syn::Expr::MethodCall(mc) = input
            && mc.method == "select"
        {
            if mc.args.len() != 1 {
                return Err(syn::Error::new(
                    mc.args.span(),
                    "`.select(...)` takes one closure argument",
                ));
            }
            let peeled = quals.peel(mc.receiver.as_ref())?;
            let (source, ids, pred) = shape::QuerySource::parse_chain(peeled, cte_map)?;
            let n = 1 + source.joins.len();
            let (idents, body) = mc.args[0].closure_n(n, "select")?;
            let elems = match body {
                syn::Expr::Tuple(t) => t.elems.into_iter().collect(),
                single => vec![single],
            };
            return Ok(Self::Select {
                source,
                pred_idents: ids,
                predicate: pred,
                projection: Some(shape::Projection { idents, elems }),
                terminator: term,
                qualifiers: quals,
            });
        }

        let (source, ids, pred) = shape::QuerySource::parse_chain(input, cte_map)?;
        Ok(Self::Select {
            source,
            pred_idents: ids,
            predicate: pred,
            projection: None,
            terminator: term,
            qualifiers: quals,
        })
    }
}

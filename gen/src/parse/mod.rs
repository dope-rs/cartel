use crate::shape;
use shape::{mutation, qualify};
use std::collections;

use syn::spanned::Spanned as _;

use crate::ext::{ExprExt as _, PatExt as _, PathExt as _};
use syn::ext::IdentExt as _;

pub(super) mod assign;
pub(super) mod branches;
pub(super) mod ctes;
pub(super) mod sources;

impl<'a> shape::QueryShape<'a> {
    pub(crate) fn parse(
        expr: &'a syn::Expr,
        cte_map: &collections::BTreeMap<String, syn::Path>,
    ) -> syn::Result<Self> {
        use mutation::{From, Input, Insert, Mutation, Query, Update, Values};

        if let syn::Expr::MethodCall(top) = expr {
            let method = top.method.to_string();
            if method != "all" {
                crate::ext::inferred(top)?;
            }
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
                let upd = shape::Update { set_arg, set_body };
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
                let table = path.table()?;
                let (arg, body) = args[0].closure_one(method)?;
                let values = Values { table, arg, body };
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
                let table = path.table()?;
                let selection = shape::Selection::parse(&args[0], cte_map)?;
                selection.source.single_table("insert_from")?;
                let binding = args[1].closure(None, "insert_from")?;
                let mut inputs = binding.inputs.iter();
                let (Some(target), Some(source), None) =
                    (inputs.next(), inputs.next(), inputs.next())
                else {
                    return Err(syn::Error::new(
                        binding.inputs.span(),
                        "`insert_from` closure must take exactly 2 parameter(s)",
                    ));
                };
                let target = target.ident()?;
                let source = source.ident()?;
                if target.unraw() == source.unraw() {
                    return Err(syn::Error::new(source.span(), "duplicate row binding"));
                }
                return Ok(Self::Mutation(Query::new(Mutation::Insert(Insert {
                    input: Input::From(Box::new(From {
                        table,
                        selection,
                        target,
                        source,
                        body: binding.body.unbrace(),
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
            let selection = shape::Selection::parse(&top.receiver, cte_map)?;
            selection.source.single_table("update")?;
            let (set_arg, body) = top.args[0].closure_one("update")?;
            return Ok(Self::Mutation(Query::new(Mutation::Update(Update {
                selection,
                set_arg,
                body,
            }))));
        }

        if method == "delete" {
            if !top.args.is_empty() {
                return Err(syn::Error::new(
                    top.args.span(),
                    "`.delete()` takes no arguments",
                ));
            }
            let selection = shape::Selection::parse(&top.receiver, cte_map)?;
            selection.source.single_table("delete")?;
            return Ok(Self::Mutation(Query::new(Mutation::Delete(selection))));
        }

        if matches!(method.as_str(), "count" | "sum" | "avg" | "min" | "max") {
            let kind = shape::Aggregation::parse(top)?;
            let mut quals = qualify::State::default();
            let mut current = quals.peel(top.receiver.as_ref())?;
            let mut having = None;
            if let syn::Expr::MethodCall(mc) = current
                && mc.method == "having"
            {
                crate::ext::inferred(mc)?;
                if mc.args.len() != 1 {
                    return Err(syn::Error::new(
                        mc.args.span(),
                        "`.having(...)` takes one closure argument",
                    ));
                }
                let mut binding = mc.args[0].binding(None, "having")?;
                let Some(agg_arg) = binding.idents.pop() else {
                    return Err(syn::Error::new(
                        mc.args[0].span(),
                        "`.having(...)` closure must take at least 2 args (row vars + agg handle)",
                    ));
                };
                having = Some(shape::HavingClause {
                    row_args: binding.idents,
                    agg_arg,
                    pred: binding.body,
                });
                current = mc.receiver.as_ref();
            }
            let mut group_by = None;
            if let syn::Expr::MethodCall(mc) = current
                && mc.method == "group_by"
            {
                crate::ext::inferred(mc)?;
                if mc.args.len() != 1 {
                    return Err(syn::Error::new(
                        mc.args.span(),
                        "`.group_by(...)` takes one closure argument",
                    ));
                }
                group_by = Some(mc.args[0].binding(None, "group_by")?);
                current = mc.receiver.as_ref();
            }
            let selection = shape::Selection::parse(current, cte_map)?;
            let source = &selection.source;
            if group_by.is_some() {
                source.single_table("group_by")?;
                quals.ordered(top.method.span())?;
                if matches!(
                    kind.function(),
                    Some((shape::AggregateFn::Avg | shape::AggregateFn::Min, _))
                ) {
                    return Err(syn::Error::new(
                        top.method.span(),
                        "grouped aggregates support count, sum and max",
                    ));
                }
            }
            if !source.joins.is_empty() {
                quals.ordered(top.method.span())?;
                if source.joins.len() != 1 || kind.function().is_some() {
                    return Err(syn::Error::new(
                        top.method.span(),
                        "joined aggregates support count on two tables",
                    ));
                }
            }
            let n = 1 + source.joins.len();
            if let Some(gb) = &group_by
                && gb.idents.len() != n
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
                && column.idents.len() != n
            {
                return Err(syn::Error::new(
                    column.body.span(),
                    format!(
                        "aggregate column closure must take {n} parameter(s) to match the source"
                    ),
                ));
            }
            return Ok(Self::Aggregate(Box::new(shape::Aggregate {
                selection,
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
        let row = match &top.turbofish {
            None => None,
            Some(args) => match args.args.first() {
                Some(syn::GenericArgument::Type(row)) if args.args.len() == 1 => Some(row),
                _ => {
                    return Err(syn::Error::new_spanned(
                        args,
                        "joined all expects one row type",
                    ));
                }
            },
        };
        let receiver = top.receiver.unbrace();
        let mut quals = qualify::State::default();
        if let syn::Expr::MethodCall(mc) = receiver
            && mc.method == "select"
        {
            crate::ext::inferred(mc)?;
            if mc.args.len() != 1 {
                return Err(syn::Error::new(
                    mc.args.span(),
                    "`.select(...)` takes one closure argument",
                ));
            }
            if let Some(row) = row {
                return Err(syn::Error::new_spanned(
                    row,
                    "a projection fixes its result type",
                ));
            }
            let peeled = quals.peel(mc.receiver.as_ref())?;
            let selection = shape::Selection::parse(peeled, cte_map)?;
            let n = 1 + selection.source.joins.len();
            if n > 2 {
                return Err(syn::Error::new_spanned(
                    &mc.method,
                    "select supports up to two source rows",
                ));
            }
            let projection = mc.args[0].binding(Some(n), "select")?;
            return Ok(Self::Select {
                selection,
                projection: Some(projection),
                row: None,
                terminator: term,
                qualifiers: quals,
            });
        }

        let input = quals.peel(receiver)?;

        if let syn::Expr::MethodCall(mc) = input
            && shape::SetOpKind::from_method(&mc.method.to_string()).is_some()
        {
            if let Some(row) = row {
                return Err(syn::Error::new_spanned(row, "a set fixes its result type"));
            }
            let set = shape::SetQuery::parse(input, cte_map)?;
            return Ok(Self::SetOp {
                set,
                terminator: term,
                outer_qualifiers: quals,
            });
        }

        let selection = shape::Selection::parse(input, cte_map)?;
        if let Some(row) = row
            && selection.source.joins.is_empty()
        {
            return Err(syn::Error::new_spanned(
                row,
                "only joined all takes an explicit row type",
            ));
        }
        Ok(Self::Select {
            row,
            selection,
            projection: None,
            terminator: term,
            qualifiers: quals,
        })
    }
}

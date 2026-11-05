use crate::emit;
use crate::parse::assign;
use crate::plan;
use crate::pred;
use crate::shape;

use syn::spanned::Spanned as _;

use crate::ext::SqlIdent as _;

use crate::plan::write::reply::Op;
use crate::plan::write::{Conflict, Reply};

pub(in crate::plan) struct From<'a> {
    pub(in crate::plan) table: &'a syn::Path,
    pub(in crate::plan) source: Source<'a>,
    pub(in crate::plan) target: &'a syn::Ident,
    pub(in crate::plan) body: &'a syn::Expr,
    pub(in crate::plan) returning: shape::Returning,
    pub(in crate::plan) conflict: &'a shape::Conflict,
}

pub(in crate::plan) struct Source<'a> {
    pub(in crate::plan) query: &'a shape::QuerySource,
    pub(in crate::plan) pred_idents: &'a [syn::Ident],
    pub(in crate::plan) predicate: &'a syn::Expr,
    pub(in crate::plan) idents: &'a [syn::Ident],
}

pub(in crate::plan) struct Insert<'a>(pub(in crate::plan) plan::State<'a>);

#[derive(Clone, Copy)]
enum Kind {
    Single,
    Each,
}

impl<'a> Insert<'a> {
    pub(in crate::plan) fn build(
        self,
        table: &syn::Path,
        arg: &syn::Ident,
        body: &syn::Expr,
        ret: shape::Returning,
        conflict: &shape::Conflict,
    ) -> syn::Result<plan::Step<'a, emit::Plan>> {
        self.insert(table, arg, body, ret, conflict, Kind::Single)
    }

    pub(in crate::plan) fn each(
        self,
        table: &syn::Path,
        arg: &syn::Ident,
        body: &syn::Expr,
        ret: shape::Returning,
        conflict: &shape::Conflict,
    ) -> syn::Result<plan::Step<'a, emit::Plan>> {
        self.insert(table, arg, body, ret, conflict, Kind::Each)
    }

    fn insert(
        mut self,
        table: &syn::Path,
        arg: &syn::Ident,
        body: &syn::Expr,
        ret: shape::Returning,
        conflict: &shape::Conflict,
        kind: Kind,
    ) -> syn::Result<plan::Step<'a, emit::Plan>> {
        let step = Reply(self.0).validate(Op::Insert, ret, table)?;
        self.0 = step.state;
        let output = step.value;

        let assigns = assign::Assign::collect(
            body,
            arg,
            &pred::RowScope::single(arg.clone()),
            self.0.fn_params,
            self.0.param_tys,
            self.0.dialect,
            self.0.cte_map,
        )?;
        if assigns.is_empty() {
            let message = match kind {
                Kind::Single => "`Table::insert(...)` body must assign at least one column",
                Kind::Each => "`Table::insert_each(...)` body must assign at least one column",
            };
            return Err(syn::Error::new(body.span(), message));
        }
        let target = shape::QuerySource::new(table.clone());
        let action = Conflict::new(conflict, &target, &self.0)?;

        let mut cols_str = String::new();
        for (i, a) in assigns.iter().enumerate() {
            if i > 0 {
                cols_str.push(',');
            }
            cols_str.push_str(&a.column.quoted());
        }

        let mut sql = vec![self.0.dialect.kw("INSERT_INTO_KW")];
        sql.extend(target.render_base());
        sql.extend([
            self.0.dialect.kw("PAREN_OPEN_LEADING_SPACE"),
            quote::quote! { #cols_str },
            self.0.dialect.kw(match kind {
                Kind::Single => "VALUES_OPEN",
                Kind::Each => "UNNEST_OPEN",
            }),
        ]);
        for (index, assign) in assigns.into_iter().enumerate() {
            if index > 0 {
                sql.push(self.0.dialect.kw("COMMA_TIGHT"));
            }
            sql.extend(self.0.render(assign.value));
        }
        sql.push(self.0.dialect.kw("PAREN_CLOSE"));
        let step = action.render(self.0);
        self.0 = step.state;
        sql.extend(step.value);
        Ok(Reply(self.0).build(sql, table, target.qualifier(0), output))
    }

    pub(in crate::plan) fn from(
        mut self,
        input: From<'_>,
    ) -> syn::Result<plan::Step<'a, emit::Plan>> {
        let From {
            table,
            source,
            target,
            body,
            returning: ret,
            conflict,
        } = input;
        let Source {
            query,
            pred_idents: preds,
            predicate: pred,
            idents,
        } = source;
        let step = Reply(self.0).validate(Op::Insert, ret, table)?;
        self.0 = step.state;
        let output = step.value;

        let scope = pred::RowScope::for_source(query, idents)?;
        let selected = assign::Assign::collect(
            body,
            target,
            &scope,
            self.0.fn_params,
            self.0.param_tys,
            self.0.dialect,
            self.0.cte_map,
        )?;
        if selected.is_empty() {
            return Err(syn::Error::new(
                body.span(),
                "`Table::insert_from(...)` body must assign at least one column",
            ));
        }
        let scope = pred::RowScope::for_source(query, preds)?;
        let context = self.0.context();
        let where_ = pred::build(pred, &scope, false, &context)?;
        let mut ons = Vec::with_capacity(query.joins.len());
        for (index, join) in query.joins.iter().enumerate() {
            let scope = pred::RowScope::for_join_on(query, index)?;
            ons.push(pred::build(&join.cond, &scope, true, &context)?.join_tokens()?);
        }
        let target = shape::QuerySource::new(table.clone());
        let action = Conflict::new(conflict, &target, &self.0)?;

        let mut cols_str = String::new();
        let mut select: Vec<proc_macro2::TokenStream> = Vec::new();
        for (index, assign) in selected.into_iter().enumerate() {
            let rhs = self.0.render(assign.value);
            if index > 0 {
                cols_str.push(',');
                select.push(self.0.dialect.kw("COMMA"));
            }
            cols_str.push_str(&assign.column.quoted());
            select.extend(rhs);
        }
        let where_ = self.0.render(where_);

        let mut sql = vec![self.0.dialect.kw("INSERT_INTO_KW")];
        sql.extend(target.render_base());
        sql.extend([
            self.0.dialect.kw("PAREN_OPEN_LEADING_SPACE"),
            quote::quote! { #cols_str },
            self.0.dialect.kw("INSERT_FROM_SELECT"),
        ]);
        sql.extend(select);
        sql.push(self.0.dialect.kw("FROM_KW"));
        sql.extend(query.render_base());
        for (i, j) in query.joins.iter().enumerate() {
            sql.push(j.kind.sql_ref(self.0.dialect));
            sql.extend(query.render_join(i, j));
            sql.push(self.0.dialect.kw("ON_KW"));
            sql.extend(ons[i].iter().cloned());
        }
        sql.push(self.0.dialect.kw("WHERE_KW"));
        sql.extend(where_);
        let step = action.render(self.0);
        self.0 = step.state;
        sql.extend(step.value);
        Ok(Reply(self.0).build(sql, table, target.qualifier(0), output))
    }
}

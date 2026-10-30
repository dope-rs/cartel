use crate::backend;
use crate::pred;
use std::collections;
use syn::ext::IdentExt as _;

use pred::expr::Expr as _;
use pred::sql::Sql;

mod cte;
mod query;
mod read;
mod row;
mod write;

pub(crate) use cte::Cte;
pub(crate) use query::Query;

#[derive(Clone, Copy)]
pub(super) enum Bound {
    Limit,
    Offset,
}

pub(super) struct State<'a> {
    pub(super) fn_params: &'a [syn::Ident],
    pub(super) param_tys: &'a [syn::Type],
    pub(super) captures: Vec<syn::Ident>,
    pub(super) nonnegative: Vec<(syn::Ident, Bound)>,
    pub(super) return_type: &'a syn::ReturnType,
    pub(super) dialect: &'a backend::Dialect,
    pub(super) cte_map: &'a collections::HashMap<String, syn::Path>,
}

pub(super) struct Step<'a, T> {
    pub(super) state: State<'a>,
    pub(super) value: T,
}

impl State<'_> {
    pub(super) fn context(&self) -> pred::WhereContext<'_> {
        pred::WhereContext {
            fn_params: self.fn_params,
            param_tys: self.param_tys,
            dialect: self.dialect,
            cte_map: self.cte_map,
        }
    }

    pub(super) fn intern(&mut self, ident: syn::Ident) -> usize {
        if let Some(index) = self
            .captures
            .iter()
            .position(|current| current.unraw() == ident.unraw())
        {
            return index;
        }
        let index = self.captures.len();
        self.captures.push(ident);
        index
    }

    pub(super) fn mark_nonneg(&mut self, ident: syn::Ident, bound: Bound) {
        if self
            .nonnegative
            .iter()
            .all(|current| current.0.unraw() != ident.unraw())
        {
            self.nonnegative.push((ident, bound));
        }
    }

    pub(super) fn parse(
        &self,
        scope: &pred::RowScope,
        expr: &syn::Expr,
        error: syn::Error,
    ) -> syn::Result<Sql> {
        let sql = scope.parse(
            expr,
            Some(self.fn_params),
            self.param_tys,
            self.dialect,
            self.cte_map,
        )?;
        sql.required(error)
    }

    pub(super) fn render(&mut self, sql: Sql) -> Vec<proc_macro2::TokenStream> {
        sql.render(
            &mut self.captures,
            self.fn_params,
            self.param_tys,
            self.dialect,
        )
    }
}

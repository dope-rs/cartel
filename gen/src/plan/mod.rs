use crate::backend;
use crate::pred;
use std::collections;

use pred::sql::Sql;

pub(super) mod cte;
pub(super) mod query;
mod read;
mod row;
mod write;

#[derive(Clone, Copy)]
pub(super) enum Bound {
    Limit,
    Offset,
}

pub(super) struct State<'a, 'output> {
    pub(super) params: &'a [backend::Param],
    pub(super) captures: Vec<backend::Capture>,
    pub(super) nonnegative: Vec<(usize, Bound)>,
    pub(super) return_type: &'output syn::ReturnType,
    pub(super) dialect: &'a backend::Dialect,
    pub(super) cte_map: collections::BTreeMap<String, syn::Path>,
}

impl State<'_, '_> {
    pub(super) fn context(&self) -> pred::WhereContext<'_> {
        pred::WhereContext {
            params: self.params,
            dialect: self.dialect,
            cte_map: &self.cte_map,
        }
    }

    pub(super) fn mark_nonneg(&mut self, index: usize, bound: Bound) {
        if self.nonnegative.iter().all(|current| current.0 != index) {
            self.nonnegative.push((index, bound));
        }
    }

    pub(super) fn parse(
        &self,
        scope: &pred::RowScope<'_>,
        expr: &syn::Expr,
        error: impl FnOnce() -> syn::Error,
    ) -> syn::Result<Sql> {
        let sql = scope.parse(expr, Some(self.params), self.dialect, &self.cte_map)?;
        sql.ok_or_else(error)
    }

    pub(super) fn render(&mut self, sql: Sql) -> Vec<proc_macro2::TokenStream> {
        sql.render(&mut self.captures, self.params, self.dialect)
    }
}

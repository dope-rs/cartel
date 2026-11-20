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
    pub(crate) names: &'a mut backend::Names,
    pub(crate) checks: proc_macro2::TokenStream,
    pub(super) params: &'a [backend::Param],
    pub(super) captures: Vec<backend::Capture>,
    pub(super) nonnegative: Vec<(usize, Bound)>,
    pub(super) return_type: &'output syn::ReturnType,
    pub(super) dialect: &'a backend::Dialect,
    pub(super) cte_map: collections::BTreeMap<String, syn::Path>,
}

impl State<'_, '_> {
    pub(crate) fn context(&self) -> pred::WhereContext<'_> {
        pred::WhereContext {
            params: self.params,
            dialect: self.dialect,
            cte_map: &self.cte_map,
        }
    }

    pub(super) fn condition(&mut self, scope: &pred::RowScope<'_>, rust: proc_macro2::TokenStream) {
        let value = scope.bound(rust);
        self.checks.extend(quote::quote! { let _: bool = #value; });
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
    ) -> syn::Result<pred::sql::Operand> {
        let sql = scope.parse(expr, Some(self.params), self.dialect, &self.cte_map)?;
        sql.ok_or_else(error)
    }

    pub(super) fn render(&mut self, sql: Sql) -> Vec<proc_macro2::TokenStream> {
        let mut tokens = Vec::with_capacity(sql.len());
        self.render_into(sql, &mut tokens);
        tokens
    }

    pub(super) fn render_into(&mut self, sql: Sql, tokens: &mut Vec<proc_macro2::TokenStream>) {
        sql.render_into(&mut self.captures, self.params, self.dialect, tokens);
    }
}

use crate::backend;
use crate::pred;
use std::collections;
use syn::ext::IdentExt as _;

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

pub(super) struct State<'a> {
    pub(super) fn_params: &'a [syn::Ident],
    pub(super) param_tys: &'a [syn::Type],
    pub(super) captures: Vec<syn::Ident>,
    pub(super) nonnegative: Vec<(syn::Ident, Bound)>,
    pub(super) return_type: &'a syn::ReturnType,
    pub(super) dialect: &'a backend::Dialect,
    pub(super) cte_map: collections::BTreeMap<String, syn::Path>,
}

impl State<'_> {
    pub(super) fn context(&self) -> pred::WhereContext<'_> {
        pred::WhereContext {
            fn_params: self.fn_params,
            param_tys: self.param_tys,
            dialect: self.dialect,
            cte_map: &self.cte_map,
        }
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
        scope: &pred::RowScope<'_>,
        expr: &syn::Expr,
        error: syn::Error,
    ) -> syn::Result<Sql> {
        let sql = scope.parse(
            expr,
            Some(self.fn_params),
            self.param_tys,
            self.dialect,
            &self.cte_map,
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

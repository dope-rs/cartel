use crate::parse::{branches, sources};
use crate::plan;
use crate::pred::{self, layouts, var};
use crate::shape::{self, qualify};

use syn::spanned::Spanned as _;

use branches::Qualifier as _;
use layouts::ScopeLayout as _;
use shape::join;
use sources::Source as _;

use crate::plan::read::window::Window;
use crate::plan::read::{Filter, Qualify};

pub(in crate::plan) struct Project<'a>(pub(in crate::plan) plan::State<'a>);

impl<'a> Project<'a> {
    pub(in crate::plan) fn element(
        mut self,
        elem: &syn::Expr,
        scope: &pred::RowScope,
        lock: shape::lock::Lock,
    ) -> syn::Result<plan::Step<'a, Vec<proc_macro2::TokenStream>>> {
        if let syn::Expr::MethodCall(mc) = elem
            && mc.method == "over"
        {
            lock.reject("row locking is not supported with window functions")?;
            return Window(self.0).build(mc, scope);
        }
        let parsed = self
            .0
            .parse(
                scope,
                elem,
                syn::Error::new(
                    elem.span(),
                    "`.select(...)` element must be a column / arithmetic / function expression or a window expression `<fn>().over(|w| ...)`",
                ),
            )?;
        let value = self.0.render(parsed);
        Ok(plan::Step {
            state: self.0,
            value,
        })
    }

    pub(in crate::plan) fn lateral(
        mut self,
        j: &join::Join,
        index: usize,
        source: &shape::QuerySource,
    ) -> syn::Result<plan::Step<'a, Vec<proc_macro2::TokenStream>>> {
        let outer = pred::RowScope::for_outer(source, &j.on_idents, index)?;
        let mut quals = qualify::State::default();
        let body = quals.peel(&j.cond)?;
        let (inner, ids, pred) = shape::QuerySource::parse_chain(body, self.0.cte_map)?;
        let inner = inner.nested(Some(source.alias_depth));
        if !inner.joins.is_empty() {
            return Err(syn::Error::new(
                j.cond.span(),
                "joined source not yet supported as a lateral subquery body",
            ));
        }
        let table = inner.primary_path.clone();
        let scope = pred::RowScope::for_source(&inner, &ids)?.with_outer(outer);
        let qual = Qualify::new(&quals, None, &scope, &self.0)?;
        let step = Filter(self.0).build(&pred, &scope, true)?;
        self.0 = step.state;
        let filter = step.value;

        let columns = self
            .0
            .dialect
            .columns(&syn::parse_quote!(#table), inner.qualifier(0));
        let mut sql = vec![
            self.0.dialect.kw("SUBQUERY_OPEN"),
            columns,
            self.0.dialect.kw("FROM_KW"),
        ];
        sql.extend(inner.render_base());
        sql.push(self.0.dialect.kw("WHERE_KW"));
        sql.extend(filter);
        let step = qual.render(self.0);
        self.0 = step.state;
        sql.extend(step.value);
        sql.push(self.0.dialect.kw("PAREN_CLOSE_SPACE"));
        sql.push(source.qualifier(index + 1));
        sql.push(self.0.dialect.kw("ON_TRUE"));
        Ok(plan::Step {
            state: self.0,
            value: sql,
        })
    }

    pub(in crate::plan) fn distinct(
        mut self,
        quals: &qualify::State,
        scope: &pred::RowScope,
    ) -> syn::Result<plan::Step<'a, Vec<proc_macro2::TokenStream>>> {
        let value = match &quals.distinct {
            qualify::Distinct::None => Vec::new(),
            qualify::Distinct::All => vec![self.0.dialect.kw("DISTINCT_KW")],
            qualify::Distinct::On(arg, expr) => {
                let mut vars = Vec::with_capacity(scope.vars.len().max(1));
                if let Some(first) = scope.vars.first() {
                    vars.push(var::Var {
                        ident: arg.clone(),
                        table_const: first.table_const.clone(),
                    });
                } else {
                    vars.push(var::Var {
                        ident: arg.clone(),
                        table_const: None,
                    });
                }
                let local = pred::RowScope {
                    vars,
                    agg_var: None,
                    outer: None,
                    alias_depth: scope.alias_depth,
                    unnest_cols: Vec::new(),
                };
                let parsed = self
                    .0
                    .parse(
                        &local,
                        expr,
                        syn::Error::new(
                            expr.span(),
                            "`.distinct_on(...)` body must be a column / arithmetic / function expression",
                        ),
                    )?;
                let inner = self.0.render(parsed);
                let mut parts = vec![self.0.dialect.kw("DISTINCT_ON_OPEN")];
                parts.extend(inner);
                parts.push(self.0.dialect.kw("PAREN_CLOSE_SPACE"));
                parts
            }
        };
        Ok(plan::Step {
            state: self.0,
            value,
        })
    }
}

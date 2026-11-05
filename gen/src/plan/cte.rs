use crate::plan;
use crate::pred;
use crate::shape;
use crate::shape::qualify;

use crate::plan::read::{Filter, Project, Qualify, Select, join_parts};

pub(crate) struct Cte<'a>(pub(crate) plan::State<'a>);

impl<'a> Cte<'a> {
    pub(crate) fn build(
        mut self,
        inner: &syn::Expr,
    ) -> syn::Result<plan::Step<'a, (syn::Path, Vec<proc_macro2::TokenStream>)>> {
        let mut quals = qualify::State::default();
        let after = quals.peel(inner)?;
        let (source, ids, pred) = shape::QuerySource::parse_chain(after, self.0.cte_map)?;
        let path = &source.primary_path;
        let scope = pred::RowScope::for_source(&source, &ids)?;
        let tail = Qualify::new(&quals, None, &scope, &self.0)?;

        let (state, ons) = join_parts(self.0, &source)?;
        self.0 = state;

        let step = Filter(self.0).build(&pred, &scope, false)?;
        self.0 = step.state;
        let where_ = step.value;

        let cols = if source.joins.is_empty() {
            quote::quote! { <#path>::__CARTEL_SELECT_COLS }
        } else {
            self.0
                .dialect
                .columns(&syn::parse_quote!(#path), source.qualifier(0))
        };

        let mut sql = vec![self.0.dialect.kw("SELECT_KW")];
        let step = Project(self.0).distinct(&quals, &scope)?;
        self.0 = step.state;
        sql.extend(step.value);
        sql.push(cols);
        sql.extend(Select::from_clause(&source, &ons, self.0.dialect));
        sql.push(self.0.dialect.kw("WHERE_KW"));
        sql.extend(where_);
        let step = tail.render(self.0);
        self.0 = step.state;
        sql.extend(step.value);
        Ok(plan::Step {
            state: self.0,
            value: (source.primary_path, sql),
        })
    }
}

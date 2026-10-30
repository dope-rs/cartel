use crate::plan;
use crate::pred;

pub(in crate::plan) struct Filter<'a>(pub(in crate::plan) plan::State<'a>);

impl<'a> Filter<'a> {
    pub(in crate::plan) fn build(
        mut self,
        expr: &syn::Expr,
        scope: &pred::RowScope,
        columns: bool,
    ) -> syn::Result<plan::Step<'a, Vec<proc_macro2::TokenStream>>> {
        let context = self.0.context();
        let sql = pred::build(expr, scope, columns, &context)?;
        let value = self.0.render(sql);
        Ok(plan::Step {
            state: self.0,
            value,
        })
    }

    pub(in crate::plan) fn join(
        self,
        expr: &syn::Expr,
        scope: &pred::RowScope,
    ) -> syn::Result<plan::Step<'a, Vec<proc_macro2::TokenStream>>> {
        let context = self.0.context();
        let value = pred::build(expr, scope, true, &context)?.join_tokens()?;
        Ok(plan::Step {
            state: self.0,
            value,
        })
    }
}

use crate::plan;
use crate::pred;

pub(in crate::plan) fn build(
    state: &mut plan::State<'_, '_>,
    expr: &syn::Expr,
    scope: &pred::RowScope<'_>,
    columns: bool,
) -> syn::Result<Vec<proc_macro2::TokenStream>> {
    let context = state.context();
    let sql = pred::build(expr, scope, columns, &context)?;
    Ok(state.render(sql))
}

pub(in crate::plan) fn join(
    state: &plan::State<'_, '_>,
    expr: &syn::Expr,
    scope: &pred::RowScope<'_>,
) -> syn::Result<Vec<proc_macro2::TokenStream>> {
    let context = state.context();
    pred::build(expr, scope, true, &context)?.join_tokens()
}

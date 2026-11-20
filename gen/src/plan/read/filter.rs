use crate::plan;
use crate::pred;

pub(in crate::plan) fn build(
    state: &mut plan::State<'_, '_>,
    expr: Option<&syn::Expr>,
    scope: &pred::RowScope<'_>,
    columns: bool,
) -> syn::Result<Vec<proc_macro2::TokenStream>> {
    let context = state.context();
    let sql = pred::build(expr, scope, columns, &context)?;
    state.condition(scope, sql.rust);
    Ok(state.render(sql.sql))
}

pub(in crate::plan) fn join(
    state: &mut plan::State<'_, '_>,
    expr: &syn::Expr,
    scope: &pred::RowScope<'_>,
) -> syn::Result<Vec<proc_macro2::TokenStream>> {
    let context = state.context();
    let checked = pred::build(Some(expr), scope, true, &context)?;
    state.condition(scope, checked.rust);
    checked.sql.join_tokens()
}

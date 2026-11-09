use crate::plan;
use crate::pred;
use crate::shape;
use crate::shape::qualify;

use crate::plan::read::{Qualify, filter, join_parts, project, select};

pub(crate) fn build(
    state: &mut plan::State<'_, '_>,
    inner: &syn::Expr,
) -> syn::Result<(syn::Path, Vec<proc_macro2::TokenStream>)> {
    let mut quals = qualify::State::default();
    let after = quals.peel(inner)?;
    let (source, ids, pred) = shape::QuerySource::parse_chain(after, &state.cte_map)?;
    let path = &source.primary_path;
    let scope = pred::RowScope::for_source(&source, &ids)?;
    let tail = Qualify::new(&quals, None, &scope, state)?;

    let ons = join_parts(state, &source)?;

    let where_ = filter::build(state, &pred, &scope, false)?;

    let cols = if source.joins.is_empty() {
        quote::quote! { <#path>::__CARTEL_SELECT_COLS }
    } else {
        state
            .dialect
            .columns(&syn::parse_quote!(#path), source.qualifier(0))
    };

    let mut sql = vec![state.dialect.kw("SELECT_KW")];
    sql.extend(project::distinct(state, &quals, &scope)?);
    sql.push(cols);
    select::from_clause(&mut sql, &source, ons, state.dialect);
    sql.push(state.dialect.kw("WHERE_KW"));
    sql.extend(where_);
    sql.extend(tail.render(state));
    Ok((source.primary_path, sql))
}

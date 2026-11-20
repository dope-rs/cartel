use crate::emit::token::{FROM, SELECT, WHERE, fragment};
use crate::plan;
use crate::pred;
use crate::shape;
use crate::shape::qualify;

use crate::plan::read::{Qualify, filter, project};

pub(crate) fn build(
    state: &mut plan::State<'_, '_>,
    inner: &syn::Expr,
) -> syn::Result<(syn::Path, Vec<proc_macro2::TokenStream>)> {
    let mut quals = qualify::State::default();
    let after = quals.peel(inner)?;
    let shape::Selection {
        source,
        idents,
        predicate,
    } = shape::Selection::parse(after, &state.cte_map)?;
    source.single_table("cte")?;
    let path = &source.primary_path;
    let scope = pred::RowScope::for_source(&source, &idents)?;
    let tail = Qualify::new(&quals, None, &scope, state)?;

    let where_ = filter::build(state, predicate, &scope, false)?;

    let mut sql = vec![fragment(SELECT)];
    project::distinct(state, &quals, &scope, &mut sql)?;
    sql.push(quote::quote! { <#path>::__CARTEL_SELECT_COLS });
    sql.push(fragment(FROM));
    sql.extend(source.render_base());
    sql.push(fragment(WHERE));
    sql.extend(where_);
    tail.render_into(state, &mut sql);
    Ok((source.primary_path, sql))
}

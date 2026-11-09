pub(in crate::plan) mod agg;
pub(in crate::plan) mod filter;
pub(in crate::plan) mod project;
mod qualify;
pub(in crate::plan) mod select;
pub(in crate::plan) mod set;
pub(in crate::plan) mod window;

pub(in crate::plan) use qualify::Qualify;

pub(in crate::plan) fn join_parts(
    state: &mut super::State<'_, '_>,
    source: &crate::shape::QuerySource,
) -> syn::Result<Vec<Vec<proc_macro2::TokenStream>>> {
    let mut parts = Vec::with_capacity(source.joins.len());
    for (index, join) in source.joins.iter().enumerate() {
        let on = if join.kind.is_lateral() {
            project::lateral(state, join, index, source)?
        } else {
            let scope = crate::pred::RowScope::for_join_on(source, index)?;
            filter::join(state, &join.cond, &scope)?
        };
        parts.push(on);
    }
    Ok(parts)
}

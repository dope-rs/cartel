mod agg;
mod filter;
mod project;
mod qualify;
mod select;
mod set;
pub(in crate::plan) mod window;

use crate::pred::layouts::ScopeLayout as _;

pub(in crate::plan) use agg::Agg;
pub(in crate::plan) use filter::Filter;
pub(in crate::plan) use project::Project;
pub(in crate::plan) use qualify::Qualify;
pub(in crate::plan) use select::Select;
pub(in crate::plan) use set::Set;

pub(in crate::plan) fn join_parts<'a>(
    mut state: super::State<'a>,
    source: &crate::shape::QuerySource,
) -> syn::Result<(super::State<'a>, Vec<Vec<proc_macro2::TokenStream>>)> {
    let mut parts = Vec::with_capacity(source.joins.len());
    for (index, join) in source.joins.iter().enumerate() {
        let step = if join.kind.is_lateral() {
            Project(state).lateral(join, index, source)?
        } else {
            let scope = crate::pred::RowScope::for_join_on(source, index)?;
            Filter(state).join(&join.cond, &scope)?
        };
        state = step.state;
        parts.push(step.value);
    }
    Ok((state, parts))
}

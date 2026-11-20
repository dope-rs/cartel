use crate::emit;
use crate::plan;
use crate::shape;
use shape::mutation;

use crate::plan::read::{agg, select, set};
use crate::plan::write::{insert, update};

pub(crate) fn build<'a>(
    state: &mut plan::State<'_, 'a>,
    shape: &shape::QueryShape<'_>,
) -> syn::Result<emit::Plan<'a>> {
    use mutation::Mutation;

    match shape {
        shape::QueryShape::Select {
            selection,
            projection: proj,
            row,
            terminator: term,
            qualifiers: quals,
        } => select::build(state, selection, proj.as_ref(), *row, *term, quals),
        shape::QueryShape::Mutation(query) => match &query.kind {
            Mutation::Update(update) => update::build(state, update, query.returning),
            Mutation::Delete(selection) => update::delete(state, selection, query.returning),
            Mutation::Each(each) => update::each(state, each, query.returning),
            Mutation::Insert(insert) => insert::build(state, insert, query.returning),
        },
        shape::QueryShape::Aggregate(agg) => agg::build(state, agg),
        shape::QueryShape::SetOp {
            set,
            terminator: term,
            outer_qualifiers: quals,
        } => set::build(state, set, *term, quals),
    }
}

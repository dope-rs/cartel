use crate::emit;
use crate::plan;
use crate::shape;
use shape::mutation;

use crate::plan::read::{agg, select, set};
use crate::plan::write::{insert, update};

pub(crate) fn build(
    state: &mut plan::State<'_>,
    shape: &shape::QueryShape,
) -> syn::Result<emit::Plan> {
    use mutation::{Input, Mutation};

    match shape {
        shape::QueryShape::Select {
            source,
            pred_idents: preds,
            predicate: pred,
            projection: proj,
            terminator: term,
            qualifiers: quals,
        } => select::build(state, source, preds, pred, proj.as_ref(), *term, quals),
        shape::QueryShape::Mutation(query) => match &query.kind {
            Mutation::Update(update) => update::build(
                state,
                &update.source,
                &update.pred_idents,
                &update.predicate,
                &update.set_arg,
                &update.body,
                query.returning,
            ),
            Mutation::Delete(delete) => update::delete(
                state,
                &delete.source,
                &delete.pred_idents,
                &delete.predicate,
                query.returning,
            ),
            Mutation::Each(each) => update::each(state, each, query.returning),
            Mutation::Insert(insert) => match &insert.input {
                Input::One(values) => insert::build(
                    state,
                    &values.table,
                    &values.arg,
                    &values.body,
                    query.returning,
                    &insert.conflict,
                ),
                Input::Each(values) => insert::each(
                    state,
                    &values.table,
                    &values.arg,
                    &values.body,
                    query.returning,
                    &insert.conflict,
                ),
                Input::From(from) => insert::from(state, from, query.returning, &insert.conflict),
            },
        },
        shape::QueryShape::Aggregate(agg) => agg::build(state, agg),
        shape::QueryShape::SetOp {
            set,
            terminator: term,
            outer_qualifiers: quals,
        } => set::build(state, set, *term, quals),
    }
}

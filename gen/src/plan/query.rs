use crate::emit;
use crate::plan;
use crate::shape;
use shape::mutation;

use crate::plan::read::{Agg, Select, Set};
use crate::plan::write::{Each, From, Insert, Source, Update};

pub(crate) struct Query<'a>(pub(crate) plan::State<'a>);

impl<'a> Query<'a> {
    pub(crate) fn build(
        self,
        shape: &shape::QueryShape,
    ) -> syn::Result<plan::Step<'a, emit::Plan>> {
        use mutation::{Input, Mutation};

        match shape {
            shape::QueryShape::Select {
                source,
                pred_idents: preds,
                predicate: pred,
                projection: proj,
                terminator: term,
                qualifiers: quals,
            } => Select(self.0).build(source, preds, pred, proj.as_ref(), *term, quals),
            shape::QueryShape::Mutation(query) => match &query.kind {
                Mutation::Update(update) => Update(self.0).build(
                    &update.source,
                    &update.pred_idents,
                    &update.predicate,
                    &update.set_arg,
                    &update.body,
                    query.returning,
                ),
                Mutation::Delete(delete) => Update(self.0).delete(
                    &delete.source,
                    &delete.pred_idents,
                    &delete.predicate,
                    query.returning,
                ),
                Mutation::Each(each) => Update(self.0).each(Each {
                    source: &each.source,
                    data: &each.data,
                    row: &each.row,
                    cols: &each.cols,
                    predicate: &each.predicate,
                    body: &each.body,
                    returning: query.returning,
                }),
                Mutation::Insert(insert) => match &insert.input {
                    Input::One(values) => Insert(self.0).build(
                        &values.table,
                        &values.arg,
                        &values.body,
                        query.returning,
                        &insert.conflict,
                    ),
                    Input::Each(values) => Insert(self.0).each(
                        &values.table,
                        &values.arg,
                        &values.body,
                        query.returning,
                        &insert.conflict,
                    ),
                    Input::From(from) => Insert(self.0).from(From {
                        table: &from.table,
                        source: Source {
                            query: &from.source,
                            pred_idents: &from.pred_idents,
                            predicate: &from.predicate,
                            idents: &from.source_idents,
                        },
                        target: &from.target,
                        body: &from.body,
                        returning: query.returning,
                        conflict: &insert.conflict,
                    }),
                },
            },
            shape::QueryShape::Aggregate(agg) => Agg(self.0).build(agg),
            shape::QueryShape::SetOp {
                set,
                terminator: term,
                outer_qualifiers: quals,
            } => Set(self.0).build(set, *term, quals),
        }
    }
}

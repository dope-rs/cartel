use crate::emit;
use crate::plan;
use crate::pred;
use crate::shape;

use syn::spanned::Spanned as _;

use crate::plan::read::{Filter, Qualify, Select, join_parts};

pub(in crate::plan) struct Agg<'a>(pub(in crate::plan) plan::State<'a>);

impl<'a> Agg<'a> {
    pub(in crate::plan) fn build(
        mut self,
        agg: &shape::Aggregate,
    ) -> syn::Result<plan::Step<'a, emit::Plan>> {
        agg.qualifiers
            .lock
            .reject("row locking is not supported on aggregate queries")?;
        if !matches!(agg.qualifiers.distinct, shape::qualify::Distinct::None) {
            return Err(syn::Error::new(
                self.0.return_type.span(),
                "DISTINCT qualifiers are not supported on aggregate queries",
            ));
        }
        let explicit = match shape::ReturnShape::parse(self.0.return_type) {
            shape::ReturnShape::Default => {
                return Err(syn::Error::new(
                    self.0.return_type.span(),
                    "aggregate query needs an explicit return type (e.g. `-> i64` for `.count()`)",
                ));
            }
            shape::ReturnShape::Explicit(explicit) => explicit,
        };
        let (output, row, kind) = explicit.into_parts();
        let dispatch = match kind {
            shape::RowReturn::Plain => emit::Kind::One(row),
            shape::RowReturn::Optional => emit::Kind::First(row),
            shape::RowReturn::Many => emit::Kind::All(row),
            shape::RowReturn::Stream => {
                return Err(syn::Error::new(
                    row.span(),
                    "aggregate (.count/.sum/.avg/.min/.max) does not support `.stream()` — aggregate returns a single row",
                ));
            }
        };
        if let (Some(having), None) = (&agg.having, &agg.group_by) {
            return Err(syn::Error::new(
                having.pred.span(),
                "`.having(...)` requires a preceding `.group_by(...)`",
            ));
        }

        let source = &agg.source;
        let scope = pred::RowScope::for_source(source, &agg.pred_idents)?;
        let group = match &agg.group_by {
            Some(group) => {
                let scope = pred::RowScope::for_source(source, &group.args)?;
                Some(self.0.parse(
                    &scope,
                    &group.body,
                    syn::Error::new(
                        group.body.span(),
                        "`.group_by(...)` body must be a column / arithmetic / function expression",
                    ),
                )?)
            }
            None => None,
        };
        let value = match agg.kind.function() {
            Some((name, column)) => {
                let scope = pred::RowScope::for_source(source, &column.args)?;
                let sql = self.0.parse(
                    &scope,
                    &column.body,
                    syn::Error::new(
                        column.body.span(),
                        format!(
                            "`.{}(...)` body must be a column / arithmetic / function expression",
                            name.lower(),
                        ),
                    ),
                )?;
                Some((name, sql))
            }
            None => None,
        };
        let having = match &agg.having {
            Some(having) => {
                let scope = pred::RowScope::for_source(source, &having.row_args)?
                    .with_agg(having.agg_arg.clone());
                Some(pred::build(&having.pred, &scope, false, &self.0.context())?)
            }
            None => None,
        };
        let quals = Qualify::new(&agg.qualifiers, None, &scope, &self.0)?;
        let (state, joins) = join_parts(self.0, source)?;
        self.0 = state;

        let step = Filter(self.0).build(&agg.predicate, &scope, false)?;
        self.0 = step.state;
        let pred = step.value;

        let group = group.map(|sql| self.0.render(sql));
        let mut sql = vec![self.0.dialect.kw("SELECT_KW")];
        if let Some(parts) = &group {
            sql.extend(parts.iter().cloned());
            sql.push(self.0.dialect.kw("COMMA"));
        }
        match value {
            None => sql.push(self.0.dialect.kw("COUNT_STAR")),
            Some((name, value)) => {
                let prefix = format!("{}(", name.upper());
                sql.push(quote::quote! { #prefix });
                sql.extend(self.0.render(value));
                sql.push(self.0.dialect.kw("PAREN_CLOSE"));
            }
        }
        sql.extend(Select::from_clause(source, &joins, self.0.dialect));
        sql.push(self.0.dialect.kw("WHERE_KW"));
        sql.extend(pred);
        let columns = match group {
            None => 1u16,
            Some(parts) => {
                sql.push(self.0.dialect.kw("GROUP_BY_KW"));
                sql.extend(parts);
                2u16
            }
        };
        if let Some(having) = having {
            sql.push(self.0.dialect.kw("HAVING_KW"));
            sql.extend(self.0.render(having));
        }
        let step = quals.render(self.0);
        self.0 = step.state;
        sql.extend(step.value);

        let value = emit::Plan {
            output,
            sql_parts: sql,
            n_result_cols: quote::quote! { #columns },
            dispatch,
            probe_override: None,
        };
        Ok(plan::Step {
            state: self.0,
            value,
        })
    }
}

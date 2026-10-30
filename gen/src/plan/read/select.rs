use crate::backend;
use crate::emit;
use crate::plan;
use crate::plan::row;
use crate::pred;
use crate::pred::layouts;
use crate::shape;
use crate::shape::qualify;

use syn::spanned::Spanned as _;

use crate::ext::ExprExt as _;
use layouts::ScopeLayout as _;

use crate::plan::read::{Filter, Project, Qualify, join_parts};

pub(in crate::plan) struct Select<'a>(pub(in crate::plan) plan::State<'a>);

impl<'a> Select<'a> {
    pub(in crate::plan) fn build(
        mut self,
        source: &shape::QuerySource,
        preds: &[syn::Ident],
        pred: &syn::Expr,
        proj: Option<&shape::Projection>,
        term: shape::Terminator,
        quals: &qualify::State,
    ) -> syn::Result<plan::Step<'a, emit::Plan>> {
        let explicit = match shape::ReturnShape::parse(self.0.return_type) {
            shape::ReturnShape::Default => {
                return Err(syn::Error::new(
                    self.0.return_type.span(),
                    "SELECT queries require an explicit return type",
                ));
            }
            shape::ReturnShape::Explicit(explicit) => explicit,
        };
        let (output, row_ty, actual) = explicit.into_parts();
        let kind = actual.kind();
        if kind != term.expected() {
            return Err(syn::Error::new(
                row_ty.span(),
                format!(
                    "return type {} does not match terminator `.{}()`; expected {}",
                    kind.syntax(),
                    term.method(),
                    term.expected().syntax(),
                ),
            ));
        }

        let scope = pred::RowScope::for_source(source, preds)?;
        let qual = Qualify::new(quals, Some(term), &scope, &self.0)?;

        let (state, ons) = join_parts(self.0, source)?;
        self.0 = state;

        let skip = pred.is_true();
        let where_ = if skip {
            Vec::new()
        } else {
            let step = Filter(self.0).build(pred, &scope, false)?;
            self.0 = step.state;
            step.value
        };

        let step = qual.render(self.0);
        self.0 = step.state;
        let tail = step.value;

        let mut sql = vec![self.0.dialect.kw("SELECT_KW")];
        let step = Project(self.0).distinct(quals, &scope)?;
        self.0 = step.state;
        sql.extend(step.value);
        if let Some(proj) = proj {
            let scope = pred::RowScope::for_source(source, &proj.idents)?;
            for (i, elem) in proj.elems.iter().enumerate() {
                if i > 0 {
                    sql.push(self.0.dialect.kw("COMMA"));
                }
                let step = Project(self.0).element(elem, &scope, quals.lock)?;
                self.0 = step.state;
                sql.extend(step.value);
            }
        } else if source.joins.is_empty() {
            let table = &source.primary_path;
            sql.push(quote::quote! { <#table>::__CARTEL_SELECT_COLS });
        } else {
            sql.extend(row::View::new(&row_ty).select(source, self.0.dialect)?);
        }
        sql.extend(Self::from_clause(source, &ons, self.0.dialect));
        if !skip {
            sql.push(self.0.dialect.kw("WHERE_KW"));
            sql.extend(where_);
        }
        sql.extend(tail);

        let cols = match proj {
            Some(proj) => {
                let actual = proj.elems.len();
                if actual > i16::MAX as usize {
                    return Err(syn::Error::new(
                        row_ty.span(),
                        format!(
                            "PostgreSQL row has {actual} columns; the maximum is {}",
                            i16::MAX,
                        ),
                    ));
                }
                let n = actual as u16;
                quote::quote! { #n }
            }
            None => row::View::new(&row_ty).cols(),
        };

        let dispatch = match actual {
            shape::RowReturn::Plain => emit::Kind::One(row_ty),
            shape::RowReturn::Optional => emit::Kind::First(row_ty),
            shape::RowReturn::Many => emit::Kind::All(row_ty),
            shape::RowReturn::Stream => emit::Kind::Stream(row_ty),
        };

        let value = emit::Plan {
            output,
            sql_parts: sql,
            n_result_cols: cols,
            dispatch,
            probe_override: None,
        };
        Ok(plan::Step {
            state: self.0,
            value,
        })
    }

    pub(in crate::plan) fn from_clause(
        source: &shape::QuerySource,
        ons: &[Vec<proc_macro2::TokenStream>],
        dialect: &backend::Dialect,
    ) -> Vec<proc_macro2::TokenStream> {
        let mut parts = vec![dialect.kw("FROM_KW")];
        parts.extend(source.render_base());
        for (index, (j, on_parts)) in source.joins.iter().zip(ons.iter()).enumerate() {
            parts.push(j.kind.sql_ref(dialect));
            if j.kind.is_lateral() {
                parts.extend(on_parts.iter().cloned());
            } else {
                parts.extend(source.render_join(index, j));
                parts.push(dialect.kw("ON_KW"));
                parts.extend(on_parts.iter().cloned());
            }
        }
        parts
    }
}

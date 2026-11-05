use crate::pred;
use crate::pred::sql;
use crate::shape;

use syn::spanned::Spanned as _;

impl pred::RowScope {
    pub(super) fn exists(
        &self,
        input: &syn::Expr,
        negate: bool,
        context: &pred::WhereContext<'_>,
    ) -> syn::Result<sql::Sql> {
        let dialect = context.dialect;
        let (source, ids, body) = shape::QuerySource::parse_chain(input, context.cte_map)?;
        let source = source.nested(self.alias_depth);
        if !source.joins.is_empty() {
            return Err(syn::Error::new(
                input.span(),
                "joined source is not yet supported inside `exists(...)` / `not_exists(...)`",
            ));
        }
        let outer = self.clone();
        let scope = Self::for_source(&source, &ids)?.with_outer(outer);
        let filter = pred::build(&body, &scope, true, context)?;
        let head = if negate {
            dialect.kw("NOT_EXISTS_OPEN")
        } else {
            dialect.kw("EXISTS_OPEN")
        };
        let mut parts = vec![head];
        parts.extend(source.render_base());
        parts.push(dialect.kw("WHERE_KW"));
        let out = sql::Sql::tokens(parts)
            .append(filter)
            .push(dialect.kw("PAREN_CLOSE"));
        Ok(out)
    }

    pub(super) fn scalar_agg(
        &self,
        input: &syn::ExprCall,
        kind: &shape::Aggregation,
        context: &pred::WhereContext<'_>,
    ) -> syn::Result<sql::Sql> {
        let dialect = context.dialect;
        let (table, arg, body) = shape::QuerySource::parse_call(input)?;
        let source = shape::QuerySource::new(table).nested(self.alias_depth);
        let outer = self.clone();
        let scope = Self::for_source(&source, &[arg])?.with_outer(outer);
        let filter = pred::build(&body, &scope, true, context)?;

        let agg_sql = match kind.function() {
            Some((name, agg)) => {
                let scope = Self::for_source(&source, &agg.args)?.with_outer(self.clone());
                let parts = scope
                    .parse(
                        &agg.body,
                        Some(context.fn_params),
                        context.param_tys,
                        dialect,
                        context.cte_map,
                    )?
                    .required(syn::Error::new(
                        agg.body.span(),
                        format!(
                            "scalar subquery `.{}(...)` body must be a column / arithmetic / function expression",
                            name.lower(),
                        ),
                    ))?;
                let prefix = format!("{}(", name.upper());
                sql::Sql::new()
                    .push(quote::quote! { #prefix })
                    .append(parts)
                    .push(dialect.kw("PAREN_CLOSE"))
            }
            None => sql::Sql::tokens(vec![dialect.kw("COUNT_STAR")]),
        };
        let sql = sql::Sql::tokens(vec![dialect.kw("SUBQUERY_OPEN")])
            .append(agg_sql)
            .push(dialect.kw("FROM_KW"))
            .append(sql::Sql::tokens(source.render_base()))
            .push(dialect.kw("WHERE_KW"))
            .append(filter)
            .push(dialect.kw("PAREN_CLOSE"));
        Ok(sql)
    }
}

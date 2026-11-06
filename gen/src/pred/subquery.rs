use crate::pred;
use crate::pred::sql;
use crate::shape;

use syn::spanned::Spanned as _;

impl pred::RowScope<'_> {
    pub(super) fn exists(
        &self,
        input: &syn::Expr,
        negate: bool,
        context: &pred::WhereContext<'_>,
        sql: &mut sql::Sql,
    ) -> syn::Result<()> {
        let dialect = context.dialect;
        let (source, ids, body) = shape::QuerySource::parse_chain(input, context.cte_map)?;
        let source = source.nested(self.alias_depth);
        if !source.joins.is_empty() {
            return Err(syn::Error::new(
                input.span(),
                "joined source is not yet supported inside `exists(...)` / `not_exists(...)`",
            ));
        }
        let scope = pred::RowScope::for_source(&source, &ids)?.with_outer(self);
        let head = if negate {
            dialect.kw("NOT_EXISTS_OPEN")
        } else {
            dialect.kw("EXISTS_OPEN")
        };
        sql.push(head);
        sql.extend(source.render_base());
        sql.push(dialect.kw("WHERE_KW"));
        pred::build_into(&body, &scope, true, context, sql)?;
        sql.push(dialect.kw("PAREN_CLOSE"));
        Ok(())
    }

    pub(super) fn scalar_agg(
        &self,
        input: &syn::ExprCall,
        kind: pred::expr::Aggregate<'_>,
        context: &pred::WhereContext<'_>,
        sql: &mut sql::Sql,
    ) -> syn::Result<()> {
        let dialect = context.dialect;
        let (table, arg, body) = shape::QuerySource::parse_call(input)?;
        let source = shape::QuerySource::new(table).nested(self.alias_depth);
        let scope = pred::RowScope::for_primary(&source, arg).with_outer(self);
        sql.push(dialect.kw("SUBQUERY_OPEN"));
        // Parsing only appends deferred parts. Emit in SQL order while retaining
        // WHERE's diagnostic precedence; captures are interned later by render.
        let aggregate = (|| match kind {
            pred::expr::Aggregate::Column(name, arg, body) => {
                let scope = pred::RowScope::for_primary(&source, arg).with_outer(self);
                let prefix = format!("{}(", name.upper());
                sql.push(quote::quote! { #prefix });
                if !scope.parse_into(
                    body,
                    Some(context.fn_params),
                    context.param_tys,
                    dialect,
                    context.cte_map,
                    sql,
                )? {
                    return Err(syn::Error::new(
                        body.span(),
                        format!(
                            "scalar subquery `.{}(...)` body must be a column / arithmetic / function expression",
                            name.lower(),
                        ),
                    ));
                }
                sql.push(dialect.kw("PAREN_CLOSE"));
                Ok(())
            }
            pred::expr::Aggregate::Count => {
                sql.push(dialect.kw("COUNT_STAR"));
                Ok(())
            }
        })();
        sql.push(dialect.kw("FROM_KW"));
        sql.extend(source.render_base());
        sql.push(dialect.kw("WHERE_KW"));
        pred::build_into(body, &scope, true, context, sql)?;
        aggregate?;
        sql.push(dialect.kw("PAREN_CLOSE"));
        Ok(())
    }
}

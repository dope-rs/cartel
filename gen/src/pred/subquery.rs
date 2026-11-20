use crate::emit::token::{COUNT_STAR, FROM, PAREN_CLOSE, SUBQUERY_OPEN, WHERE, fragment};
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
    ) -> syn::Result<proc_macro2::TokenStream> {
        let shape::Selection {
            source,
            idents,
            predicate,
        } = shape::Selection::parse(input, context.cte_map)?;
        let source = source.nested(self.alias_depth);
        if !source.joins.is_empty() {
            return Err(syn::Error::new(
                input.span(),
                "joined source is not yet supported inside `exists(...)` / `not_exists(...)`",
            ));
        }
        let scope = pred::RowScope::for_source(&source, &idents)?.with_outer(self);
        let head = if negate {
            fragment("NOT EXISTS (SELECT 1 FROM ")
        } else {
            fragment("EXISTS (SELECT 1 FROM ")
        };
        sql.push(head);
        sql.extend(source.render_base());
        sql.push(fragment(WHERE));
        let predicate = pred::filter_into(predicate, &scope, true, context, sql)?;
        sql.push(fragment(PAREN_CLOSE));
        Ok(scope.local(quote::quote! { let _: bool = #predicate; true }))
    }

    pub(super) fn scalar_agg(
        &self,
        input: &syn::ExprCall,
        kind: pred::expr::Aggregate<'_>,
        context: &pred::WhereContext<'_>,
        sql: &mut sql::Sql,
    ) -> syn::Result<proc_macro2::TokenStream> {
        let dialect = context.dialect;
        let (table, arg, body) = shape::QuerySource::parse_call(input)?;
        let source = shape::QuerySource::new(table).nested(self.alias_depth);
        let scope = pred::RowScope::for_primary(&source, arg).with_outer(self);
        sql.push(fragment(SUBQUERY_OPEN));
        // Parsing only appends deferred parts. Emit in SQL order while retaining
        // WHERE's diagnostic precedence; captures are interned later by render.
        let aggregate = (|| match kind {
            pred::expr::Aggregate::Column(name, arg, body) => {
                let scope = pred::RowScope::for_primary(&source, arg).with_outer(self);
                let prefix = format!("{}(", name.upper());
                sql.push(quote::quote! { #prefix });
                let Some(value) =
                    scope.parse_into(body, Some(context.params), dialect, context.cte_map, sql)?
                else {
                    return Err(syn::Error::new(
                        body.span(),
                        format!(
                            "scalar subquery `.{}(...)` body must be a column / arithmetic / function expression",
                            name.lower(),
                        ),
                    ));
                };
                sql.push(fragment(PAREN_CLOSE));
                Ok(scope.local(quote::quote! { let _ = #value; ::core::option::Option::None }))
            }
            pred::expr::Aggregate::Count => {
                sql.push(fragment(COUNT_STAR));
                Ok(quote::quote!(0i64))
            }
        })();
        sql.push(fragment(FROM));
        sql.extend(source.render_base());
        sql.push(fragment(WHERE));
        let predicate = pred::build_into(body, &scope, true, context, sql)?;
        let checked = scope.local(quote::quote! { let _: bool = #predicate; });
        let value = aggregate?;
        sql.push(fragment(PAREN_CLOSE));
        Ok(quote::quote! {{ #checked; #value }})
    }
}

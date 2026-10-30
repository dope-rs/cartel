mod name;
mod param;

use crate::backend;
use syn::{spanned::Spanned as _, visit_mut};

enum Action {
    Keep,
    Scope,
    ScopeAs(syn::Ident),
    Rename(syn::Ident),
}

impl Action {
    fn scope(id: &syn::Ident, name: &str) -> Self {
        Self::ScopeAs(syn::Ident::new(name, id.span()))
    }

    fn rename(id: &syn::Ident, name: &str) -> Self {
        Self::Rename(syn::Ident::new(name, id.span()))
    }

    fn function(id: &syn::Ident) -> Self {
        if id == "exists" {
            Self::scope(id, "has_rows")
        } else if id == "not_exists" {
            Self::scope(id, "has_no_rows")
        } else if id == "row_number" {
            Self::scope(id, "row_number")
        } else if id == "count" {
            Self::scope(id, "count")
        } else if id == "lag" {
            Self::scope(id, "lag")
        } else if id == "lead" {
            Self::scope(id, "lead")
        } else if id == "to_tsvector" {
            Self::scope(id, "vector")
        } else if id == "to_tsquery" {
            Self::scope(id, "query")
        } else if id == "plainto_tsquery" {
            Self::scope(id, "plain_query")
        } else if id == "ts_rank" {
            Self::scope(id, "rank")
        } else if id == "array_length" {
            Self::scope(id, "array_len")
        } else if id == "cardinality" {
            Self::scope(id, "cardinality")
        } else {
            Self::Keep
        }
    }

    fn table(id: &syn::Ident) -> Self {
        if id == "lateral_left_join" {
            Self::scope(id, "lateral_left")
        } else if id == "filter"
            || id == "join"
            || id == "left_join"
            || id == "lateral_join"
            || id == "filter_each"
            || id == "insert"
            || id == "insert_each"
            || id == "insert_from"
        {
            Self::Scope
        } else {
            Self::Keep
        }
    }

    fn method(id: &syn::Ident) -> Self {
        if id == "fts_match" {
            Self::scope(id, "is_match")
        } else if id == "like" {
            Self::scope(id, "is_like")
        } else if id == "glob" {
            Self::scope(id, "is_glob")
        } else if id == "ilike" {
            Self::scope(id, "is_ilike")
        } else if id == "not_like" {
            Self::scope(id, "is_not_like")
        } else if id == "not_ilike" {
            Self::scope(id, "is_not_ilike")
        } else if id == "regex_match" {
            Self::scope(id, "is_regex")
        } else if id == "regex_imatch" {
            Self::scope(id, "is_iregex")
        } else if id == "not_regex_match" {
            Self::scope(id, "is_mismatch")
        } else if id == "not_regex_imatch" {
            Self::scope(id, "is_imismatch")
        } else if id == "in_" {
            Self::scope(id, "is_in")
        } else if id == "not_in" {
            Self::scope(id, "is_not_in")
        } else if id == "pg_contains" {
            Self::scope(id, "has_all")
        } else if id == "pg_overlaps" {
            Self::scope(id, "has_any")
        } else if id == "descendant_of" {
            Self::scope(id, "is_below")
        } else if id == "ancestor_of" {
            Self::scope(id, "is_above")
        } else if id == "order_by_desc" {
            Self::rename(id, "order_desc")
        } else if id == "returning_one" {
            Self::rename(id, "return_one")
        } else if id == "returning_first" {
            Self::rename(id, "return_first")
        } else if id == "returning_all" {
            Self::rename(id, "return_all")
        } else if id == "on_conflict_do_nothing" || id == "do_nothing" {
            Self::rename(id, "ignore")
        } else if id == "do_update" {
            Self::rename(id, "update")
        } else if id == "intersect_all" {
            Self::rename(id, "meet_all")
        } else {
            Self::Keep
        }
    }
}

pub(super) struct ScopeCalls {
    dialect: syn::Path,
    owner: Option<syn::Path>,
}

impl ScopeCalls {
    pub(super) fn sqlite(block: &syn::Block) -> syn::Block {
        Self::lower(block, syn::parse_quote!(::cartel_sqlite), true)
    }

    pub(super) fn pg(block: &syn::Block) -> syn::Block {
        Self::lower(block, syn::parse_quote!(::cartel_pg), true)
    }

    pub(super) fn pg_assign(block: &syn::Block) -> syn::Block {
        Self::lower(block, syn::parse_quote!(::cartel_pg), false)
    }

    fn lower(block: &syn::Block, dialect: syn::Path, scoped: bool) -> syn::Block {
        let mut block = block.clone();
        let owner = if scoped {
            Some(syn::parse_quote!(#dialect::dsl::Scope))
        } else {
            None
        };
        visit_mut::VisitMut::visit_block_mut(&mut Self { dialect, owner }, &mut block);
        block
    }
}

impl visit_mut::VisitMut for ScopeCalls {
    /// Assignments retain the field's exact type through the sealed trait.
    fn visit_expr_mut(&mut self, expr: &mut syn::Expr) {
        visit_mut::visit_expr_mut(self, expr);
        let syn::Expr::Assign(assign) = expr else {
            return;
        };
        let dialect = &self.dialect;
        let scope = backend::scope();
        let syn::ExprAssign {
            attrs,
            left,
            eq_token,
            right,
        } = assign;
        *expr = syn::parse_quote_spanned! { eq_token.span()=>
            #(#attrs)* #dialect::sql::Assign::assign(#scope.target(&mut #left), #right)
        };
    }
    /// Qualified calls preserve the SQL metadata's type path and default parameters.
    fn visit_expr_call_mut(&mut self, expr: &mut syn::ExprCall) {
        for argument in &mut expr.args {
            self.visit_expr_mut(argument);
        }
        let Some(owner) = &self.owner else {
            return;
        };
        let syn::Expr::Path(function) = expr.func.as_mut() else {
            return;
        };
        if function.qself.is_none() && function.path.segments.len() < 2 {
            return;
        }
        let Some(segment) = function.path.segments.last() else {
            return;
        };
        let dialect = function
            .path
            .segments
            .iter()
            .rev()
            .nth(1)
            .is_some_and(|segment| segment.ident == "Dialect");
        let action = if dialect {
            Action::function(&segment.ident)
        } else {
            Action::table(&segment.ident)
        };
        let scope = backend::scope();
        match action {
            Action::Keep => return,
            Action::Scope => expr.args.insert(0, syn::parse_quote!(#scope)),
            Action::ScopeAs(target) => {
                if dialect {
                    let args = segment.arguments.clone();
                    let mut path = owner.clone();
                    path.segments.push(syn::PathSegment {
                        ident: target,
                        arguments: args,
                    });
                    function.qself = None;
                    function.path = path;
                } else {
                    let Some(segment) = function.path.segments.last_mut() else {
                        return;
                    };
                    segment.ident = target;
                }
                expr.args.insert(0, syn::parse_quote!(#scope));
            }
            Action::Rename(target) => {
                let Some(segment) = function.path.segments.last_mut() else {
                    return;
                };
                segment.ident = target;
            }
        }
        if !dialect && function.qself.is_none() {
            let mut table = function.path.segments.iter();
            let Some(method) = table.next_back() else {
                return;
            };
            let leading = &function.path.leading_colon;
            let attrs = &function.attrs;
            *function = syn::parse_quote_spanned! { function.span()=>
                #(#attrs)* <#leading #(#table)::*>::#method
            };
        }
    }

    fn visit_expr_method_call_mut(&mut self, expr: &mut syn::ExprMethodCall) {
        self.visit_expr_mut(&mut expr.receiver);
        for argument in &mut expr.args {
            self.visit_expr_mut(argument);
        }
        if self.owner.is_none() {
            return;
        }
        let scope = backend::scope();
        match Action::method(&expr.method) {
            Action::Keep => {}
            Action::Scope => expr.args.insert(0, syn::parse_quote!(#scope)),
            Action::ScopeAs(target) => {
                expr.method = target;
                expr.args.insert(0, syn::parse_quote!(#scope));
            }
            Action::Rename(target) => expr.method = target,
        }
    }
}

pub(super) mod pg;
pub(super) mod sqlite;

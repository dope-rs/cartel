use crate::ext::SqlIdent as _;
use crate::parse::assign;
use crate::plan;
use crate::pred;
use crate::shape;

use std::slice;
use syn::spanned::Spanned as _;

pub(in crate::plan) enum Conflict {
    None,
    DoNothing,
    Target(String),
    Update(String, Vec<assign::Assign>),
}

impl Conflict {
    fn columns(columns: &[String]) -> String {
        let mut out = String::new();
        for (index, column) in columns.iter().enumerate() {
            if index > 0 {
                out.push(',');
            }
            out.push_str(&column.quoted());
        }
        out
    }

    pub(in crate::plan) fn new(
        kind: &shape::Conflict,
        source: &shape::QuerySource,
        state: &plan::State<'_>,
    ) -> syn::Result<Self> {
        match kind {
            shape::Conflict::None => Ok(Self::None),
            shape::Conflict::DoNothing => Ok(Self::DoNothing),
            shape::Conflict::TargetDoNothing(target) => {
                let cols = Self::columns(&target.cols);
                Ok(Self::Target(format!(" ON CONFLICT ({cols}) DO NOTHING")))
            }
            shape::Conflict::TargetDoUpdate(target, update) => {
                let scope = pred::RowScope::for_source(source, slice::from_ref(&update.set_arg))?;
                let assigns = assign::Assign::collect(
                    &update.set_body,
                    &update.set_arg,
                    &scope,
                    state.fn_params,
                    state.param_tys,
                    state.dialect,
                    state.cte_map,
                )?;
                if assigns.is_empty() {
                    return Err(syn::Error::new(
                        update.set_body.span(),
                        "`.do_update(...)` body must assign at least one column",
                    ));
                }
                let cols = Self::columns(&target.cols);
                let prefix = format!(" ON CONFLICT ({cols}) DO UPDATE SET ");
                Ok(Self::Update(prefix, assigns))
            }
        }
    }

    pub(in crate::plan) fn render<'a>(
        self,
        mut state: plan::State<'a>,
    ) -> plan::Step<'a, Vec<proc_macro2::TokenStream>> {
        let value = match self {
            Self::None => Vec::new(),
            Self::DoNothing => vec![state.dialect.kw("ON_CONFLICT_DO_NOTHING")],
            Self::Target(sql) => vec![quote::quote! { #sql }],
            Self::Update(prefix, assigns) => {
                let mut parts = vec![quote::quote! { #prefix }];
                for (index, assign) in assigns.into_iter().enumerate() {
                    if index > 0 {
                        parts.push(state.dialect.kw("COMMA"));
                    }
                    let col_eq = format!("{} = ", assign.column.quoted());
                    parts.push(quote::quote! { #col_eq });
                    parts.extend(state.render(assign.value));
                }
                parts
            }
        };
        plan::Step { state, value }
    }
}

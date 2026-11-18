use crate::ext::SqlIdent as _;
use crate::parse::assign;
use crate::plan;
use crate::pred;
use crate::shape;

use std::slice;

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
        state: &plan::State<'_, '_>,
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
                    &state.context(),
                    "`.do_update(...)` body must assign at least one column",
                )?;
                let cols = Self::columns(&target.cols);
                let prefix = format!(" ON CONFLICT ({cols}) DO UPDATE SET ");
                Ok(Self::Update(prefix, assigns))
            }
        }
    }

    pub(in crate::plan) fn append(
        self,
        state: &mut plan::State<'_, '_>,
        sql: &mut Vec<proc_macro2::TokenStream>,
    ) {
        match self {
            Self::None => {}
            Self::DoNothing => sql.push(state.dialect.kw("ON_CONFLICT_DO_NOTHING")),
            Self::Target(target) => sql.push(quote::quote! { #target }),
            Self::Update(prefix, assigns) => {
                sql.push(quote::quote! { #prefix });
                super::assignments(state, sql, assigns);
            }
        }
    }
}

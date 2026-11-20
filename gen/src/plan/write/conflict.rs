use crate::emit::token::fragment;
use crate::ext::SqlIdent as _;
use crate::parse::assign;
use crate::plan;
use crate::pred;
use crate::shape;

pub(in crate::plan) enum Conflict {
    None,
    DoNothing,
    Target(String),
    Update(String, Vec<assign::Assign>),
}

impl Conflict {
    fn columns(
        columns: &[&syn::Ident],
        table: &syn::Path,
        state: &mut plan::State<'_, '_>,
    ) -> String {
        let scope = crate::backend::scope();
        state
            .checks
            .extend(quote::quote! { #(let _ = #scope.value::<#table>().#columns;)* });
        let mut out = String::new();
        for (index, column) in columns.iter().enumerate() {
            if index > 0 {
                out.push(',');
            }
            out.push_str(&crate::ext::name(column).quoted());
        }
        out
    }

    pub(in crate::plan) fn new(
        kind: &shape::Conflict<'_>,
        source: &shape::QuerySource<'_>,
        state: &mut plan::State<'_, '_>,
    ) -> syn::Result<Self> {
        let table = &source.primary_path;
        match kind {
            shape::Conflict::None => Ok(Self::None),
            shape::Conflict::DoNothing => Ok(Self::DoNothing),
            shape::Conflict::TargetDoNothing(target) => {
                let cols = Self::columns(&target.cols, table, state);
                Ok(Self::Target(format!(" ON CONFLICT ({cols}) DO NOTHING")))
            }
            shape::Conflict::TargetDoUpdate(target, update) => {
                let scope = pred::RowScope::for_primary(source, update.set_arg);
                let assigns = assign::Assign::collect::<false>(
                    update.set_body,
                    update.set_arg,
                    &scope,
                    quote::quote!(#table),
                    state,
                    "`.do_update(...)` body must assign at least one column",
                )?;
                let cols = Self::columns(&target.cols, table, state);
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
            Self::DoNothing => sql.push(fragment(" ON CONFLICT DO NOTHING")),
            Self::Target(target) => sql.push(quote::quote! { #target }),
            Self::Update(prefix, assigns) => {
                sql.push(quote::quote! { #prefix });
                super::assignments(state, sql, assigns);
            }
        }
    }
}

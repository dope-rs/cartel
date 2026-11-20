use crate::emit::token::{COMMA, fragment};
mod conflict;
pub(in crate::plan) mod insert;
pub(in crate::plan) mod reply;
pub(in crate::plan) mod update;

pub(in crate::plan) use conflict::Conflict;

fn assignments(
    state: &mut super::State<'_, '_>,
    sql: &mut Vec<proc_macro2::TokenStream>,
    values: Vec<crate::parse::assign::Assign>,
) {
    use crate::ext::SqlIdent as _;

    for (index, assign) in values.into_iter().enumerate() {
        if index > 0 {
            sql.push(fragment(COMMA));
        }
        let column = format!("{} = ", assign.column.quoted());
        sql.push(quote::quote! { #column });
        state.render_into(assign.value, sql);
    }
}

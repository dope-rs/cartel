use quote::quote;
use syn::Type;

use crate::backend::Dialect;

pub(super) trait RowTyExt {
    fn n_cols_const(&self) -> proc_macro2::TokenStream;
    fn qualified_select_cols(&self, dialect: &Dialect) -> Vec<proc_macro2::TokenStream>;
}

impl RowTyExt for Type {
    fn n_cols_const(&self) -> proc_macro2::TokenStream {
        if let Type::Tuple(tup) = self {
            let mut acc = quote! { 0u16 };
            for elem in &tup.elems {
                acc = quote! { #acc + <#elem>::__CARTEL_N_COLS };
            }
            return acc;
        }
        quote! { <#self>::__CARTEL_N_COLS }
    }

    fn qualified_select_cols(&self, dialect: &Dialect) -> Vec<proc_macro2::TokenStream> {
        if let Type::Tuple(tup) = self {
            let mut parts = Vec::new();
            for (i, elem) in tup.elems.iter().enumerate() {
                if i > 0 {
                    parts.push(dialect.kw("COMMA_TIGHT"));
                }
                parts.push(quote! { <#elem>::__CARTEL_SELECT_COLS_QUALIFIED });
            }
            return parts;
        }
        vec![quote! { <#self>::__CARTEL_SELECT_COLS_QUALIFIED }]
    }
}

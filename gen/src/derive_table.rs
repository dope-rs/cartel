use syn::spanned::Spanned;
use syn::{Data, DataStruct, DeriveInput, Fields, FieldsNamed};

use crate::backend::TableSpec;
use crate::util::AttrSliceExt;

pub(super) fn parse(input: &DeriveInput) -> syn::Result<TableSpec<'_>> {
    let fields = match &input.data {
        Data::Struct(DataStruct {
            fields: Fields::Named(FieldsNamed { named, .. }),
            ..
        }) => named,
        _ => {
            return Err(syn::Error::new(
                input.span(),
                "Table can only be derived for structs with named fields",
            ));
        }
    };
    let table_name = input.attrs.table_name(&input.ident)?;
    let pk_cols = fields
        .iter()
        .filter(|f| f.attrs.has_pk())
        .map(|f| f.ident.as_ref().expect("named field").to_string())
        .collect();
    Ok(TableSpec {
        input,
        table_name,
        fields,
        pk_cols,
    })
}

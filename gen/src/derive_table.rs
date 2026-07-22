use syn::spanned::Spanned;
use syn::{Data, DataStruct, DeriveInput, Fields, FieldsNamed};

use crate::backend::{TableField, TableSpec};
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
    let mut table_fields = Vec::with_capacity(fields.len());
    let mut pk_cols = Vec::new();
    for field in fields {
        let Some(name) = field.ident.as_ref() else {
            return Err(syn::Error::new(field.span(), "expected a named field"));
        };
        if field.attrs.has_pk() {
            pk_cols.push(name.to_string());
        }
        table_fields.push(TableField {
            name,
            ty: &field.ty,
        });
    }
    Ok(TableSpec {
        input,
        table_name,
        fields: table_fields,
        pk_cols,
    })
}

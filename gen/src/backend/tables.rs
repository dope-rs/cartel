use crate::backend;
use syn::spanned::Spanned as _;

pub(super) trait TableParse {
    fn parse_table(&self) -> syn::Result<backend::TableSpec<'_>>;
}

impl TableParse for syn::DeriveInput {
    fn parse_table(&self) -> syn::Result<backend::TableSpec<'_>> {
        let input = self;
        let data = match &input.data {
            syn::Data::Struct(data) => data,
            syn::Data::Enum(data) => {
                return Err(syn::Error::new(
                    data.enum_token.span,
                    "Table can only be derived for structs with named fields",
                ));
            }
            syn::Data::Union(data) => {
                return Err(syn::Error::new(
                    data.union_token.span,
                    "Table can only be derived for structs with named fields",
                ));
            }
        };
        if let syn::Fields::Unnamed(fields) = &data.fields {
            return Err(syn::Error::new(
                fields.paren_token.span.open(),
                "Table can only be derived for structs with named fields",
            ));
        }
        let fields = data.fields.iter();
        let mut names = input
            .attrs
            .iter()
            .filter(|attr| attr.path().is_ident("table_name"));
        let name = names.next();
        if let Some(dup) = names.next() {
            return Err(syn::Error::new_spanned(
                dup,
                "duplicate table_name attribute",
            ));
        }
        let table = match name {
            Some(attr) => {
                let literal = attr.parse_args::<syn::LitStr>()?;
                let value = literal.value();
                if value.as_bytes().contains(&0) || value.split('.').any(str::is_empty) {
                    return Err(syn::Error::new_spanned(
                        literal,
                        "table name segments must be nonempty and contain no NUL byte",
                    ));
                }
                value
            }
            None => {
                let mut table = String::new();
                for (index, ch) in crate::ext::unraw(&input.ident.to_string())
                    .chars()
                    .enumerate()
                {
                    if ch.is_uppercase() && index > 0 {
                        table.push('_');
                    }
                    table.extend(ch.to_lowercase());
                }
                if !table.ends_with('s') {
                    table.push('s');
                }
                table
            }
        };
        let mut cols = Vec::with_capacity(fields.len());
        for field in fields {
            let Some(name) = field.ident.as_ref() else {
                return Err(syn::Error::new(field.span(), "expected a named field"));
            };
            cols.push(backend::TableField {
                name,
                ty: &field.ty,
                vis: &field.vis,
            });
        }
        Ok(backend::TableSpec {
            input,
            table_name: table,
            fields: cols,
        })
    }
}

use crate::backend;
use crate::ext::SqlIdent as _;

impl backend::TableBackend for super::PgBackend {
    fn emit_table(
        spec: backend::TableSpec<'_>,
        dialect: &backend::Dialect,
    ) -> syn::Result<proc_macro2::TokenStream> {
        let columns = spec.columns();
        let backend::TableSpec {
            input,
            table_name: table,
            fields,
        } = spec;
        if let Some(param) = input.generics.params.first() {
            return Err(syn::Error::new_spanned(
                param,
                "#[derive(PgTable)] does not support generic structs",
            ));
        }
        if let Some(clause) = &input.generics.where_clause {
            return Err(syn::Error::new_spanned(
                clause,
                "#[derive(PgTable)] does not support generic structs",
            ));
        }
        let rt = &dialect.rt_crate;
        let name = &input.ident;
        if fields.len() > i16::MAX as usize {
            return Err(syn::Error::new(
                name.span(),
                format!(
                    "PostgreSQL row has {} columns; maximum is {}",
                    fields.len(),
                    i16::MAX
                ),
            ));
        }
        let mut batch = false;
        for attr in &input.attrs {
            if attr.path().is_ident("table") {
                attr.parse_nested_meta(|meta| {
                    if !meta.path.is_ident("batch") {
                        return Err(meta.error("expected batch"));
                    }
                    if batch {
                        return Err(meta.error("duplicate batch"));
                    }
                    batch = true;
                    Ok(())
                })?;
            }
        }
        let sql = table.quoted();
        if fields.is_empty() {
            if batch {
                return Err(syn::Error::new_spanned(input, "batch requires columns"));
            }
            return Ok(quote::quote! {
                impl #name {
                    #[doc(hidden)]
                    pub const __CARTEL_TABLE: &'static str = #sql;
                }
            });
        }
        let emit = if batch {
            let slices = quote::format_ident!("{name}Slices");
            let vis = &input.vis;
            let slice = fields.iter().map(|field| {
                let (vis, name, ty) = (field.vis, field.name, field.ty);
                quote::quote! { #vis #name: &'data [<#ty as #rt::dsl::shape::BatchColumn>::Item<'data>] }
            });
            quote::quote! {
                #vis struct #slices<'data> { #(#slice,)* }

                impl #rt::dsl::shape::Batch for #name {
                    type Columns<'data> = #slices<'data>;
                }
            }
        } else {
            quote::quote! {}
        };

        Ok(quote::quote! {
            #emit

            impl #name {
                #columns

                #[doc(hidden)]
                pub const __CARTEL_TABLE: &'static str = #sql;
            }
        })
    }
}

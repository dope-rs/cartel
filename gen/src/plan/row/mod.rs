use crate::backend;
use crate::shape;
use std::iter;
use syn::spanned::Spanned as _;

pub(in crate::plan) struct View<'a>(&'a syn::Type);

impl<'a> View<'a> {
    pub(in crate::plan) const fn new(ty: &'a syn::Type) -> Self {
        Self(ty)
    }

    pub(in crate::plan) fn cols(&self) -> proc_macro2::TokenStream {
        if let syn::Type::Tuple(tup) = self.0 {
            let mut cols = quote::quote! { 0u16 };
            for ty in &tup.elems {
                cols = quote::quote! { #cols + <#ty>::__CARTEL_N_COLS };
            }
            return cols;
        }
        let ty = self.0;
        quote::quote! { <#ty>::__CARTEL_N_COLS }
    }

    pub(in crate::plan) fn select(
        &self,
        source: &shape::QuerySource,
        dialect: &backend::Dialect,
    ) -> syn::Result<Vec<proc_macro2::TokenStream>> {
        let rows: Vec<_> = match self.0 {
            syn::Type::Tuple(tuple) => tuple.elems.iter().collect(),
            row => vec![row],
        };
        let sources: Vec<_> = iter::once(&source.primary_path)
            .chain(source.joins.iter().map(|join| &join.path))
            .collect();
        let matches = |row: &syn::Type, path: &syn::Path| matches!(row, syn::Type::Path(row) if row.qself.is_none() && row.path == *path);
        let ordered = rows.len() == sources.len()
            && rows
                .iter()
                .zip(&sources)
                .all(|(row, path)| matches(row, path));
        let mut sql = Vec::new();
        for (position, row) in rows.iter().enumerate() {
            let prefix = if ordered {
                source.qualifier(position)
            } else {
                let mut hits = sources
                    .iter()
                    .enumerate()
                    .filter(|(_, path)| matches(row, path));
                match hits.next() {
                    Some((index, _)) => {
                        if hits.next().is_some() {
                            return Err(syn::Error::new(
                                row.span(),
                                "implicit row projection is ambiguous; return every source in order or use `.select(...)`",
                            ));
                        }
                        source.qualifier(index)
                    }
                    None => Self::physical(row, &sources, source, dialect),
                }
            };
            if position > 0 {
                sql.push(dialect.kw("COMMA_TIGHT"));
            }
            sql.push(dialect.columns(row, prefix));
        }
        Ok(sql)
    }

    fn physical(
        row: &syn::Type,
        paths: &[&syn::Path],
        source: &shape::QuerySource,
        dialect: &backend::Dialect,
    ) -> proc_macro2::TokenStream {
        let rt = &dialect.rt_crate;
        let aliases = (0..paths.len()).map(|index| source.qualifier(index));
        quote::quote! {{
            const SOURCES: &[(&str, &str)] = &[
                #((<#paths>::__CARTEL_TABLE, #aliases)),*
            ];
            let table = <#row>::__CARTEL_TABLE;
            let mut selected = "";
            let mut found = false;
            let mut index = 0usize;
            while index < SOURCES.len() {
                if #rt::sql::raw::Parts::new(&[table]).matches(SOURCES[index].0) {
                    ::core::assert!(!found,
                        "implicit row projection is ambiguous; use `.select(...)`");
                    selected = SOURCES[index].1;
                    found = true;
                }
                index += 1;
            }
            ::core::assert!(found,
                "implicit row projection must name a query source");
            selected
        }}
    }
}

const _: () = assert!(std::mem::size_of::<View<'_>>() == std::mem::size_of::<&syn::Type>());

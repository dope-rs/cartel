use crate::dialect::{name::Name, sqlite::Query};
use syn::parse;
use syn::punctuated::Punctuated;
use syn::spanned::Spanned as _;

pub(crate) struct Catalog {
    visibility: syn::Visibility,
    name: syn::Ident,
    items: Vec<syn::Item>,
}

struct Raw {
    name: syn::Ident,
    parameters: syn::LitInt,
    columns: syn::LitInt,
    assign: syn::Token![=],
    sql: syn::LitStr,
}

struct Reference(syn::Path);

impl parse::Parse for Raw {
    fn parse(input: parse::ParseStream<'_>) -> syn::Result<Self> {
        let name = input.parse()?;
        let content;
        syn::parenthesized!(content in input);
        let params = content.parse()?;
        content.parse::<syn::Token![,]>()?;
        let columns = content.parse()?;
        if !content.is_empty() {
            return Err(content.error("expected two arity values"));
        }
        let assign = input.parse()?;
        let sql = input.parse()?;
        Ok(Self {
            name,
            parameters: params,
            columns,
            assign,
            sql,
        })
    }
}

impl parse::Parse for Reference {
    fn parse(input: parse::ParseStream<'_>) -> syn::Result<Self> {
        let mut path = input.parse::<syn::Path>()?;
        let Some(segment) = path.segments.last_mut() else {
            return Err(input.error("query path is empty"));
        };
        if !matches!(segment.arguments, syn::PathArguments::None) {
            return Err(syn::Error::new_spanned(segment, "query path has arguments"));
        }
        segment.ident = Query::marker(&segment.ident);
        Ok(Self(path))
    }
}

impl parse::Parse for Catalog {
    fn parse(input: parse::ParseStream<'_>) -> syn::Result<Self> {
        let vis = input.parse()?;
        let name = input.parse()?;
        input.parse::<syn::Token![;]>()?;
        let mut items = Vec::new();
        while !input.is_empty() {
            items.push(input.parse()?);
        }
        Ok(Self {
            visibility: vis,
            name,
            items,
        })
    }
}

impl Catalog {
    pub(crate) fn from_tokens(
        input: proc_macro::TokenStream,
    ) -> syn::Result<proc_macro2::TokenStream> {
        syn::parse::<Self>(input)?.expand()
    }

    fn expand(self) -> syn::Result<proc_macro2::TokenStream> {
        let Self {
            visibility: vis,
            name,
            items,
        } = self;
        let mut emitted = Vec::with_capacity(items.len());
        let mut queries = Vec::new();
        let mut raw = Vec::new();
        for item in items {
            match item {
                syn::Item::Fn(mut function) => {
                    let attr = function
                        .attrs
                        .iter()
                        .position(|attr| attr.path().is_ident("query"));
                    match attr {
                        Some(index) => {
                            let attr = function.attrs.remove(index);
                            let meta = attr.meta.require_path_only()?;
                            let (mut marker, tokens) = Query::expand(&function)?;
                            marker.set_span(meta.span());
                            queries.push(syn::Path::from(marker));
                            emitted.push(tokens);
                        }
                        None => emitted.push(quote::quote! { #function }),
                    }
                }
                syn::Item::Macro(item) if item.mac.path.is_ident("statements") => {
                    raw.extend(
                        item.mac
                            .parse_body_with(Punctuated::<Raw, syn::Token![,]>::parse_terminated)?,
                    );
                }
                syn::Item::Macro(item) if item.mac.path.is_ident("queries") => {
                    let paths = item.mac.parse_body_with(
                        Punctuated::<Reference, syn::Token![,]>::parse_terminated,
                    )?;
                    queries.extend(paths.into_iter().map(|query| query.0));
                }
                item => emitted.push(quote::quote! { #item }),
            }
        }
        let raw_ids = raw
            .iter()
            .map(|entry| syn::Path::from(entry.marker(&name)))
            .collect::<Vec<_>>();
        let markers = raw.iter().zip(&raw_ids).map(|(entry, marker)| {
            let sql = &entry.sql;
            let params = &entry.parameters;
            let columns = &entry.columns;
            let assign = &entry.assign;
            quote::quote! {
                #[doc(hidden)]
                pub struct #marker;

                impl ::cartel_sqlite::TypedQuery for #marker {
                    const SQL: &str #assign #sql;
                    const PARAMETERS: usize = #params;
                    const COLUMNS: usize = #columns;
                }
            }
        });
        let queries = queries.iter().chain(raw_ids.iter());
        let metas = queries.clone().map(|query| {
            quote::quote! {
                ::cartel_sqlite::Entry::of::<#query>()
            }
        });
        let slots = queries.enumerate().map(|(slot, query)| {
            quote::quote! {
                impl ::cartel_sqlite::HasQuery<#query> for #name {
                    const SLOT: usize = #slot;
                }
            }
        });
        let consts = raw.iter().zip(raw_ids.iter()).map(|(entry, query)| {
            let stmt = &entry.name;
            quote::quote! {
                pub const #stmt: ::cartel_sqlite::Statement<Self, #query> =
                    ::cartel_sqlite::Statement::<Self, #query>::of();
            }
        });
        Ok(quote::quote! {
            #(#emitted)*
            #(#markers)*

            #vis struct #name;

            impl ::cartel_sqlite::Catalog for #name {
                const STATEMENTS: &[::cartel_sqlite::Entry<'_>] = &[
                    #(#metas,)*
                ];
            }

            #(#slots)*

            impl #name {
                #(#consts)*
            }
        })
    }
}

impl Raw {
    fn marker(&self, catalog: &syn::Ident) -> syn::Ident {
        let catalog = Name::new(catalog);
        let stmt = Name::new(&self.name);
        let (catalog, stmt) = (catalog.as_str(), stmt.as_str());
        quote::format_ident!("__CartelSqliteRaw{catalog}{stmt}")
    }
}

use crate::backend;
use crate::dialect::pg;
use std::collections::BTreeSet;
use std::iter;
use syn::parse;
use syn::punctuated;

use syn::spanned::Spanned as _;

pub(crate) struct Group;

impl Group {
    fn queries(
        items: Vec<syn::ImplItem>,
        group: &syn::Type,
        owner: &syn::Ident,
    ) -> syn::Result<Vec<(syn::Ident, proc_macro2::TokenStream)>> {
        let mut names = BTreeSet::new();
        let mut methods = BTreeSet::new();
        let mut queries = Vec::with_capacity(items.len());
        for item in items {
            let syn::ImplItem::Fn(fun) = item else {
                return Err(syn::Error::new(
                    item.span(),
                    "#[query_group] impl block may only contain query methods",
                ));
            };
            fun.modifiers.require_empty()?;
            backend::Compiled::validate(&fun.sig, &fun.attrs, "#[query_group] methods")?;
            let name = fun.sig.ident;
            let text = name.to_string();
            let text = text.trim_start_matches("r#");
            if Group::DSL_BUILDER_METHODS.contains(&text) {
                return Err(syn::Error::new(
                    name.span(),
                    format!(
                        "query method name `{text}` collides with a DSL builder method on the table type — rename it"
                    ),
                ));
            }
            let code = backend::Compiled::build::<pg::PgBackend>(
                &fun.sig.generics,
                fun.sig.inputs.into_iter().map(backend::Compiled::fn_arg),
                fun.block,
                &fun.sig.output,
            )?;
            let each = (!matches!(code.plan.dispatch, crate::emit::Kind::Stream(_)))
                .then(|| Group::batch_method(&name, "each"));
            let grouped = matches!(code.plan.dispatch, crate::emit::Kind::One(_))
                .then(|| Group::batch_method(&name, "group"));
            for method in iter::once(&name).chain(each.iter()).chain(grouped.iter()) {
                let text = method.to_string();
                let text = text.trim_start_matches("r#");
                if !methods.insert(text.to_owned()) {
                    return Err(syn::Error::new(
                        name.span(),
                        format!(
                            "query method name `{text}` collides with another query or generated batch method — rename it"
                        ),
                    ));
                }
            }
            let marker = Group::query_name(owner, &name);
            if !names.insert(marker.to_string()) {
                return Err(syn::Error::new(
                    name.span(),
                    "query methods must have distinct public query names",
                ));
            }
            let q = pg::GroupQuery {
                compiled: code,
                group_ty: group.clone(),
                group_ident: owner.clone(),
                marker: marker.clone(),
                method: name,
                vis: fun.vis,
            };
            queries.push((marker, pg::PgBackend::emit_query(q, &fun.attrs)?));
        }
        Ok(queries)
    }

    pub(super) fn batch_method(method: &syn::Ident, suffix: &str) -> syn::Ident {
        let name = method.to_string();
        let name = name.trim_start_matches("r#");
        syn::Ident::new(&format!("{name}_{suffix}"), method.span())
    }

    fn query_name(owner: &syn::Ident, method: &syn::Ident) -> syn::Ident {
        let span = method.span();
        let method = method.to_string();
        let method = method.trim_start_matches("r#");
        let mut name = owner.to_string().trim_start_matches("r#").to_owned();
        for word in method.split('_') {
            let mut chars = word.chars();
            if let Some(first) = chars.next() {
                name.extend(first.to_uppercase());
                name.extend(chars);
            }
        }
        syn::Ident::new(&name, span)
    }

    const DSL_BUILDER_METHODS: &[&str] = &[
        "filter",
        "join",
        "left_join",
        "right_join",
        "full_join",
        "lateral_join",
        "lateral_left",
        "lateral_left_join",
        "filter_each",
        "insert",
        "insert_each",
        "insert_from",
    ];

    pub(crate) fn expand(block: syn::ItemImpl) -> syn::Result<proc_macro2::TokenStream> {
        block.modifiers.require_empty()?;
        if let Some(attr) = block.attrs.first() {
            return Err(syn::Error::new_spanned(
                attr,
                "#[query_group] impl attributes are not supported; place attributes on individual query methods",
            ));
        }
        if let Some(mark) = &block.unsafety {
            return Err(syn::Error::new_spanned(
                mark,
                "#[query_group] does not support unsafe impl blocks",
            ));
        }
        if block.trait_.is_some() {
            return Err(syn::Error::new(
                block.span(),
                "#[query_group] applies to an inherent impl block, not a trait impl",
            ));
        }
        if !block.generics.params.is_empty() || block.generics.where_clause.is_some() {
            return Err(syn::Error::new(
                block.generics.span(),
                "#[query_group] impl block must not be generic",
            ));
        }
        let group = *block.self_ty;
        let owner = match &group {
            syn::Type::Path(tp) => tp
                .path
                .segments
                .last()
                .map(|s| s.ident.clone())
                .ok_or_else(|| syn::Error::new(group.span(), "empty group type path"))?,
            other => {
                return Err(syn::Error::new(
                    other.span(),
                    "#[query_group] group type must be a named type",
                ));
            }
        };

        let queries = Self::queries(block.items, &group, &owner)?;

        let metas: proc_macro2::TokenStream = queries
            .iter()
            .map(|entry| {
                let query = &entry.0;
                quote::quote! {
                    ::cartel_pg::schema::QueryMeta::fixed::<#query>(),
                }
            })
            .collect();
        let items = queries.iter().rev().fold(
            quote::quote! { ::cartel_pg::schema::groups::Empty },
            |tail, entry| {
                let query = &entry.0;
                quote::quote! {
                    ::cartel_pg::schema::groups::Item<#query, #tail>
                }
            },
        );
        let mut out: proc_macro2::TokenStream = queries.into_iter().map(|entry| entry.1).collect();

        out.extend(quote::quote! {
            impl ::cartel_pg::schema::QueryGroup for #group {
                type Items = #items;

                const QUERIES: &[::cartel_pg::schema::QueryMeta<'_>] = &[ #metas ];
            }
        });

        Ok(out)
    }
}

pub(super) struct InstanceDecl {
    vis: syn::Visibility,
    marker: syn::Ident,
    groups: Vec<syn::Type>,
}

impl parse::Parse for InstanceDecl {
    fn parse(input: parse::ParseStream<'_>) -> syn::Result<Self> {
        let vis = input.parse()?;
        let marker: syn::Ident = input.parse()?;
        input.step(|cursor| {
            let Some((punct, next)) = cursor.punct() else {
                return Err(cursor.error("expected `:`"));
            };
            if punct.as_char() != ':' {
                return Err(cursor.error("expected `:`"));
            }
            Ok(((), next))
        })?;
        let groups = punctuated::Punctuated::<syn::Type, syn::Token![,]>::parse_terminated(input)?;
        Ok(Self {
            vis,
            marker,
            groups: groups.into_iter().collect(),
        })
    }
}

impl InstanceDecl {
    pub(super) fn expand(&self) -> proc_macro2::TokenStream {
        let vis = &self.vis;
        let marker = &self.marker;
        let groups = &self.groups;
        let impls = groups.iter().map(|g| {
            quote::quote! {
                impl ::cartel_pg::schema::HasGroup<#g> for #marker {}
            }
        });
        let list = groups.iter().rev().fold(
            quote::quote! { ::cartel_pg::schema::groups::End },
            |tail, group| quote::quote! { ::cartel_pg::schema::groups::Cons<#group, #tail> },
        );
        quote::quote! {
            #vis struct #marker;

            #( #impls )*

            impl ::cartel_pg::schema::QuerySet for #marker {
                type Groups = #list;

                const QUERIES: &[&[::cartel_pg::schema::QueryMeta<'_>]] = &[
                    #( <#groups as ::cartel_pg::schema::QueryGroup>::QUERIES, )*
                ];

                const PREPARE: ::cartel_pg::schema::Prepare<'_> = {
                    const __WIRE_LEN: usize = ::cartel_pg::schema::Prepare::fixed_len::<#marker>();
                    const __WIRE: ::cartel_pg::schema::Wire<'_, __WIRE_LEN> =
                        ::cartel_pg::schema::Prepare::fixed::<#marker, __WIRE_LEN>();
                    __WIRE.prepare()
                };

            }
        }
    }
}

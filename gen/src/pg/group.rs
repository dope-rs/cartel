use quote::{format_ident, quote};
use syn::parse::{Parse, ParseStream};
use syn::punctuated::Punctuated;
use syn::spanned::Spanned;
use syn::{Ident, ImplItem, ItemImpl, Token, Type};

use crate::backend::Compiled;
use crate::pg::{GroupQuery, PgBackend};

pub(super) struct QueryGroupItem;

impl QueryGroupItem {
    const DSL_BUILDER_METHODS: &'static [&'static str] = &[
        "filter",
        "join",
        "left_join",
        "right_join",
        "full_join",
        "lateral_join",
        "lateral_left_join",
        "filter_each",
        "insert",
        "insert_each",
        "insert_from",
    ];

    pub(super) fn expand(block: ItemImpl) -> syn::Result<proc_macro2::TokenStream> {
        if block.trait_.is_some() {
            return Err(syn::Error::new(
                block.span(),
                "#[query_group] applies to an inherent impl block, not a trait impl",
            ));
        }
        if !block.generics.params.is_empty() {
            return Err(syn::Error::new(
                block.generics.span(),
                "#[query_group] impl block must not be generic",
            ));
        }
        let group_ty = (*block.self_ty).clone();
        let group_ident = match &group_ty {
            Type::Path(tp) => tp
                .path
                .segments
                .last()
                .map(|s| s.ident.clone())
                .ok_or_else(|| syn::Error::new(group_ty.span(), "empty group type path"))?,
            _ => {
                return Err(syn::Error::new(
                    group_ty.span(),
                    "#[query_group] group type must be a named type",
                ));
            }
        };

        let mut out = proc_macro2::TokenStream::new();
        let mut q_structs: Vec<Ident> = Vec::new();
        for item in &block.items {
            let ImplItem::Fn(method) = item else {
                return Err(syn::Error::new(
                    item.span(),
                    "#[query_group] impl block may only contain query methods",
                ));
            };
            if method.sig.asyncness.is_some() {
                return Err(syn::Error::new(
                    method.sig.span(),
                    "#[query_group] methods must not be async",
                ));
            }
            let compiled = Compiled::build::<PgBackend>(
                &method.sig.generics,
                method.sig.inputs.iter().map(Compiled::fn_arg),
                &method.block,
                &method.sig.output,
                method.sig.span(),
            )?;
            let method_ident = method.sig.ident.clone();
            let method_name = method_ident.to_string();
            if Self::DSL_BUILDER_METHODS.contains(&method_name.as_str()) {
                return Err(syn::Error::new(
                    method_ident.span(),
                    format!(
                        "query method name `{method_name}` collides with a DSL builder method on the table type — rename it"
                    ),
                ));
            }
            q_structs.push(format_ident!(
                "__CartelPgQuery_{}_{}",
                group_ident,
                method_ident
            ));
            let q = GroupQuery {
                compiled,
                group_ty: group_ty.clone(),
                group_ident: group_ident.clone(),
                method: method_ident,
                vis: method.vis.clone(),
            };
            out.extend(PgBackend::emit_grouped_query(&q)?);
        }

        let metas = q_structs.iter().map(|qs| {
            quote! {
                ::cartel_pg::QueryMeta {
                    name: <#qs as ::cartel_pg::TypedQuery>::STATEMENT_NAME,
                    sql: <#qs as ::cartel_pg::TypedQuery>::SQL,
                    param_oids: <#qs as ::cartel_pg::TypedQuery>::PARAM_OIDS,
                }
            }
        });

        out.extend(quote! {
            impl ::cartel_pg::QueryGroup for #group_ty {
                const QUERIES: &'static [::cartel_pg::QueryMeta] = &[ #( #metas, )* ];
            }
        });

        Ok(out)
    }
}

pub(super) struct InstanceDecl {
    marker: Ident,
    groups: Vec<Type>,
}

impl Parse for InstanceDecl {
    fn parse(input: ParseStream<'_>) -> syn::Result<Self> {
        let marker: Ident = input.parse()?;
        input.parse::<Token![:]>()?;
        let groups = Punctuated::<Type, Token![,]>::parse_terminated(input)?;
        Ok(Self {
            marker,
            groups: groups.into_iter().collect(),
        })
    }
}

impl InstanceDecl {
    pub(super) fn expand(&self) -> proc_macro2::TokenStream {
        let marker = &self.marker;
        let groups = &self.groups;
        let has_group = groups.iter().map(|g| {
            quote! {
                impl ::cartel_pg::HasGroup<#g> for #marker {}
            }
        });
        quote! {
            pub struct #marker;

            #( #has_group )*

            impl ::cartel_pg::QuerySet for #marker {
                const GROUPS: &'static [&'static [::cartel_pg::QueryMeta]] = &[
                    #( <#groups as ::cartel_pg::QueryGroup>::QUERIES, )*
                ];
            }
        }
    }
}

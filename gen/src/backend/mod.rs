use crate::emit;
use crate::ext::SqlIdent as _;
use crate::parse::{self, ctes};
use crate::plan;
use crate::shape;
use ctes::Binding as _;
use parse::Shape as _;
use std::collections;

use syn::{ext::IdentExt as _, spanned::Spanned as _};

mod names;
pub(super) use names::Names;
mod tables;
use tables::TableParse as _;

pub(super) fn scope() -> syn::Ident {
    syn::Ident::new("scope", proc_macro2::Span::mixed_site())
}

pub(super) trait Backend {
    fn dialect() -> Dialect;
}

pub(super) trait TableBackend: Backend {
    fn emit_table(spec: TableSpec<'_>, dialect: &Dialect) -> syn::Result<proc_macro2::TokenStream>;

    fn derive_table(input: &syn::DeriveInput) -> syn::Result<proc_macro2::TokenStream> {
        let dialect = Self::dialect();
        input
            .parse_table()
            .and_then(|spec| Self::emit_table(spec, &dialect))
    }
}

pub(super) struct Compiled {
    pub(super) param_ids: Vec<syn::Ident>,
    pub(super) param_tys: Vec<syn::Type>,
    pub(super) captures: Vec<syn::Ident>,
    pub(super) nonnegative: Vec<(syn::Ident, plan::Bound)>,
    pub(super) plan: emit::Plan,
    pub(super) block: syn::Block,
}

impl Compiled {
    pub(super) fn validate(
        sig: &syn::Signature,
        attrs: &[syn::Attribute],
        owner: &str,
    ) -> syn::Result<()> {
        if let syn::Safety::Unsafe(mark) = &sig.safety {
            return Err(syn::Error::new_spanned(
                mark,
                format!("{owner} must not be unsafe"),
            ));
        }
        let kind = if sig.asyncness.is_some() {
            Some("async")
        } else if sig.constness.is_some() {
            Some("const")
        } else if sig.abi.is_some() {
            Some("extern")
        } else if sig.variadic.is_some() {
            Some("variadic")
        } else {
            None
        };
        if let Some(kind) = kind {
            return Err(syn::Error::new(
                sig.span(),
                format!("{owner} must not be {kind}"),
            ));
        }
        for attr in attrs {
            let path = attr.path();
            if !path.is_ident("doc")
                && !path.is_ident("inline")
                && !path.is_ident("cold")
                && !path.is_ident("must_use")
                && !path.is_ident("deprecated")
                && !path.is_ident("track_caller")
            {
                return Err(syn::Error::new_spanned(
                    attr,
                    "query functions support only doc, inline, cold, must_use, deprecated, and track_caller attributes",
                ));
            }
        }
        Ok(())
    }

    pub(super) fn build<B: Backend>(
        generics: &syn::Generics,
        inputs: impl Iterator<Item = syn::Result<(syn::Ident, syn::Type)>>,
        block: &syn::Block,
        output: &syn::ReturnType,
    ) -> syn::Result<Self> {
        if !generics.params.is_empty() || generics.where_clause.is_some() {
            return Err(syn::Error::new(
                generics.span(),
                "query methods do not support generics",
            ));
        }

        let mut ids: Vec<syn::Ident> = Vec::new();
        let mut tys: Vec<syn::Type> = Vec::new();
        for input in inputs {
            let (id, ty) = input?;
            ids.push(id);
            tys.push(ty);
        }

        let block = names::lower(block, &ids)?;

        let (ctes, body) = shape::CteBinding::extract_from(&block)?;

        let dialect = B::dialect();
        let mut cte_map: collections::HashMap<String, syn::Path> = collections::HashMap::new();
        let mut captures = Vec::new();
        let mut nonneg = Vec::new();
        let mut prefix: Vec<proc_macro2::TokenStream> = Vec::new();
        if !ctes.is_empty() {
            prefix.push(dialect.kw("WITH_KW"));
            for (i, cte) in ctes.iter().enumerate() {
                if i > 0 {
                    prefix.push(dialect.kw("COMMA"));
                }
                let name_str = cte.name.unraw().to_string().quoted();
                prefix.push(quote::quote! { #name_str });
                prefix.push(dialect.kw("AS_OPEN"));
                let state = plan::State {
                    fn_params: &ids,
                    param_tys: &tys,
                    captures,
                    nonnegative: nonneg,
                    return_type: output,
                    dialect: &dialect,
                    cte_map: &cte_map,
                };
                let step = plan::Cte(state).build(&cte.inner_chain)?;
                let (path, sql) = step.value;
                captures = step.state.captures;
                nonneg = step.state.nonnegative;
                if cte_map.insert(cte.name.unraw().to_string(), path).is_some() {
                    return Err(syn::Error::new(
                        cte.name.span(),
                        format!("duplicate CTE name `{}`", cte.name),
                    ));
                }
                prefix.extend(sql);
                prefix.push(dialect.kw("PAREN_CLOSE"));
            }
            prefix.push(dialect.kw("SPACE"));
        }

        let shape = shape::QueryShape::parse(&body, &cte_map)?;
        let state = plan::State {
            fn_params: &ids,
            param_tys: &tys,
            captures,
            nonnegative: nonneg,
            return_type: output,
            dialect: &dialect,
            cte_map: &cte_map,
        };
        let step = plan::Query(state).build(&shape)?;
        let state = step.state;
        let mut plan = step.value;
        let mut sql = prefix;
        sql.append(&mut plan.sql_parts);
        plan.sql_parts = sql;
        let captures = state.captures;
        let nonneg = state.nonnegative;
        Ok(Self {
            param_ids: ids,
            param_tys: tys,
            captures,
            nonnegative: nonneg,
            plan,
            block,
        })
    }

    pub(super) fn fn_arg(arg: &syn::FnArg) -> syn::Result<(syn::Ident, syn::Type)> {
        let syn::FnArg::Typed(param) = arg else {
            return Err(syn::Error::new(
                arg.span(),
                "query methods do not support `self` parameters",
            ));
        };
        if let Some(attr) = param.attrs.first() {
            return Err(syn::Error::new(
                attr.span(),
                "query method parameter attributes are not supported",
            ));
        }
        let syn::Pat::Ident(ident) = param.pat.as_ref() else {
            return Err(syn::Error::new(
                param.pat.span(),
                "query method params must be plain identifiers",
            ));
        };
        if !ident.attrs.is_empty()
            || ident.by_ref.is_some()
            || ident.mutability.is_some()
            || ident.subpat.is_some()
        {
            return Err(syn::Error::new(
                ident.span(),
                "query method params must be plain identifiers",
            ));
        }
        Ok((ident.ident.clone(), (*param.ty).clone()))
    }
}

pub(super) struct TableField<'a> {
    pub(super) name: &'a syn::Ident,
    pub(super) ty: &'a syn::Type,
    pub(super) vis: &'a syn::Visibility,
}

pub(super) struct TableSpec<'a> {
    pub(super) input: &'a syn::DeriveInput,
    pub(super) table_name: String,
    pub(super) fields: Vec<TableField<'a>>,
}

impl TableSpec<'_> {
    pub(super) fn columns(&self) -> proc_macro2::TokenStream {
        let mut parts = Vec::new();
        let mut select = String::new();
        for (index, field) in self.fields.iter().enumerate() {
            if index > 0 {
                parts.push(quote::quote! { "," });
                select.push(',');
            }
            let column = field.name.unraw().to_string().quoted();
            select.push_str(&column);
            parts.extend([
                quote::quote! { prefix },
                quote::quote! { "." },
                quote::quote! { #column },
            ]);
        }
        let count = parts.len();
        let columns = self.fields.len() as u16;
        quote::quote! {
            #[doc(hidden)]
            pub const __CARTEL_SELECT_COLS: &'static str = #select;
            #[doc(hidden)]
            pub const __CARTEL_N_COLS: u16 = #columns;

            #[doc(hidden)]
            pub const fn __cartel_select_cols(prefix: &str) -> [&str; #count] {
                [#(#parts),*]
            }
        }
    }
}

#[derive(Clone, Copy)]
pub(super) enum Placeholder {
    Dollar,
    Question,
}

#[derive(Clone, Copy)]
pub(super) enum Cast {
    Ltree,
}

#[derive(Clone, Copy)]
pub(super) enum Kind {
    Pg,
    Sqlite,
}

impl Cast {
    fn apply(self, base: String) -> String {
        match self {
            Self::Ltree => format!("{base}::ltree"),
        }
    }
}

impl Kind {
    fn is_rejected(self, name: &str) -> bool {
        match self {
            Self::Pg => name == "glob",
            Self::Sqlite => matches!(
                name,
                "ilike"
                    | "not_ilike"
                    | "pg_contains"
                    | "pg_overlaps"
                    | "regex_match"
                    | "regex_imatch"
                    | "not_regex_match"
                    | "not_regex_imatch"
                    | "descendant_of"
                    | "ancestor_of"
                    | "fts_match"
                    | "in_"
                    | "not_in"
            ),
        }
    }
}

impl Placeholder {
    fn render(self, n: usize) -> String {
        match self {
            Self::Dollar => format!("${n}"),
            Self::Question => format!("?{n}"),
        }
    }
}

pub(super) struct Dialect {
    pub(super) rt_crate: syn::Path,
    pub(super) placeholder: Placeholder,
    pub(super) kind: Kind,
}

pub(super) struct ParamCtx<'a> {
    pub(super) captures: &'a [syn::Ident],
    pub(super) param_ids: &'a [syn::Ident],
    pub(super) param_tys: &'a [syn::Type],
}

impl Dialect {
    pub(super) fn text(&self, parts: proc_macro2::TokenStream) -> proc_macro2::TokenStream {
        let rt = &self.rt_crate;
        // SAFETY: LEN is the checked const sum of the same complete UTF-8 fragments.
        quote::quote! {{
            const PARTS: &[&str] = #parts;
            const LEN: usize = {
                let mut length = 0usize;
                let mut index = 0usize;
                while index < PARTS.len() {
                    length += PARTS[index].len();
                    index += 1;
                }
                length
            };
            const TEXT: #rt::sql::Text<LEN> = unsafe {
                #rt::sql::raw::Parts::new(PARTS).concat::<LEN>()
            };
            TEXT.as_str()
        }}
    }

    pub(super) fn columns(
        &self,
        ty: &syn::Type,
        prefix: proc_macro2::TokenStream,
    ) -> proc_macro2::TokenStream {
        self.text(quote::quote! { &<#ty>::__cartel_select_cols(#prefix) })
    }

    pub(super) fn kw(&self, name: &str) -> proc_macro2::TokenStream {
        let id = quote::format_ident!("{name}");
        let rt = &self.rt_crate;
        quote::quote! { #rt::dsl::sql::#id }
    }

    pub(super) fn placeholder(&self, idx0: usize, ctx: &ParamCtx<'_>) -> proc_macro2::TokenStream {
        let base = self.placeholder.render(idx0 + 1);
        let s = match self.lookup_cast(idx0, ctx) {
            Some(cast) => cast.apply(base),
            None => base,
        };
        quote::quote! { #s }
    }

    fn lookup_cast(&self, idx0: usize, ctx: &ParamCtx<'_>) -> Option<Cast> {
        if let Kind::Sqlite = self.kind {
            return None;
        }
        let cap = ctx.captures.get(idx0)?;
        let pos = ctx
            .param_ids
            .iter()
            .position(|p| p.unraw() == cap.unraw())?;
        let ty = ctx.param_tys.get(pos)?;
        let syn::Type::Path(path) = ty else {
            return None;
        };
        if path.qself.is_some() {
            return None;
        }
        let segment = path.path.segments.last()?;
        (segment.ident == "Ltree" && segment.arguments.is_empty()).then_some(Cast::Ltree)
    }

    pub(super) fn reject_op(&self, name: &str, span: proc_macro2::Span) -> syn::Result<()> {
        if self.kind.is_rejected(name) {
            return Err(syn::Error::new(
                span,
                format!("this backend does not support the `{name}` operator"),
            ));
        }
        Ok(())
    }
}

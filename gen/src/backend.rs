use std::collections::HashMap;

use proc_macro2::Span;
use quote::{format_ident, quote};
use syn::spanned::Spanned;
use syn::{Block, FnArg, Ident, Pat, PatType, Path, ReturnType, Type};

use crate::emit::QueryPlan;
use crate::shape::ChainQualifiers;

pub(super) type ParamCastFn = fn(&Type) -> Option<&'static str>;

pub(super) trait Backend {
    fn dialect() -> Dialect;
    fn emit_table(spec: TableSpec<'_>, dialect: &Dialect) -> syn::Result<proc_macro2::TokenStream>;

    fn derive_table(input: &syn::DeriveInput) -> syn::Result<proc_macro2::TokenStream> {
        let dialect = Self::dialect();
        crate::derive_table::parse(input).and_then(|spec| Self::emit_table(spec, &dialect))
    }
}

pub(super) struct Compiled {
    pub(super) param_ids: Vec<Ident>,
    pub(super) param_tys: Vec<Type>,
    pub(super) captures: Vec<Ident>,
    pub(super) plan: QueryPlan,
    pub(super) block: Block,
    pub(super) output: ReturnType,
}

impl Compiled {
    pub(super) fn build<B: Backend>(
        generics: &syn::Generics,
        inputs: impl Iterator<Item = syn::Result<(Ident, Type)>>,
        block: &Block,
        output: &ReturnType,
        span: Span,
    ) -> syn::Result<Self> {
        if !generics.params.is_empty() {
            return Err(syn::Error::new(
                generics.span(),
                "query methods do not support generics",
            ));
        }

        let mut param_ids: Vec<Ident> = Vec::new();
        let mut param_tys: Vec<Type> = Vec::new();
        for input in inputs {
            let (id, ty) = input?;
            param_ids.push(id);
            param_tys.push(ty);
        }

        let synthetic = syn::ItemFn {
            attrs: Vec::new(),
            vis: syn::Visibility::Inherited,
            sig: syn::Signature {
                constness: None,
                asyncness: None,
                unsafety: None,
                abi: None,
                fn_token: syn::Token![fn](span),
                ident: Ident::new("__cartel_query", span),
                generics: syn::Generics::default(),
                paren_token: syn::token::Paren(span),
                inputs: syn::punctuated::Punctuated::new(),
                variadic: None,
                output: output.clone(),
            },
            block: Box::new(block.clone()),
        };

        let (cte_bindings, body_expr) = crate::shape::CteBinding::extract_from(&synthetic)?;

        let dialect = B::dialect();
        let mut cte_map: HashMap<String, Path> = HashMap::new();
        for cte in &cte_bindings {
            let mut tmp_quals = ChainQualifiers::default();
            let after_quals = tmp_quals.peel(&cte.inner_chain)?;
            let (src, _, _) = crate::shape::QuerySource::parse_chain(&after_quals, &cte_map)?;
            if cte_map
                .insert(cte.name.to_string(), src.primary_path.clone())
                .is_some()
            {
                return Err(syn::Error::new(
                    cte.name.span(),
                    format!("duplicate CTE name `{}`", cte.name),
                ));
            }
        }

        let shape = crate::shape::QueryShape::parse(&body_expr, &cte_map)?;
        let mut captures: Vec<Ident> = Vec::new();

        let mut cte_sql_parts: Vec<proc_macro2::TokenStream> = Vec::new();
        if !cte_bindings.is_empty() {
            cte_sql_parts.push(dialect.kw("WITH_KW"));
            for (i, cte) in cte_bindings.iter().enumerate() {
                if i > 0 {
                    cte_sql_parts.push(dialect.kw("COMMA"));
                }
                let name_str = cte.name.to_string();
                cte_sql_parts.push(quote! { #name_str });
                cte_sql_parts.push(dialect.kw("AS_OPEN"));
                cte_sql_parts.extend(crate::build::PlanBuilder::compile_cte_body(
                    &cte.inner_chain,
                    &param_ids,
                    &param_tys,
                    &mut captures,
                    &dialect,
                    &cte_map,
                )?);
                cte_sql_parts.push(dialect.kw("PAREN_CLOSE"));
            }
            cte_sql_parts.push(dialect.kw("SPACE"));
        }

        let mut plan = shape.build_plan(
            &param_ids,
            &param_tys,
            &mut captures,
            output,
            &dialect,
            &cte_map,
        )?;
        let mut full_sql_parts = cte_sql_parts;
        full_sql_parts.append(&mut plan.sql_parts);
        plan.sql_parts = full_sql_parts;
        Ok(Self {
            param_ids,
            param_tys,
            captures,
            plan,
            block: block.clone(),
            output: output.clone(),
        })
    }

    pub(super) fn fn_arg(arg: &FnArg) -> syn::Result<(Ident, Type)> {
        let FnArg::Typed(PatType { pat, ty, .. }) = arg else {
            return Err(syn::Error::new(
                arg.span(),
                "query methods do not support `self` parameters",
            ));
        };
        let Pat::Ident(pi) = pat.as_ref() else {
            return Err(syn::Error::new(
                pat.span(),
                "query method params must be plain identifiers",
            ));
        };
        Ok((pi.ident.clone(), (**ty).clone()))
    }
}

pub(super) struct TableSpec<'a> {
    pub(super) input: &'a syn::DeriveInput,
    pub(super) table_name: String,
    pub(super) fields: &'a syn::punctuated::Punctuated<syn::Field, syn::Token![,]>,
    pub(super) pk_cols: Vec<String>,
}

#[derive(Clone, Copy)]
pub(super) enum Placeholder {
    Dollar,
    Question,
}

impl Placeholder {
    fn render(self, n_one_based: usize) -> String {
        match self {
            Self::Dollar => format!("${n_one_based}"),
            Self::Question => format!("?{n_one_based}"),
        }
    }
}

pub(super) struct Dialect {
    pub(super) rt_crate: Path,
    pub(super) placeholder: Placeholder,
    pub(super) unsupported_ops: &'static [&'static str],
    pub(super) param_cast: ParamCastFn,
}

pub(super) struct ParamCtx<'a> {
    pub(super) captures: &'a [Ident],
    pub(super) param_ids: &'a [Ident],
    pub(super) param_tys: &'a [Type],
}

impl Dialect {
    pub(super) fn kw(&self, name: &str) -> proc_macro2::TokenStream {
        let id = format_ident!("{name}");
        let rt = &self.rt_crate;
        quote! { #rt::sql::#id }
    }

    pub(super) fn placeholder(&self, idx0: usize, ctx: &ParamCtx<'_>) -> proc_macro2::TokenStream {
        let s = self.placeholder_str(idx0, ctx);
        quote! { #s }
    }

    pub(super) fn placeholder_str(&self, idx0: usize, ctx: &ParamCtx<'_>) -> String {
        let base = self.placeholder.render(idx0 + 1);
        match self.lookup_cast(idx0, ctx) {
            Some(cast) => format!("{base}::{cast}"),
            None => base,
        }
    }

    fn lookup_cast(&self, idx0: usize, ctx: &ParamCtx<'_>) -> Option<&'static str> {
        let cap = ctx.captures.get(idx0)?;
        let pos = ctx.param_ids.iter().position(|p| p == cap)?;
        let ty = ctx.param_tys.get(pos)?;
        (self.param_cast)(ty)
    }

    pub(super) fn reject_op(&self, name: &str, span: Span) -> syn::Result<()> {
        if self.unsupported_ops.contains(&name) {
            return Err(syn::Error::new(
                span,
                format!("this backend does not support the `{name}` operator"),
            ));
        }
        Ok(())
    }
}

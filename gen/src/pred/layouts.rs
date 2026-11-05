use crate::ext::PatExt as _;
use crate::pred;
use crate::pred::{expr, var};
use crate::shape::{self, join};
use std::slice;
use syn::{ext::IdentExt as _, punctuated, spanned::Spanned as _, token};

struct Rows<'a> {
    source: &'a shape::QuerySource,
    idents: &'a [syn::Ident],
}

impl<'a> Rows<'a> {
    fn new(
        source: &'a shape::QuerySource,
        joins: &'a [join::Join],
        idents: &'a [syn::Ident],
    ) -> syn::Result<Self> {
        let (primary, joined) = idents.split_first().ok_or_else(|| {
            syn::Error::new(
                source.primary_path.span(),
                "query source needs a row binding",
            )
        })?;
        if joined.len() != joins.len() {
            return Err(syn::Error::new(
                primary.span(),
                "query row bindings do not match the joined sources",
            ));
        }
        Ok(Self { source, idents })
    }

    fn scope(self) -> pred::RowScope {
        pred::RowScope::for_bindings(self.source, self.idents)
    }
}

impl pred::RowScope {
    pub(crate) fn single(ident: syn::Ident) -> Self {
        Self {
            vars: vec![var::Var {
                ident,
                table_const: None,
            }],
            agg_var: None,
            outer: None,
            alias_depth: None,
            unnest_cols: Vec::new(),
        }
    }

    pub(crate) fn for_source(src: &shape::QuerySource, idents: &[syn::Ident]) -> syn::Result<Self> {
        Ok(Rows::new(src, &src.joins, idents)?.scope())
    }

    pub(crate) fn for_primary(src: &shape::QuerySource, ident: &syn::Ident) -> Self {
        Self::for_bindings(src, slice::from_ref(ident))
    }

    pub(crate) fn for_join_on(src: &shape::QuerySource, index: usize) -> syn::Result<Self> {
        let (j, joins) = src
            .joins
            .get(..=index)
            .and_then(|joins| joins.last().map(|join| (join, joins)))
            .ok_or_else(|| {
                syn::Error::new(
                    src.primary_path.span(),
                    "join scope index exceeds the source",
                )
            })?;
        let idents = &j.on_idents;
        Ok(Rows::new(src, joins, idents)?.scope())
    }

    pub(crate) fn for_order(
        parent: &Self,
        inputs: &punctuated::Punctuated<syn::Pat, token::Comma>,
    ) -> syn::Result<Self> {
        let mut rows = parent.vars.iter();
        let mut vars = Vec::with_capacity(inputs.len());
        for input in inputs {
            let row = rows.next().ok_or_else(|| {
                syn::Error::new(
                    input.span(),
                    "order closure has more parameters than its source scope",
                )
            })?;
            vars.push(var::Var {
                ident: input.ident()?.clone(),
                table_const: row.table_const.clone(),
            });
        }
        if let Some(row) = rows.next() {
            return Err(syn::Error::new(
                row.ident.span(),
                "order closure has fewer parameters than its source scope",
            ));
        }
        Ok(Self {
            vars,
            agg_var: None,
            outer: None,
            alias_depth: parent.alias_depth,
            unnest_cols: Vec::new(),
        })
    }

    pub(crate) fn with_agg(mut self, agg: syn::Ident) -> Self {
        self.agg_var = Some(agg);
        self
    }

    pub(crate) fn with_unnest(mut self, cols: Vec<syn::Ident>) -> Self {
        self.unnest_cols = cols;
        self
    }

    pub(super) fn unnest_ref(&self, expr: &syn::Expr) -> Option<Vec<proc_macro2::TokenStream>> {
        let id = expr::Path::new(expr)?.single()?;
        let pos = self
            .unnest_cols
            .iter()
            .position(|column| column.unraw() == id.unraw())?;
        let col = format!("__d.f{pos}");
        Some(vec![quote::quote! { #col }])
    }

    pub(crate) fn with_outer(mut self, outer: Self) -> Self {
        self.outer = Some(Box::new(outer));
        self
    }

    pub(crate) fn column_ref(&self, expr: &syn::Expr) -> Option<pred::SqlColRef> {
        if let Some(c) = self.local_ref(expr) {
            return Some(c);
        }
        self.outer.as_ref().and_then(|o| o.column_ref(expr))
    }

    fn local_ref(&self, expr: &syn::Expr) -> Option<pred::SqlColRef> {
        let syn::Expr::Field(node) = expr else {
            return None;
        };
        if !node.attrs.is_empty() {
            return None;
        }
        let syn::Member::Named(col) = &node.member else {
            return None;
        };
        let base = expr::Path::new(&node.base)?.single()?;
        let var = self
            .vars
            .iter()
            .find(|var| var.ident.unraw() == base.unraw())?;
        let col_str = col.unraw().to_string();
        match &var.table_const {
            None => Some(pred::SqlColRef::Bare(col_str)),
            Some(tc) => Some(pred::SqlColRef::Qualified(tc.clone(), col_str)),
        }
    }
}

use crate::backend;
use crate::emit;
use crate::parse::assign;
use crate::plan;
use crate::pred;
use crate::pred::layouts;
use crate::shape;
use std::slice;

use syn::spanned::Spanned as _;
use syn::visit::{self, Visit as _};

use layouts::ScopeLayout as _;

use crate::ext::FnParamsExt as _;
use crate::ext::SqlIdent as _;

use crate::plan::read::Filter;
use crate::plan::write::Reply;
use crate::plan::write::reply::Op;

pub(in crate::plan) struct Each<'a> {
    pub(in crate::plan) source: &'a shape::QuerySource,
    pub(in crate::plan) data: &'a [syn::Expr],
    pub(in crate::plan) row: &'a syn::Ident,
    pub(in crate::plan) cols: &'a [syn::Ident],
    pub(in crate::plan) predicate: &'a syn::Expr,
    pub(in crate::plan) body: &'a syn::Expr,
    pub(in crate::plan) returning: shape::Returning,
}

pub(in crate::plan) struct Update<'a>(pub(in crate::plan) plan::State<'a>);

impl<'a> Update<'a> {
    fn table(source: &shape::QuerySource) -> syn::Result<&syn::Path> {
        if let Some(alias) = &source.primary_alias {
            return Err(syn::Error::new(
                alias.span(),
                "UPDATE and DELETE require a table source, not a CTE",
            ));
        }
        Ok(&source.primary_path)
    }

    fn join_ons(
        mut state: plan::State<'a>,
        source: &shape::QuerySource,
    ) -> syn::Result<(plan::State<'a>, Vec<Vec<proc_macro2::TokenStream>>)> {
        let mut joins = Vec::with_capacity(source.joins.len());
        for (index, join) in source.joins.iter().enumerate() {
            let on_scope = pred::RowScope::for_join_on(source, index)?;
            let step = Filter(state).join(&join.cond, &on_scope)?;
            state = step.state;
            joins.push(step.value);
        }
        Ok((state, joins))
    }

    pub(in crate::plan) fn build(
        mut self,
        source: &shape::QuerySource,
        preds: &[syn::Ident],
        pred: &syn::Expr,
        set_id: &syn::Ident,
        set_body: &syn::Expr,
        ret: shape::Returning,
    ) -> syn::Result<plan::Step<'a, emit::Plan>> {
        let table = Self::table(source)?;
        let step = Reply(self.0).validate(Op::Update, ret, table)?;
        self.0 = step.state;
        let output = step.value;
        let lowered;
        let body = if let syn::Expr::Binary(expr) = set_body {
            if let syn::BinOp::AddAssign(op) = &expr.op {
                if let Some(attr) = expr.attrs.first() {
                    return Err(syn::Error::new(
                        attr.span(),
                        "attributes are not supported on a compound assignment",
                    ));
                }
                let left = &expr.left;
                let right = &expr.right;
                lowered = syn::parse_quote_spanned!(op.span()=> #left = #left + #right);
                &lowered
            } else {
                set_body
            }
        } else {
            set_body
        };
        let scope = pred::RowScope::for_primary(source, set_id);
        let assigns = assign::Assign::collect(
            body,
            set_id,
            &scope,
            self.0.fn_params,
            self.0.param_tys,
            self.0.dialect,
            self.0.cte_map,
        )?;
        if assigns.is_empty() {
            return Err(syn::Error::new(
                set_body.span(),
                "`.update(...)` body must assign at least one column",
            ));
        }

        let scope = pred::RowScope::for_source(source, preds)?;

        let (state, joins) = Self::join_ons(self.0, source)?;
        self.0 = state;

        let step = Filter(self.0).build(pred, &scope, false)?;
        self.0 = step.state;
        let filter = step.value;

        let mut sql = vec![self.0.dialect.kw("UPDATE_KW")];
        sql.extend(source.render_base());
        sql.push(self.0.dialect.kw("SET_KW"));
        for (index, assign) in assigns.into_iter().enumerate() {
            if index > 0 {
                sql.push(self.0.dialect.kw("COMMA"));
            }
            let col_eq = format!("{} = ", assign.column.quoted());
            sql.push(quote::quote! { #col_eq });
            sql.extend(self.0.render(assign.value));
        }
        if !source.joins.is_empty() {
            sql.push(self.0.dialect.kw("FROM_KW"));
            for (i, j) in source.joins.iter().enumerate() {
                if i > 0 {
                    sql.push(self.0.dialect.kw("COMMA"));
                }
                sql.extend(source.render_join(i, j));
            }
        }
        sql.push(self.0.dialect.kw("WHERE_KW"));
        for on_parts in &joins {
            sql.extend(on_parts.iter().cloned());
            sql.push(self.0.dialect.kw("AND_KW"));
        }
        sql.extend(filter);

        Ok(Reply(self.0).build(sql, table, source.qualifier(0), output))
    }

    pub(in crate::plan) fn delete(
        mut self,
        source: &shape::QuerySource,
        preds: &[syn::Ident],
        pred: &syn::Expr,
        ret: shape::Returning,
    ) -> syn::Result<plan::Step<'a, emit::Plan>> {
        let table = Self::table(source)?;
        let step = Reply(self.0).validate(Op::Delete, ret, table)?;
        self.0 = step.state;
        let output = step.value;

        let scope = pred::RowScope::for_source(source, preds)?;

        let (state, joins) = Self::join_ons(self.0, source)?;
        self.0 = state;

        let step = Filter(self.0).build(pred, &scope, false)?;
        self.0 = step.state;
        let filter = step.value;

        let mut sql = vec![self.0.dialect.kw("DELETE_FROM_KW")];
        sql.extend(source.render_base());
        if !source.joins.is_empty() {
            sql.push(self.0.dialect.kw("USING_KW"));
            for (i, j) in source.joins.iter().enumerate() {
                if i > 0 {
                    sql.push(self.0.dialect.kw("COMMA"));
                }
                sql.extend(source.render_join(i, j));
            }
        }
        sql.push(self.0.dialect.kw("WHERE_KW"));
        for on_parts in &joins {
            sql.extend(on_parts.iter().cloned());
            sql.push(self.0.dialect.kw("AND_KW"));
        }
        sql.extend(filter);

        Ok(Reply(self.0).build(sql, table, source.qualifier(0), output))
    }

    pub(in crate::plan) fn each(
        mut self,
        input: Each<'_>,
    ) -> syn::Result<plan::Step<'a, emit::Plan>> {
        let Each {
            source,
            data,
            row: row_var,
            cols: col_vars,
            predicate: pred,
            body: set_body,
            returning: ret,
        } = input;
        let table = &source.primary_path;
        let step = Reply(self.0).validate(Op::Update, ret, table)?;
        self.0 = step.state;
        let output = step.value;

        let mut params: Vec<proc_macro2::TokenStream> = Vec::with_capacity(data.len());
        for ds in data {
            let captured = self.0.fn_params.resolve_ref(ds)?;
            let idx = self.0.intern(captured);
            let ctx = backend::ParamCtx {
                captures: &self.0.captures,
                param_ids: self.0.fn_params,
                param_tys: self.0.param_tys,
            };
            params.push(self.0.dialect.placeholder(idx, &ctx));
        }

        let scope = pred::RowScope::for_source(source, slice::from_ref(row_var))?
            .with_unnest(col_vars.to_vec());

        let changes = assign::Assign::collect(
            set_body,
            row_var,
            &scope,
            self.0.fn_params,
            self.0.param_tys,
            self.0.dialect,
            self.0.cte_map,
        )?;
        if changes.is_empty() {
            return Err(syn::Error::new(
                set_body.span(),
                "`.update(...)` body must assign at least one column",
            ));
        }
        let mut assigns: Vec<proc_macro2::TokenStream> = Vec::new();
        for (i, assign) in changes.into_iter().enumerate() {
            let rhs = self.0.render(assign.value);
            if i > 0 {
                assigns.push(self.0.dialect.kw("COMMA"));
            }
            let col_eq = format!("{} = ", assign.column.quoted());
            assigns.push(quote::quote! { #col_eq });
            assigns.extend(rhs);
        }
        let step = Filter(self.0).build(pred, &scope, true)?;
        self.0 = step.state;
        let filter = step.value;

        let mut cols = String::new();
        for i in 0..data.len() {
            if i > 0 {
                cols.push(',');
            }
            cols.push('f');
            cols.push_str(&i.to_string());
        }

        let mut sql = vec![self.0.dialect.kw("UPDATE_KW")];
        sql.extend(source.render_base());
        sql.push(self.0.dialect.kw("SET_KW"));
        sql.extend(assigns);
        sql.push(self.0.dialect.kw("UNNEST_FROM_OPEN"));
        for (i, ph) in params.iter().enumerate() {
            if i > 0 {
                sql.push(self.0.dialect.kw("COMMA_TIGHT"));
            }
            sql.push(ph.clone());
        }
        sql.push(self.0.dialect.kw("UNNEST_ALIAS_OPEN"));
        sql.push(quote::quote! { #cols });
        sql.push(self.0.dialect.kw("PAREN_CLOSE"));
        sql.push(self.0.dialect.kw("WHERE_KW"));
        sql.extend(filter);

        let probe = {
            let scope = backend::scope();
            let rt = &self.0.dialect.rt_crate;
            let mut names = backend::Names::default();
            for name in self
                .0
                .fn_params
                .iter()
                .chain(col_vars)
                .chain(slice::from_ref(row_var))
            {
                names.visit_ident(name);
            }
            names.visit_expr(pred);
            names.visit_expr(set_body);
            let inputs: Vec<_> = data.iter().map(|_| names.fresh("column")).collect();
            let when: Vec<_> = col_vars
                .iter()
                .map(|name| Binding::pattern(name, pred))
                .collect();
            let set: Vec<_> = col_vars
                .iter()
                .map(|name| Binding::pattern(name, set_body))
                .collect();
            syn::parse_quote! {
                {
                    let (#(#inputs,)*) = (#(#data,)*);
                    #rt::dsl::query::ops::Change::update(
                        <#table>::filter(
                            #scope,
                            |#row_var: #table| -> bool {
                                #(let #when = #scope.element(&#inputs);)*
                                #pred
                            },
                        ),
                        |#row_var: &mut #table| {
                            #(let #set = #scope.element(&#inputs);)*
                            #set_body
                        },
                    )
                }
            }
        };

        let mut step = Reply(self.0).build(sql, table, source.qualifier(0), output);
        step.value.probe_override = Some(probe);
        Ok(step)
    }
}

struct Binding<'a> {
    name: &'a syn::Ident,
    used: bool,
}

impl Binding<'_> {
    fn pattern(name: &syn::Ident, body: &syn::Expr) -> proc_macro2::TokenStream {
        let mut binding = Binding { name, used: false };
        binding.visit_expr(body);
        if binding.used {
            quote::quote! { #name }
        } else {
            quote::quote! { _ }
        }
    }
}

impl<'ast> visit::Visit<'ast> for Binding<'_> {
    fn visit_expr_path(&mut self, path: &'ast syn::ExprPath) {
        self.used |= path.qself.is_none() && path.path.is_ident(self.name);
        visit::visit_expr_path(self, path);
    }
}

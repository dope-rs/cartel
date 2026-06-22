use syn::spanned::Spanned;
use syn::{Attribute, Expr, ExprClosure, ExprField, ExprPath, Ident, Member, Pat, Path, Stmt};

pub(super) trait PathExt {
    fn without_last(&self) -> syn::Result<Path>;
}

impl PathExt for Path {
    fn without_last(&self) -> syn::Result<Path> {
        let mut t = self.clone();
        t.segments.pop();
        if let Some(pair) = t.segments.pop() {
            t.segments.push(pair.into_value());
        }
        if t.segments.is_empty() {
            return Err(syn::Error::new(
                self.span(),
                "expected `Table::<method>`; missing Table path before `::<method>`",
            ));
        }
        Ok(t)
    }
}

pub(super) trait ExprExt {
    fn as_column_ref(&self, row_var: &Ident) -> Option<String>;
    fn is_synthetic_true(&self) -> bool;
    fn normalized_closure_body(&self) -> Expr;
    fn as_closure_form(
        &self,
        expected_n: Option<usize>,
        method_name: &str,
    ) -> syn::Result<(Vec<Ident>, Expr)>;
    fn as_closure_n(&self, n: usize, method_name: &str) -> syn::Result<(Vec<Ident>, Expr)>;
    fn as_closure_any_arity(&self, method_name: &str) -> syn::Result<(Vec<Ident>, Expr)>;
    fn as_closure_single(&self, method_name: &str) -> syn::Result<(Ident, Expr)>;
}

impl ExprExt for Expr {
    fn as_column_ref(&self, row_var: &Ident) -> Option<String> {
        let Expr::Field(ExprField { base, member, .. }) = self else {
            return None;
        };
        let Expr::Path(ExprPath { path, .. }) = base.as_ref() else {
            return None;
        };
        if path.segments.len() != 1 {
            return None;
        }
        if path.segments[0].ident != *row_var {
            return None;
        }
        let Member::Named(col) = member else {
            return None;
        };
        Some(col.to_string())
    }

    fn is_synthetic_true(&self) -> bool {
        matches!(
            self,
            Expr::Lit(syn::ExprLit {
                lit: syn::Lit::Bool(b),
                ..
            }) if b.value
        )
    }

    fn normalized_closure_body(&self) -> Expr {
        if let Expr::Block(eb) = self
            && eb.block.stmts.len() == 1
            && let Stmt::Expr(inner, None) = &eb.block.stmts[0]
        {
            return inner.clone();
        }
        self.clone()
    }

    fn as_closure_form(
        &self,
        expected_n: Option<usize>,
        method_name: &str,
    ) -> syn::Result<(Vec<Ident>, Expr)> {
        let Expr::Closure(ExprClosure { inputs, body, .. }) = self else {
            return Err(syn::Error::new(
                self.span(),
                format!("`{method_name}` argument must be a closure"),
            ));
        };
        if let Some(n) = expected_n
            && inputs.len() != n
        {
            return Err(syn::Error::new(
                inputs.span(),
                format!("`{method_name}` closure must take exactly {n} parameter(s)"),
            ));
        }
        let mut idents = Vec::with_capacity(inputs.len());
        for inp in inputs {
            idents.push(inp.closure_ident()?);
        }
        Ok((idents, body.normalized_closure_body()))
    }

    fn as_closure_n(&self, n: usize, method_name: &str) -> syn::Result<(Vec<Ident>, Expr)> {
        self.as_closure_form(Some(n), method_name)
    }

    fn as_closure_any_arity(&self, method_name: &str) -> syn::Result<(Vec<Ident>, Expr)> {
        self.as_closure_form(None, method_name)
    }

    fn as_closure_single(&self, method_name: &str) -> syn::Result<(Ident, Expr)> {
        let (mut idents, body) = self.as_closure_form(Some(1), method_name)?;
        Ok((idents.remove(0), body))
    }
}

pub(super) trait FnParamsExt {
    fn resolve(&self, expr: &Expr) -> syn::Result<Ident>;
    fn resolve_borrowed(&self, expr: &Expr) -> syn::Result<Ident>;
    fn index_of(&self, target: &Ident) -> syn::Result<usize>;
}

impl FnParamsExt for [Ident] {
    fn index_of(&self, target: &Ident) -> syn::Result<usize> {
        self.iter()
            .position(|p| p == target)
            .ok_or_else(|| syn::Error::new(target.span(), "captured ident not in fn params"))
    }

    fn resolve(&self, expr: &Expr) -> syn::Result<Ident> {
        let Expr::Path(ExprPath { path, .. }) = expr else {
            return Err(syn::Error::new(
                expr.span(),
                "#[query] v0 captured value must be a single fn parameter ident",
            ));
        };
        if path.segments.len() != 1 {
            return Err(syn::Error::new(
                path.span(),
                "captured value must be a single ident",
            ));
        }
        let id = &path.segments[0].ident;
        if !self.iter().any(|p| p == id) {
            return Err(syn::Error::new(
                id.span(),
                format!(
                    "`{id}` is not a function parameter — only fn params can be captured in v0"
                ),
            ));
        }
        Ok(id.clone())
    }

    fn resolve_borrowed(&self, expr: &Expr) -> syn::Result<Ident> {
        let inner = match expr {
            Expr::Reference(r) => r.expr.as_ref(),
            other => other,
        };
        self.resolve(inner)
    }
}

pub(super) trait CaptureSet {
    fn intern(&mut self, cap: Ident) -> usize;
}

impl CaptureSet for Vec<Ident> {
    fn intern(&mut self, cap: Ident) -> usize {
        if let Some(i) = self.iter().position(|c| c == &cap) {
            return i;
        }
        self.push(cap);
        self.len() - 1
    }
}

pub(super) trait PatExt {
    fn closure_ident(&self) -> syn::Result<Ident>;
}

impl PatExt for Pat {
    fn closure_ident(&self) -> syn::Result<Ident> {
        match self {
            Pat::Ident(pi) => Ok(pi.ident.clone()),
            Pat::Type(syn::PatType { pat, .. }) => match pat.as_ref() {
                Pat::Ident(pi) => Ok(pi.ident.clone()),
                other => Err(syn::Error::new(
                    other.span(),
                    "closure param must be a plain identifier",
                )),
            },
            other => Err(syn::Error::new(
                other.span(),
                "closure param must be a plain identifier",
            )),
        }
    }
}

pub(super) trait AttrSliceExt {
    fn has_pk(&self) -> bool;
    fn table_name(&self, struct_ident: &Ident) -> syn::Result<String>;
}

impl AttrSliceExt for [Attribute] {
    fn has_pk(&self) -> bool {
        self.iter().any(|a| a.path().is_ident("pk"))
    }

    fn table_name(&self, struct_ident: &Ident) -> syn::Result<String> {
        for a in self {
            if a.path().is_ident("table_name") {
                let s: syn::LitStr = a.parse_args()?;
                return Ok(s.value());
            }
        }
        let mut out = String::new();
        let s = struct_ident.to_string();
        for (i, ch) in s.chars().enumerate() {
            if ch.is_uppercase() && i > 0 {
                out.push('_');
            }
            out.extend(ch.to_lowercase());
        }
        if !out.ends_with('s') {
            out.push('s');
        }
        Ok(out)
    }
}

pub(super) trait SqlIdent {
    fn quote_if_needed(&self) -> String;
}

impl SqlIdent for str {
    fn quote_if_needed(&self) -> String {
        if self.contains('.') {
            return self
                .split('.')
                .map(|p| p.quote_if_needed())
                .collect::<Vec<_>>()
                .join(".");
        }
        let needs_quote = self.is_empty()
            || self
                .chars()
                .next()
                .map(|c| c.is_ascii_digit())
                .unwrap_or(true)
            || self
                .chars()
                .any(|c| !(c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_'));
        if needs_quote {
            format!("\"{}\"", self.replace('"', "\"\""))
        } else {
            self.to_string()
        }
    }
}

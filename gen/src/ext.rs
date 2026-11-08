use syn::{ext::IdentExt as _, spanned::Spanned as _};

pub(super) fn unraw(name: &str) -> &str {
    name.strip_prefix("r#").unwrap_or(name)
}

pub(super) fn name(ident: &syn::Ident) -> String {
    let mut name = ident.to_string();
    let prefix = name.len() - unraw(&name).len();
    if prefix != 0 {
        name.replace_range(..prefix, "");
    }
    name
}

pub(super) trait PathExt {
    fn without_last(&self) -> syn::Result<syn::Path>;
}

impl PathExt for syn::Path {
    fn without_last(&self) -> syn::Result<syn::Path> {
        let mut t = self.clone();
        if t.segments.pop().is_none() || t.segments.is_empty() {
            return Err(syn::Error::new(
                self.span(),
                "expected `Table::<method>`; missing Table path before `::<method>`",
            ));
        }
        t.segments.pop_punct();
        Ok(t)
    }
}

pub(super) trait ExprExt {
    fn column(&self, row_var: &syn::Ident) -> Option<String>;
    fn is_true(&self) -> bool;
    fn unbrace(&self) -> &syn::Expr;
    fn closure<'a>(&'a self, arity: Option<usize>, method: &str) -> syn::Result<Closure<'a>>;
    fn closure_n(&self, n: usize, method: &str) -> syn::Result<(Vec<syn::Ident>, syn::Expr)>;
    fn closure_args(&self, method: &str) -> syn::Result<(Vec<syn::Ident>, syn::Expr)>;
    fn closure_one(&self, method: &str) -> syn::Result<(&syn::Ident, &syn::Expr)>;
    fn closure_same(&self, names: &[syn::Ident], method: &str) -> syn::Result<syn::Expr>;
}

pub(super) struct Closure<'a>(&'a syn::ExprClosure);

impl Closure<'_> {
    fn parts(self) -> syn::Result<(Vec<syn::Ident>, syn::Expr)> {
        let mut idents = Vec::with_capacity(self.0.inputs.len());
        for input in &self.0.inputs {
            idents.push(input.ident()?.clone());
        }
        let body = self.0.body.unbrace().clone();
        Ok((idents, body))
    }
}

impl ExprExt for syn::Expr {
    fn column(&self, row_var: &syn::Ident) -> Option<String> {
        let syn::Expr::Field(field) = self else {
            return None;
        };
        if !field.attrs.is_empty() {
            return None;
        }
        let syn::Expr::Path(path) = field.base.as_ref() else {
            return None;
        };
        if !path.attrs.is_empty() || path.qself.is_some() {
            return None;
        }
        let row = path.path.get_ident()?;
        if row.unraw() != row_var.unraw() {
            return None;
        }
        let syn::Member::Named(col) = &field.member else {
            return None;
        };
        Some(name(col))
    }

    fn is_true(&self) -> bool {
        let syn::Expr::Lit(literal) = self else {
            return false;
        };
        if !literal.attrs.is_empty() {
            return false;
        }
        let syn::Lit::Bool(value) = &literal.lit else {
            return false;
        };
        value.value
    }

    fn unbrace(&self) -> &syn::Expr {
        if let syn::Expr::Block(block) = self
            && block.attrs.is_empty()
            && block.label.is_none()
            && block.block.stmts.len() == 1
            && let syn::Stmt::Expr(inner, None) = &block.block.stmts[0]
        {
            return inner.unbrace();
        }
        self
    }

    fn closure<'a>(&'a self, arity: Option<usize>, method: &str) -> syn::Result<Closure<'a>> {
        let syn::Expr::Closure(closure) = self else {
            return Err(syn::Error::new(
                self.span(),
                format!("`{method}` argument must be a closure"),
            ));
        };
        if let Some(attr) = closure.attrs.first() {
            return Err(syn::Error::new(
                attr.span(),
                format!("`{method}` closure attributes are not supported"),
            ));
        }
        if let Some(bounds) = &closure.lifetimes {
            return Err(syn::Error::new(
                bounds.span(),
                format!("`{method}` closure lifetime binders are not supported"),
            ));
        }
        if let Some(token) = closure.constness {
            return Err(syn::Error::new(
                token.span,
                format!("`{method}` closure cannot be const"),
            ));
        }
        if let Some(token) = closure.asyncness {
            return Err(syn::Error::new(
                token.span,
                format!("`{method}` closure cannot be async"),
            ));
        }
        if let Some(capture) = closure.capture {
            return Err(syn::Error::new(
                capture.span,
                format!("`{method}` closure cannot be move"),
            ));
        }
        let syn::ReturnType::Default = &closure.output else {
            return Err(syn::Error::new(
                closure.output.span(),
                format!("`{method}` closure cannot declare a return type"),
            ));
        };
        let inputs = &closure.inputs;
        if let Some(n) = arity
            && inputs.len() != n
        {
            return Err(syn::Error::new(
                inputs.span(),
                format!("`{method}` closure must take exactly {n} parameter(s)"),
            ));
        }
        Ok(Closure(closure))
    }

    fn closure_n(&self, n: usize, method: &str) -> syn::Result<(Vec<syn::Ident>, syn::Expr)> {
        let closure = self.closure(Some(n), method)?;
        closure.parts()
    }

    fn closure_args(&self, method: &str) -> syn::Result<(Vec<syn::Ident>, syn::Expr)> {
        let closure = self.closure(None, method)?;
        closure.parts()
    }

    fn closure_one(&self, method: &str) -> syn::Result<(&syn::Ident, &syn::Expr)> {
        let closure = self.closure(None, method)?;
        let mut inputs = closure.0.inputs.iter();
        let (Some(input), None) = (inputs.next(), inputs.next()) else {
            return Err(syn::Error::new(
                closure.0.inputs.span(),
                format!("`{method}` closure must take exactly 1 parameter(s)"),
            ));
        };
        Ok((input.ident()?, closure.0.body.unbrace()))
    }

    fn closure_same(&self, names: &[syn::Ident], method: &str) -> syn::Result<syn::Expr> {
        let closure = self.closure(Some(names.len()), method)?;
        let mut mismatch = None;
        for (input, name) in closure.0.inputs.iter().zip(names) {
            if input.ident()?.unraw() != name.unraw() && mismatch.is_none() {
                mismatch = Some(input.span());
            }
        }
        if let Some(span) = mismatch {
            return Err(syn::Error::new(
                span,
                "`filter_each` and `.update` closures must use the same parameter names",
            ));
        }
        Ok(closure.0.body.unbrace().clone())
    }
}

pub(super) trait FnParamsExt {
    fn resolve(&self, expr: &syn::Expr) -> syn::Result<crate::backend::Capture>;
    fn resolve_ref(&self, expr: &syn::Expr) -> syn::Result<crate::backend::Capture>;
}

impl FnParamsExt for [crate::backend::Param] {
    fn resolve(&self, expr: &syn::Expr) -> syn::Result<crate::backend::Capture> {
        let syn::Expr::Path(path) = expr else {
            return Err(syn::Error::new(
                expr.span(),
                "#[query] v0 captured value must be a single fn parameter ident",
            ));
        };
        if let Some(attr) = path.attrs.first() {
            return Err(syn::Error::new(
                attr.span(),
                "captured value must be a single ident",
            ));
        }
        if let Some(qself) = &path.qself {
            return Err(syn::Error::new(
                qself.span(),
                "captured value must be a single ident",
            ));
        }
        let Some(id) = path.path.get_ident() else {
            return Err(syn::Error::new(
                path.path.span(),
                "captured value must be a single ident",
            ));
        };
        let Some(index) = self.iter().position(|(p, _)| p.unraw() == id.unraw()) else {
            return Err(syn::Error::new(
                id.span(),
                format!(
                    "`{id}` is not a function parameter — only fn params can be captured in v0"
                ),
            ));
        };
        Ok((index, id.span()))
    }

    fn resolve_ref(&self, expr: &syn::Expr) -> syn::Result<crate::backend::Capture> {
        let inner = match expr {
            syn::Expr::Reference(r) => r.expr.as_ref(),
            other => other,
        };
        self.resolve(inner)
    }
}

pub(super) trait PatExt {
    fn ident(&self) -> syn::Result<&syn::Ident>;
}

impl PatExt for syn::Pat {
    fn ident(&self) -> syn::Result<&syn::Ident> {
        let syn::Pat::Ident(ident) = self else {
            return Err(syn::Error::new(
                self.span(),
                "closure param must be a plain identifier",
            ));
        };
        if !ident.attrs.is_empty()
            || ident.by_ref.is_some()
            || ident.mutability.is_some()
            || ident.subpat.is_some()
        {
            return Err(syn::Error::new(
                ident.span(),
                "closure param must be a plain identifier",
            ));
        }
        Ok(&ident.ident)
    }
}

pub(super) trait SqlIdent {
    fn quoted(&self) -> String;
}

impl SqlIdent for str {
    fn quoted(&self) -> String {
        let extra: usize = self
            .bytes()
            .map(|byte| match byte {
                b'"' => 1,
                b'.' => 2,
                _ => 0,
            })
            .sum();
        let mut quoted = String::with_capacity(self.len() + extra + 2);
        quoted.push('"');
        let mut start = 0;
        for (end, byte) in self
            .bytes()
            .enumerate()
            .filter(|(_, byte)| matches!(*byte, b'"' | b'.'))
        {
            quoted.push_str(&self[start..end]);
            quoted.push_str(if byte == b'.' { "\".\"" } else { "\"\"" });
            start = end + 1;
        }
        quoted.push_str(&self[start..]);
        quoted.push('"');
        quoted
    }
}

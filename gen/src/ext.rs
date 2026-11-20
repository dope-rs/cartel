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
    fn table(&self) -> syn::Result<syn::Path>;
}

impl PathExt for syn::Path {
    fn table(&self) -> syn::Result<syn::Path> {
        if let Some(last) = self.segments.last()
            && !last.arguments.is_none()
        {
            return Err(syn::Error::new_spanned(
                &last.arguments,
                "this table operation infers its argument types",
            ));
        }
        self.without_last()
    }

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

pub(super) fn inferred(call: &syn::ExprMethodCall) -> syn::Result<()> {
    match &call.turbofish {
        Some(args) => Err(syn::Error::new_spanned(
            args,
            "this operation infers its argument types",
        )),
        None => Ok(()),
    }
}

pub(super) trait ExprExt {
    fn column(&self, row_var: &syn::Ident) -> Option<&syn::Ident>;
    fn is_true(&self) -> bool;
    fn unbrace(&self) -> &syn::Expr;
    fn closure(&self, arity: Option<usize>, method: &str) -> syn::Result<&syn::ExprClosure>;
    fn binding(&self, arity: Option<usize>, method: &str) -> syn::Result<Binding<'_>>;
    fn closure_one(&self, method: &str) -> syn::Result<(&syn::Ident, &syn::Expr)>;
    fn closure_same(&self, names: &[syn::Ident], method: &str) -> syn::Result<&syn::Expr>;
}

pub(super) struct Binding<'a> {
    pub(super) idents: Vec<syn::Ident>,
    pub(super) body: &'a syn::Expr,
}

impl ExprExt for syn::Expr {
    fn column(&self, row_var: &syn::Ident) -> Option<&syn::Ident> {
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
        Some(col)
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

    fn closure(&self, arity: Option<usize>, method: &str) -> syn::Result<&syn::ExprClosure> {
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
        Ok(closure)
    }

    fn binding(&self, arity: Option<usize>, method: &str) -> syn::Result<Binding<'_>> {
        let closure = self.closure(arity, method)?;
        let mut idents = Vec::with_capacity(closure.inputs.len());
        for input in &closure.inputs {
            let id = input.ident()?;
            if idents
                .iter()
                .any(|prev: &syn::Ident| prev.unraw() == id.unraw())
            {
                return Err(syn::Error::new(id.span(), "duplicate row binding"));
            }
            idents.push(id.clone());
        }
        Ok(Binding {
            idents,
            body: closure.body.unbrace(),
        })
    }

    fn closure_one(&self, method: &str) -> syn::Result<(&syn::Ident, &syn::Expr)> {
        let closure = self.closure(None, method)?;
        let mut inputs = closure.inputs.iter();
        let (Some(input), None) = (inputs.next(), inputs.next()) else {
            return Err(syn::Error::new(
                closure.inputs.span(),
                format!("`{method}` closure must take exactly 1 parameter(s)"),
            ));
        };
        Ok((input.ident()?, closure.body.unbrace()))
    }

    fn closure_same(&self, names: &[syn::Ident], method: &str) -> syn::Result<&syn::Expr> {
        let closure = self.closure(Some(names.len()), method)?;
        let mut mismatch = None;
        for (input, name) in closure.inputs.iter().zip(names) {
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
        Ok(closure.body.unbrace())
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

use crate::backend;
use crate::pred::sql;

pub(super) enum Literal<'a> {
    Int(&'a syn::LitInt),
    Float(&'a syn::LitFloat),
    Bool(&'a syn::LitBool),
    String(&'a syn::LitStr),
}

impl<'a> Literal<'a> {
    pub(super) fn new(expr: &'a syn::Expr) -> Option<Self> {
        let syn::Expr::Lit(syntax) = expr else {
            return None;
        };
        if !syntax.attrs.is_empty() {
            return None;
        }
        if let syn::Lit::Int(literal) = &syntax.lit {
            return Some(Self::Int(literal));
        }
        if let syn::Lit::Float(literal) = &syntax.lit {
            return Some(Self::Float(literal));
        }
        if let syn::Lit::Bool(literal) = &syntax.lit {
            return Some(Self::Bool(literal));
        }
        if let syn::Lit::Str(literal) = &syntax.lit {
            return Some(Self::String(literal));
        }
        None
    }

    pub(super) fn render(self, dialect: &backend::Dialect) -> sql::Sql {
        match self {
            Self::Int(literal) => {
                let value = literal.base10_digits().to_string();
                sql::Sql::tokens(vec![quote::quote! { #value }])
            }
            Self::Float(literal) => {
                let value = literal.base10_digits().to_string();
                sql::Sql::tokens(vec![quote::quote! { #value }])
            }
            Self::Bool(literal) => {
                let keyword = if literal.value { "TRUE" } else { "FALSE" };
                sql::Sql::tokens(vec![dialect.kw(keyword)])
            }
            Self::String(literal) => {
                let value = format!("'{}'", literal.value().replace('\'', "''"));
                sql::Sql::tokens(vec![quote::quote! { #value }])
            }
        }
    }
}

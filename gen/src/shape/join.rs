use crate::{
    backend,
    ext::{ExprExt as _, PathExt as _},
};
use syn::spanned::Spanned as _;

pub(crate) struct Join {
    pub(crate) kind: Kind,
    pub(crate) path: syn::Path,
    pub(crate) on_idents: Vec<syn::Ident>,
    pub(crate) cond: syn::Expr,
}

#[derive(Clone, Copy)]
pub(crate) enum Kind {
    Inner,
    Left,
    Right,
    Full,
    LateralInner,
    LateralLeft,
}

impl Kind {
    fn new(name: &str) -> Option<Self> {
        Some(match name {
            "join" => Self::Inner,
            "left_join" => Self::Left,
            "right_join" => Self::Right,
            "full_join" => Self::Full,
            "lateral_join" => Self::LateralInner,
            "lateral_left_join" => Self::LateralLeft,
            _ => return None,
        })
    }

    pub(crate) fn sql_ref(self, dialect: &backend::Dialect) -> proc_macro2::TokenStream {
        match self {
            Self::Inner => dialect.kw("INNER_JOIN"),
            Self::Left => dialect.kw("LEFT_OUTER_JOIN"),
            Self::Right => dialect.kw("RIGHT_OUTER_JOIN"),
            Self::Full => dialect.kw("FULL_OUTER_JOIN"),
            Self::LateralInner => dialect.kw("INNER_JOIN_LATERAL"),
            Self::LateralLeft => dialect.kw("LEFT_JOIN_LATERAL"),
        }
    }

    pub(crate) fn is_lateral(self) -> bool {
        matches!(self, Self::LateralInner | Self::LateralLeft)
    }

    pub(crate) fn from_method(method: &syn::Ident) -> syn::Result<Self> {
        let name = method.to_string();
        Self::new(&name).ok_or_else(|| {
            syn::Error::new(
                method.span(),
                format!(
                    "expected `.join` / `.left_join` / `.right_join` / `.full_join` / `.lateral_join` / `.lateral_left_join`, got `.{name}`",
                ),
            )
        })
    }
}

impl Join {
    pub(crate) fn parse_type(mc: &syn::ExprMethodCall) -> syn::Result<syn::Path> {
        let Some(generic) = &mc.turbofish else {
            return Err(syn::Error::new(
                mc.method.span(),
                "join must specify the joined table via turbofish: `.join::<Table>(...)`",
            ));
        };
        Self::parse_table(generic)
    }

    pub(crate) fn parse_call(
        call: &syn::ExprCall,
    ) -> syn::Result<(syn::Path, syn::Path, Kind, Vec<syn::Ident>, syn::Expr)> {
        let syn::Expr::Path(function) = call.func.as_ref() else {
            return Err(syn::Error::new(
                call.func.span(),
                "expected `Table1::<join_kind>::<Table2>(...)` path",
            ));
        };
        if let Some(attr) = function.attrs.first() {
            return Err(syn::Error::new(
                attr.span(),
                "attributes are not supported on a join path",
            ));
        }
        if let Some(qself) = &function.qself {
            return Err(syn::Error::new(
                qself.span(),
                "a join must be called through an unqualified Table path",
            ));
        }
        let path = &function.path;
        let last = path
            .segments
            .last()
            .ok_or_else(|| syn::Error::new(path.span(), "empty path"))?;
        let method = last.ident.to_string();
        let kind = Kind::new(&method).ok_or_else(|| {
            syn::Error::new(
                last.ident.span(),
                format!(
                    "expected `::join` / `::left_join` / `::right_join` / `::full_join` / `::lateral_join` / `::lateral_left_join`, got `::{method}`"
                ),
            )
        })?;
        let syn::PathArguments::AngleBracketed(generic) = &last.arguments else {
            return Err(syn::Error::new(
                last.span(),
                "join must specify the joined table via turbofish: `Table1::join::<Table2>(...)`",
            ));
        };
        let joined = Self::parse_table(generic)?;
        let primary = path.without_last()?;
        if call.args.len() != 1 {
            return Err(syn::Error::new(
                call.args.span(),
                "`join` takes exactly one closure argument",
            ));
        }
        let n = if kind.is_lateral() { 1 } else { 2 };
        let (idents, body) = call.args[0].closure_n(n, &method)?;
        Ok((primary, joined, kind, idents, body))
    }

    fn parse_table(generic: &syn::AngleBracketedGenericArguments) -> syn::Result<syn::Path> {
        if generic.args.len() != 1 {
            return Err(syn::Error::new(
                generic.span(),
                "`.join` turbofish must have exactly one type argument",
            ));
        }
        let syn::GenericArgument::Type(ty) = &generic.args[0] else {
            return Err(syn::Error::new(
                generic.args[0].span(),
                "`.join` turbofish argument must be a type",
            ));
        };
        let syn::Type::Path(table) = ty else {
            return Err(syn::Error::new(
                ty.span(),
                "`.join` turbofish argument must be a plain Table path",
            ));
        };
        if let Some(qself) = &table.qself {
            return Err(syn::Error::new(
                qself.span(),
                "`.join` turbofish argument cannot use a qualified-self type",
            ));
        }
        Ok(table.path.clone())
    }
}

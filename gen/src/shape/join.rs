use crate::emit::token::fragment;
use crate::ext::{Binding, ExprExt as _, PathExt as _};
use syn::spanned::Spanned as _;

pub(crate) struct Join<'a> {
    pub(crate) kind: Kind,
    pub(crate) path: &'a syn::Path,
    pub(crate) on: Binding<'a>,
}

#[derive(Clone, Copy)]
pub(crate) enum Kind {
    Inner,
    Left,
    LateralInner,
    LateralLeft,
}

impl Kind {
    fn new(name: &str) -> Option<Self> {
        Some(match name {
            "join" => Self::Inner,
            "left_join" => Self::Left,
            "lateral_join" => Self::LateralInner,
            "lateral_left_join" => Self::LateralLeft,
            _ => return None,
        })
    }

    pub(crate) fn sql_ref(self) -> proc_macro2::TokenStream {
        fragment(match self {
            Self::Inner => " INNER JOIN ",
            Self::Left => " LEFT OUTER JOIN ",
            Self::LateralInner => " INNER JOIN LATERAL ",
            Self::LateralLeft => " LEFT JOIN LATERAL ",
        })
    }

    pub(crate) fn is_lateral(self) -> bool {
        matches!(self, Self::LateralInner | Self::LateralLeft)
    }
}

impl<'a> Join<'a> {
    pub(crate) fn parse_type(mc: &'a syn::ExprMethodCall) -> syn::Result<&'a syn::Path> {
        let Some(generic) = &mc.turbofish else {
            return Err(syn::Error::new(
                mc.method.span(),
                "join must specify the joined table via turbofish: `.join::<Table>(...)`",
            ));
        };
        Self::parse_table(generic)
    }

    pub(crate) fn parse_call(call: &'a syn::ExprCall) -> syn::Result<(syn::Path, Self)> {
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
                    "expected `::join` / `::left_join` / `::lateral_join` / `::lateral_left_join`, got `::{method}`"
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
        let on = call.args[0].binding(Some(n), &method)?;
        Ok((
            primary,
            Self {
                kind,
                path: joined,
                on,
            },
        ))
    }

    fn parse_table(generic: &'a syn::AngleBracketedGenericArguments) -> syn::Result<&'a syn::Path> {
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
        Ok(&table.path)
    }
}

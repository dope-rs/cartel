pub(super) enum Tag {
    Scalar(Scalar),
    Bytes,
    RowOnly,
    Borrowed,
    Unsupported,
}

pub(super) enum Scalar {
    Bool,
    I16,
    I32,
    I64,
    F32,
    F64,
    String,
    Text,
    Jsonb,
    Uuid,
    Timestamp,
    Date,
    Ltree,
}

impl Scalar {
    fn from_ident(ident: &syn::Ident) -> Option<Self> {
        Some(if ident == "bool" {
            Self::Bool
        } else if ident == "i16" {
            Self::I16
        } else if ident == "i32" {
            Self::I32
        } else if ident == "i64" {
            Self::I64
        } else if ident == "f32" {
            Self::F32
        } else if ident == "f64" {
            Self::F64
        } else if ident == "String" {
            Self::String
        } else if ident == "Text" {
            Self::Text
        } else if ident == "Jsonb" {
            Self::Jsonb
        } else if ident == "Uuid" {
            Self::Uuid
        } else if ident == "Timestamp" {
            Self::Timestamp
        } else if ident == "Date" {
            Self::Date
        } else if ident == "Ltree" {
            Self::Ltree
        } else {
            return None;
        })
    }
}

impl Tag {
    pub(super) fn simple(syntax: &syn::Type) -> Option<&syn::Ident> {
        let syn::Type::Path(path) = syntax else {
            return None;
        };
        if path.qself.is_some()
            || path
                .path
                .segments
                .iter()
                .any(|seg| !matches!(&seg.arguments, syn::PathArguments::None))
        {
            return None;
        }
        path.path.segments.last().map(|last| &last.ident)
    }

    pub(super) fn plain(ident: &syn::Ident) -> Self {
        Scalar::from_ident(ident).map_or(Self::Unsupported, Self::Scalar)
    }

    pub(super) fn vector(ident: &syn::Ident) -> Self {
        if ident == "u8" {
            Self::Bytes
        } else if ident == "i32" || ident == "i64" || ident == "String" {
            Self::RowOnly
        } else {
            Self::Unsupported
        }
    }

    pub(super) fn optional(ident: &syn::Ident) -> Self {
        match Scalar::from_ident(ident) {
            Some(_) => Self::RowOnly,
            None => Self::Unsupported,
        }
    }

    pub(super) fn slice(syntax: &syn::Type) -> Self {
        let supported = match syntax {
            syn::Type::Reference(rf) => Self::simple(&rf.elem).is_some_and(|id| id == "str"),
            _ => Self::simple(syntax).is_some_and(|id| {
                id == "u8"
                    || id == "i16"
                    || id == "i32"
                    || id == "i64"
                    || id == "f32"
                    || id == "f64"
                    || id == "bool"
            }),
        };
        if supported {
            Self::Borrowed
        } else {
            Self::Unsupported
        }
    }

    pub(super) fn slice_type(self) -> Option<proc_macro2::TokenStream> {
        match self {
            Self::Scalar(Scalar::Bool) => Some(quote::quote! { &'data [bool] }),
            Self::Scalar(Scalar::I16) => Some(quote::quote! { &'data [i16] }),
            Self::Scalar(Scalar::I32) => Some(quote::quote! { &'data [i32] }),
            Self::Scalar(Scalar::I64) => Some(quote::quote! { &'data [i64] }),
            Self::Scalar(Scalar::F32) => Some(quote::quote! { &'data [f32] }),
            Self::Scalar(Scalar::F64) => Some(quote::quote! { &'data [f64] }),
            Self::Scalar(Scalar::String) => Some(quote::quote! { &'data [&'data str] }),
            Self::Scalar(Scalar::Uuid) => Some(quote::quote! { &'data [::cartel_pg::Uuid] }),
            Self::Scalar(Scalar::Timestamp) => {
                Some(quote::quote! { &'data [::cartel_pg::Timestamp] })
            }
            Self::Scalar(Scalar::Date) => Some(quote::quote! { &'data [::cartel_pg::Date] }),
            Self::Scalar(Scalar::Ltree) => Some(quote::quote! { &'data [::cartel_pg::Ltree] }),
            Self::Bytes => Some(quote::quote! { &'data [&'data [u8]] }),
            Self::Scalar(Scalar::Text | Scalar::Jsonb)
            | Self::RowOnly
            | Self::Borrowed
            | Self::Unsupported => None,
        }
    }
}

#[derive(Clone, Copy)]
pub(super) enum Tag {
    Bool,
    I16,
    I32,
    I64,
    U8,
    U32,
    F32,
    F64,
    String,
    Str,
    Text,
    Jsonb,
    Uuid,
    Timestamp,
    Date,
    Ltree,
    VecU8,
    VecI32,
    VecI64,
    VecString,
    VecOther,
    OptBool,
    OptI16,
    OptI32,
    OptI64,
    OptF32,
    OptF64,
    OptString,
    OptText,
    OptJsonb,
    OptUuid,
    OptTimestamp,
    OptDate,
    OptLtree,
    OptBytes,
    OptOther,
    RefStr,
    SliceU8,
    SliceI16,
    SliceI32,
    SliceI64,
    SliceF32,
    SliceF64,
    SliceBool,
    SliceStr,
    RefOther,
    Named,
    Unsupported,
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
        if ident == "bool" {
            Self::Bool
        } else if ident == "i16" {
            Self::I16
        } else if ident == "i32" {
            Self::I32
        } else if ident == "i64" {
            Self::I64
        } else if ident == "u8" {
            Self::U8
        } else if ident == "u32" {
            Self::U32
        } else if ident == "f32" {
            Self::F32
        } else if ident == "f64" {
            Self::F64
        } else if ident == "String" {
            Self::String
        } else if ident == "str" {
            Self::Str
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
            Self::Named
        }
    }

    pub(super) fn vector(ident: &syn::Ident) -> Self {
        if ident == "u8" {
            Self::VecU8
        } else if ident == "i32" {
            Self::VecI32
        } else if ident == "i64" {
            Self::VecI64
        } else if ident == "String" {
            Self::VecString
        } else {
            Self::VecOther
        }
    }

    pub(super) fn optional(ident: &syn::Ident) -> Self {
        if ident == "bool" {
            Self::OptBool
        } else if ident == "i16" {
            Self::OptI16
        } else if ident == "i32" {
            Self::OptI32
        } else if ident == "i64" {
            Self::OptI64
        } else if ident == "f32" {
            Self::OptF32
        } else if ident == "f64" {
            Self::OptF64
        } else if ident == "String" {
            Self::OptString
        } else if ident == "Text" {
            Self::OptText
        } else if ident == "Jsonb" {
            Self::OptJsonb
        } else if ident == "Uuid" {
            Self::OptUuid
        } else if ident == "Timestamp" {
            Self::OptTimestamp
        } else if ident == "Date" {
            Self::OptDate
        } else if ident == "Ltree" {
            Self::OptLtree
        } else {
            Self::OptOther
        }
    }

    pub(super) fn slice(syntax: &syn::Type) -> Self {
        if let Some(ident) = Self::simple(syntax) {
            if ident == "u8" {
                Self::SliceU8
            } else if ident == "i16" {
                Self::SliceI16
            } else if ident == "i32" {
                Self::SliceI32
            } else if ident == "i64" {
                Self::SliceI64
            } else if ident == "f32" {
                Self::SliceF32
            } else if ident == "f64" {
                Self::SliceF64
            } else if ident == "bool" {
                Self::SliceBool
            } else {
                Self::RefOther
            }
        } else if let syn::Type::Reference(rf) = syntax
            && let Some(ident) = Self::simple(rf.elem.as_ref())
            && ident == "str"
        {
            Self::SliceStr
        } else {
            Self::RefOther
        }
    }

    pub(super) fn format(self) -> u16 {
        match self {
            Self::Ltree | Self::OptLtree => 0,
            Self::Bool
            | Self::I16
            | Self::I32
            | Self::I64
            | Self::U8
            | Self::U32
            | Self::F32
            | Self::F64
            | Self::String
            | Self::Str
            | Self::Text
            | Self::Jsonb
            | Self::Uuid
            | Self::Timestamp
            | Self::Date
            | Self::VecU8
            | Self::VecI32
            | Self::VecI64
            | Self::VecString
            | Self::VecOther
            | Self::OptBool
            | Self::OptI16
            | Self::OptI32
            | Self::OptI64
            | Self::OptF32
            | Self::OptF64
            | Self::OptString
            | Self::OptText
            | Self::OptJsonb
            | Self::OptUuid
            | Self::OptTimestamp
            | Self::OptDate
            | Self::OptBytes
            | Self::OptOther
            | Self::RefStr
            | Self::SliceU8
            | Self::SliceI16
            | Self::SliceI32
            | Self::SliceI64
            | Self::SliceF32
            | Self::SliceF64
            | Self::SliceBool
            | Self::SliceStr
            | Self::RefOther
            | Self::Named
            | Self::Unsupported => 1,
        }
    }

    pub(super) fn slice_type(self) -> Option<proc_macro2::TokenStream> {
        match self {
            Self::Bool => Some(quote::quote! { &'data [bool] }),
            Self::I16 => Some(quote::quote! { &'data [i16] }),
            Self::I32 => Some(quote::quote! { &'data [i32] }),
            Self::I64 => Some(quote::quote! { &'data [i64] }),
            Self::F32 => Some(quote::quote! { &'data [f32] }),
            Self::F64 => Some(quote::quote! { &'data [f64] }),
            Self::String => Some(quote::quote! { &'data [&'data str] }),
            Self::Uuid => Some(quote::quote! { &'data [::cartel_pg::Uuid] }),
            Self::Timestamp => Some(quote::quote! { &'data [::cartel_pg::Timestamp] }),
            Self::Date => Some(quote::quote! { &'data [::cartel_pg::Date] }),
            Self::Ltree => Some(quote::quote! { &'data [::cartel_pg::Ltree] }),
            Self::VecU8 => Some(quote::quote! { &'data [&'data [u8]] }),
            Self::U8
            | Self::U32
            | Self::Str
            | Self::Text
            | Self::Jsonb
            | Self::VecI32
            | Self::VecI64
            | Self::VecString
            | Self::VecOther
            | Self::OptBool
            | Self::OptI16
            | Self::OptI32
            | Self::OptI64
            | Self::OptF32
            | Self::OptF64
            | Self::OptString
            | Self::OptText
            | Self::OptJsonb
            | Self::OptUuid
            | Self::OptTimestamp
            | Self::OptDate
            | Self::OptLtree
            | Self::OptBytes
            | Self::OptOther
            | Self::RefStr
            | Self::SliceU8
            | Self::SliceI16
            | Self::SliceI32
            | Self::SliceI64
            | Self::SliceF32
            | Self::SliceF64
            | Self::SliceBool
            | Self::SliceStr
            | Self::RefOther
            | Self::Named
            | Self::Unsupported => None,
        }
    }

    pub(super) fn formats(self, syntax: &syn::Type) -> Option<proc_macro2::TokenStream> {
        match self {
            Self::Ltree | Self::OptLtree => Some(quote::quote! { &[0u16] }),
            Self::Named => Some(quote::quote! { <#syntax>::__CARTEL_RESULT_FORMAT_CODES }),
            Self::Bool
            | Self::I16
            | Self::I32
            | Self::I64
            | Self::U8
            | Self::U32
            | Self::F32
            | Self::F64
            | Self::String
            | Self::Str
            | Self::Text
            | Self::Jsonb
            | Self::Uuid
            | Self::Timestamp
            | Self::Date
            | Self::VecU8
            | Self::VecI32
            | Self::VecI64
            | Self::VecString
            | Self::VecOther
            | Self::OptBool
            | Self::OptI16
            | Self::OptI32
            | Self::OptI64
            | Self::OptF32
            | Self::OptF64
            | Self::OptString
            | Self::OptText
            | Self::OptJsonb
            | Self::OptUuid
            | Self::OptTimestamp
            | Self::OptDate
            | Self::OptBytes
            | Self::OptOther
            | Self::RefStr
            | Self::SliceU8
            | Self::SliceI16
            | Self::SliceI32
            | Self::SliceI64
            | Self::SliceF32
            | Self::SliceF64
            | Self::SliceBool
            | Self::SliceStr
            | Self::RefOther
            | Self::Unsupported => None,
        }
    }
}

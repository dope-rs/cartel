use crate::dialect::pg::types::tag::Tag;

pub(super) struct Kind<'a> {
    pub(super) syntax: &'a syn::Type,
    cause: &'a syn::Type,
    pub(super) tag: Tag,
}

impl<'a> Kind<'a> {
    pub(super) fn new(syntax: &'a syn::Type) -> Self {
        if let syn::Type::Path(path) = syntax {
            return Self::path(syntax, path);
        }
        if let syn::Type::Reference(rf) = syntax {
            return Self::reference(syntax, rf.elem.as_ref());
        }
        Self {
            syntax,
            cause: syntax,
            tag: Tag::Unsupported,
        }
    }

    fn path(syntax: &'a syn::Type, path: &'a syn::TypePath) -> Self {
        if path.qself.is_some()
            || path
                .path
                .segments
                .iter()
                .rev()
                .skip(1)
                .any(|seg| !matches!(&seg.arguments, syn::PathArguments::None))
        {
            return Self::unsupported(syntax);
        }
        let Some(last) = path.path.segments.last() else {
            return Self::unsupported(syntax);
        };
        if let syn::PathArguments::None = &last.arguments {
            return Self {
                syntax,
                cause: syntax,
                tag: Tag::plain(&last.ident),
            };
        }
        let syn::PathArguments::AngleBracketed(args) = &last.arguments else {
            return Self::unsupported(syntax);
        };
        if args.args.len() != 1 {
            return Self::unsupported(syntax);
        }
        let Some(syn::GenericArgument::Type(inner)) = args.args.first() else {
            return Self::unsupported(syntax);
        };
        let tag = if last.ident == "Vec" {
            match Tag::simple(inner) {
                Some(ident) => Tag::vector(ident),
                None => Tag::VecOther,
            }
        } else if last.ident == "Option" {
            match Tag::simple(inner) {
                Some(ident) => Tag::optional(ident),
                None => {
                    if let Tag::VecU8 = Self::new(inner).tag {
                        Tag::OptBytes
                    } else {
                        Tag::OptOther
                    }
                }
            }
        } else {
            Tag::Unsupported
        };
        Self {
            syntax,
            cause: inner,
            tag,
        }
    }

    fn reference(syntax: &'a syn::Type, inner: &'a syn::Type) -> Self {
        let (cause, tag) = if let Some(ident) = Tag::simple(inner) {
            if ident == "str" {
                (inner, Tag::RefStr)
            } else {
                (inner, Tag::RefOther)
            }
        } else if let syn::Type::Slice(slice) = inner {
            let elem = slice.elem.as_ref();
            (elem, Tag::slice(elem))
        } else {
            (inner, Tag::RefOther)
        };
        Self { syntax, cause, tag }
    }

    fn unsupported(syntax: &'a syn::Type) -> Self {
        Self {
            syntax,
            cause: syntax,
            tag: Tag::Unsupported,
        }
    }

    pub(super) fn param(self) -> syn::Result<&'a syn::Type> {
        match self.tag {
            Tag::Bool
            | Tag::I16
            | Tag::I32
            | Tag::I64
            | Tag::F32
            | Tag::F64
            | Tag::String
            | Tag::Text
            | Tag::Jsonb
            | Tag::Uuid
            | Tag::Timestamp
            | Tag::Date
            | Tag::Ltree
            | Tag::VecU8
            | Tag::RefStr
            | Tag::SliceU8
            | Tag::SliceI16
            | Tag::SliceI32
            | Tag::SliceI64
            | Tag::SliceF32
            | Tag::SliceF64
            | Tag::SliceBool
            | Tag::SliceStr => Ok(match self.syntax {
                syn::Type::Reference(value) => &value.elem,
                value => value,
            }),
            _ => Err(syn::Error::new_spanned(
                self.cause,
                "#[query] cannot bind this parameter type",
            )),
        }
    }

    pub(super) fn row_read(self) -> syn::Result<proc_macro2::TokenStream> {
        match self.tag {
            Tag::Bool
            | Tag::I16
            | Tag::I32
            | Tag::I64
            | Tag::F32
            | Tag::F64
            | Tag::String
            | Tag::Text
            | Tag::Jsonb
            | Tag::Uuid
            | Tag::Timestamp
            | Tag::Date
            | Tag::Ltree
            | Tag::VecU8
            | Tag::VecI32
            | Tag::VecI64
            | Tag::VecString
            | Tag::OptBool
            | Tag::OptI16
            | Tag::OptI32
            | Tag::OptI64
            | Tag::OptF32
            | Tag::OptF64
            | Tag::OptString
            | Tag::OptText
            | Tag::OptJsonb
            | Tag::OptUuid
            | Tag::OptTimestamp
            | Tag::OptDate
            | Tag::OptLtree
            | Tag::OptBytes => {
                let syntax = self.syntax;
                Ok(quote::quote! {
                    <#syntax as ::cartel_pg::schema::Row>::decode_owned(row)?
                })
            }
            Tag::U8
            | Tag::U32
            | Tag::Str
            | Tag::VecOther
            | Tag::OptOther
            | Tag::RefStr
            | Tag::SliceU8
            | Tag::SliceI16
            | Tag::SliceI32
            | Tag::SliceI64
            | Tag::SliceF32
            | Tag::SliceF64
            | Tag::SliceBool
            | Tag::SliceStr
            | Tag::RefOther
            | Tag::Named
            | Tag::Unsupported => Err(syn::Error::new_spanned(
                self.cause,
                "#[derive(PgRow)] cannot decode this field type",
            )),
        }
    }
}

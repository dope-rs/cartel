use crate::dialect::pg::types::tag::Tag;

pub(super) struct Kind<'a> {
    syntax: &'a syn::Type,
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
        Self::unsupported(syntax)
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
                None => Tag::Unsupported,
            }
        } else if last.ident == "Option" {
            match Tag::simple(inner) {
                Some(ident) => Tag::optional(ident),
                None if matches!(Self::new(inner).tag, Tag::Bytes) => Tag::RowOnly,
                None => Tag::Unsupported,
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
        let (cause, tag) = if let syn::Type::Slice(slice) = inner {
            let elem = slice.elem.as_ref();
            (elem, Tag::slice(elem))
        } else if Tag::simple(inner).is_some_and(|ident| ident == "str") {
            (inner, Tag::Borrowed)
        } else {
            (inner, Tag::Unsupported)
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
            Tag::Scalar(_) | Tag::Bytes | Tag::Borrowed => Ok(match self.syntax {
                syn::Type::Reference(value) => value.elem.as_ref(),
                value => value,
            }),
            Tag::RowOnly | Tag::Unsupported => Err(syn::Error::new_spanned(
                self.cause,
                "#[query] cannot bind this parameter type",
            )),
        }
    }

    pub(super) fn row_read(self) -> syn::Result<proc_macro2::TokenStream> {
        match self.tag {
            Tag::Scalar(_) | Tag::Bytes | Tag::RowOnly => {
                let syntax = self.syntax;
                Ok(quote::quote! {
                    <#syntax as ::cartel_pg::schema::Row>::decode_owned(row)?
                })
            }
            Tag::Borrowed | Tag::Unsupported => Err(syn::Error::new_spanned(
                self.cause,
                "#[derive(PgRow)] cannot decode this field type",
            )),
        }
    }
}

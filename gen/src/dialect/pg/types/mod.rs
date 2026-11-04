mod kind;
mod tag;

pub(super) trait TypeExt {
    fn bind(&self) -> syn::Result<Bind>;
    fn row_read(&self) -> syn::Result<proc_macro2::TokenStream>;
    fn format(&self) -> u16;
    fn slice_type(&self) -> Option<proc_macro2::TokenStream>;
    fn formats(&self) -> Option<proc_macro2::TokenStream>;
}

impl TypeExt for syn::Type {
    fn bind(&self) -> syn::Result<Bind> {
        kind::Kind::new(self).bind()
    }

    fn row_read(&self) -> syn::Result<proc_macro2::TokenStream> {
        kind::Kind::new(self).row_read()
    }

    fn format(&self) -> u16 {
        kind::Kind::new(self).tag.format()
    }

    fn slice_type(&self) -> Option<proc_macro2::TokenStream> {
        kind::Kind::new(self).tag.slice_type()
    }

    fn formats(&self) -> Option<proc_macro2::TokenStream> {
        if matches!(self, syn::Type::Tuple(_)) {
            let mut parts = Vec::new();
            format_parts(self, &mut parts);
            return Some(quote::quote! {{
                const PARTS: &[(usize, &[u16])] = &[#(#parts),*];
                const COLUMNS: usize = {
                    let mut columns = 0usize;
                    let mut index = 0usize;
                    while index < PARTS.len() {
                        columns += PARTS[index].0;
                        index += 1;
                    }
                    columns
                };
                const FORMATS: ([u16; COLUMNS], bool) = {
                    let mut values = [1u16; COLUMNS];
                    let mut binary = true;
                    let mut offset = 0usize;
                    let mut part = 0usize;
                    while part < PARTS.len() {
                        let (columns, formats) = PARTS[part];
                        ::core::assert!(formats.len() <= 1 || formats.len() == columns,
                            "row result format count must match its columns");
                        let mut column = 0usize;
                        while column < columns {
                            let format = match formats {
                                [] => 0,
                                [format] => *format,
                                formats => formats[column],
                            };
                            values[offset + column] = format;
                            binary &= format == 1;
                            column += 1;
                        }
                        offset += columns;
                        part += 1;
                    }
                    (values, binary)
                };
                if FORMATS.1 { &[1u16] } else { &FORMATS.0 }
            }});
        }
        let kind = kind::Kind::new(self);
        kind.tag.formats(kind.syntax)
    }
}

fn format_parts(syntax: &syn::Type, parts: &mut Vec<proc_macro2::TokenStream>) {
    if let syn::Type::Tuple(tuple) = syntax {
        for field in &tuple.elems {
            format_parts(field, parts);
        }
        return;
    }
    let kind = kind::Kind::new(syntax);
    let columns = if matches!(kind.tag, tag::Tag::Named) {
        quote::quote! { <#syntax>::__CARTEL_N_COLS as usize }
    } else {
        quote::quote! { 1usize }
    };
    let formats = kind
        .tag
        .formats(syntax)
        .unwrap_or_else(|| quote::quote! { &[1u16] });
    parts.push(quote::quote! { (#columns, #formats) });
}

#[derive(Clone, Copy)]
pub(super) enum Arg {
    Move,
    Borrow,
    UuidBytes,
}

#[derive(Clone, Copy)]
pub(super) enum Target {
    Scalar,
    Data,
    Array,
}

pub(super) struct Bind {
    pub(super) oid: u32,
    pub(super) target: Target,
    pub(super) method: syn::Ident,
    pub(super) arg: Arg,
    pub(super) format: u16,
}

impl Bind {
    fn new(oid: u32, target: Target, method: &str) -> Self {
        Self {
            oid,
            target,
            method: quote::format_ident!("{method}"),
            arg: Arg::Move,
            format: 1,
        }
    }

    fn scalar(oid: u32, method: &str) -> Self {
        Self::new(oid, Target::Scalar, method)
    }

    fn data(oid: u32, method: &str) -> Self {
        Self::new(oid, Target::Data, method)
    }

    fn array(oid: u32, method: &str) -> Self {
        Self::new(oid, Target::Array, method)
    }

    fn borrow(mut self) -> Self {
        self.arg = Arg::Borrow;
        self
    }

    fn arg(mut self, arg: Arg) -> Self {
        self.arg = arg;
        self
    }

    fn text(mut self) -> Self {
        self.format = 0;
        self
    }
}

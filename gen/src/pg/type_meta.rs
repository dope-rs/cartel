use proc_macro2::Span;
use quote::quote;
use syn::spanned::Spanned;
use syn::{GenericArgument, PathArguments, Type, TypePath};

pub(super) trait TypeExt {
    fn param_info(&self) -> syn::Result<ParamInfo>;
    fn row_read(&self) -> syn::Result<proc_macro2::TokenStream>;
    fn column_format_code(&self) -> u16;
    fn slice_field_type(&self) -> syn::Result<proc_macro2::TokenStream>;
    fn result_format_codes(&self) -> Option<proc_macro2::TokenStream>;
    fn path_last_ident(&self) -> Option<String>;
    fn generic_inner(&self, expected: &str) -> Option<Type>;
    fn option_inner(&self) -> Option<Type>;
    fn is_vec_of(&self, elem: &str) -> bool;
    fn is_vec_u8(&self) -> bool;
    fn option_read(&self) -> syn::Result<proc_macro2::TokenStream>;
    fn rewrite_to_p(&self) -> Type;
}

impl TypeExt for Type {
    fn param_info(&self) -> syn::Result<ParamInfo> {
        let ty = self;
        if let Some(name) = ty.path_last_ident() {
            return match name.as_str() {
                "bool" => Ok(ParamInfo::primitive(16, "write_bool")),
                "i16" => Ok(ParamInfo::primitive(21, "write_i16")),
                "i32" => Ok(ParamInfo::primitive(23, "write_i32")),
                "i64" => Ok(ParamInfo::primitive(20, "write_i64")),
                "f32" => Ok(ParamInfo::primitive(700, "write_f32")),
                "f64" => Ok(ParamInfo::primitive(701, "write_f64")),
                "String" => Ok(ParamInfo::primitive(25, "write_text").borrow()),
                "Text" => Ok(ParamInfo::primitive(25, "write_text").borrow()),
                "Jsonb" => Ok(ParamInfo::primitive(3802, "write_jsonb").borrow()),
                "Uuid" => {
                    Ok(ParamInfo::primitive(2950, "write_uuid").arg_form(ArgForm::UuidIntoBytes))
                }
                "Timestamp" => Ok(ParamInfo::primitive(1114, "write_timestamp")),
                "Date" => Ok(ParamInfo::primitive(1082, "write_date")),
                "Ltree" => Ok(ParamInfo::primitive(25, "write_ltree")
                    .arg_form(ArgForm::LtreeAsStr)
                    .text_format()
                    .with_cast("ltree")),
                _ if ty.is_vec_u8() => Ok(ParamInfo::primitive(17, "write_bytes").borrow()),
                _ => Err(syn::Error::new(
                    ty.span(),
                    format!("#[query] does not know how to bind parameter of type `{name}`"),
                )),
            };
        }
        if let Type::Reference(r) = ty {
            if let Some(name) = r.elem.path_last_ident()
                && name == "str"
            {
                return Ok(ParamInfo::primitive(25, "write_text"));
            }
            if let Type::Slice(s) = &*r.elem {
                if let Some(elem) = s.elem.path_last_ident() {
                    return match elem.as_str() {
                        "u8" => Ok(ParamInfo::primitive(17, "write_bytes")),
                        "i16" => Ok(ParamInfo::primitive(1005, "write_array_i16")),
                        "i32" => Ok(ParamInfo::primitive(1007, "write_array_i32")),
                        "i64" => Ok(ParamInfo::primitive(1016, "write_array_i64")),
                        "f32" => Ok(ParamInfo::primitive(1021, "write_array_f32")),
                        "f64" => Ok(ParamInfo::primitive(1022, "write_array_f64")),
                        "bool" => Ok(ParamInfo::primitive(1000, "write_array_bool")),
                        other => Err(syn::Error::new(
                            s.elem.span(),
                            format!("#[query] does not know how to bind &[{other}]"),
                        )),
                    };
                }
                if let Type::Reference(inner_ref) = &*s.elem
                    && let Some(name) = inner_ref.elem.path_last_ident()
                    && name == "str"
                {
                    return Ok(ParamInfo::primitive(1009, "write_array_text"));
                }
            }
        }
        Err(syn::Error::new(
            ty.span(),
            "#[query] cannot bind this parameter type; supported: bool, i16, i32, i64, f32, f64, String, &str, Vec<u8>, &[u8], &[i32], &[i64], &[&str], Uuid",
        ))
    }

    fn row_read(&self) -> syn::Result<proc_macro2::TokenStream> {
        let ty = self;
        if let Some(name) = ty.path_last_ident() {
            return match name.as_str() {
                "bool" => Ok(quote! { __r.read_bool()? }),
                "i16" => Ok(quote! { __r.read_i16()? }),
                "i32" => Ok(quote! { __r.read_i32()? }),
                "i64" => Ok(quote! { __r.read_i64()? }),
                "f32" => Ok(quote! { __r.read_f32()? }),
                "f64" => Ok(quote! { __r.read_f64()? }),
                "String" => Ok(quote! { __r.read_text()?.to_owned() }),
                "Text" => Ok(quote! { __r.read_text_shared()? }),
                "Jsonb" => Ok(quote! { __r.read_jsonb()? }),
                "Uuid" => Ok(quote! { ::cartel_pg::Uuid::from_bytes(__r.read_uuid()?) }),
                "Timestamp" => Ok(quote! { ::cartel_pg::Timestamp(__r.read_timestamp()?) }),
                "Date" => Ok(quote! { ::cartel_pg::Date(__r.read_date()?) }),
                "Ltree" => Ok(quote! { ::cartel_pg::Ltree(__r.read_text()?.to_owned()) }),
                _ if ty.is_vec_u8() => Ok(quote! { __r.read_bytes()?.to_vec() }),
                _ if ty.is_vec_of("i64") => Ok(quote! { __r.read_array_i64()? }),
                _ if ty.is_vec_of("i32") => Ok(quote! { __r.read_array_i32()? }),
                _ if ty.is_vec_of("String") => Ok(quote! { __r.read_array_text()? }),
                _ => {
                    if let Some(opt_inner) = ty.option_inner() {
                        return opt_inner.option_read();
                    }
                    Err(syn::Error::new(
                        ty.span(),
                        format!("#[derive(Table)] cannot decode field of type `{name}`"),
                    ))
                }
            };
        }
        Err(syn::Error::new(
            ty.span(),
            "#[derive(Table)] cannot decode this field type; supported: bool, i16, i32, i64, f32, f64, String, Vec<u8>, Uuid, Option<T>",
        ))
    }

    fn column_format_code(&self) -> u16 {
        let ty = self;
        if let Some(name) = ty.path_last_ident() {
            if name == "Ltree" {
                return 0;
            }
            if name == "Option"
                && let Some(inner) = ty.option_inner()
            {
                return inner.column_format_code();
            }
        }
        1
    }

    fn slice_field_type(&self) -> syn::Result<proc_macro2::TokenStream> {
        let ty = self;
        if let Some(name) = ty.path_last_ident() {
            return match name.as_str() {
                "bool" => Ok(quote! { &'__a [bool] }),
                "i16" => Ok(quote! { &'__a [i16] }),
                "i32" => Ok(quote! { &'__a [i32] }),
                "i64" => Ok(quote! { &'__a [i64] }),
                "f32" => Ok(quote! { &'__a [f32] }),
                "f64" => Ok(quote! { &'__a [f64] }),
                "String" => Ok(quote! { &'__a [&'__a str] }),
                "Uuid" => Ok(quote! { &'__a [::cartel_pg::Uuid] }),
                "Timestamp" => Ok(quote! { &'__a [::cartel_pg::Timestamp] }),
                "Date" => Ok(quote! { &'__a [::cartel_pg::Date] }),
                "Ltree" => Ok(quote! { &'__a [::cartel_pg::Ltree] }),
                _ if ty.is_vec_u8() => Ok(quote! { &'__a [&'__a [u8]] }),
                _ => Err(syn::Error::new(
                    ty.span(),
                    format!("insert_each: cannot derive slice type for field of type `{name}`"),
                )),
            };
        }
        Err(syn::Error::new(
            ty.span(),
            "insert_each: cannot derive slice type for this field",
        ))
    }

    fn result_format_codes(&self) -> Option<proc_macro2::TokenStream> {
        let row_ty = self;
        let Type::Path(TypePath {
            path, qself: None, ..
        }) = row_ty
        else {
            return None;
        };
        let last = path.segments.last()?;
        let name = last.ident.to_string();
        if matches!(last.arguments, PathArguments::AngleBracketed(_)) {
            return None;
        }
        let is_primitive = matches!(
            name.as_str(),
            "bool" | "i16" | "i32" | "i64" | "u32" | "f32" | "f64" | "String" | "str"
        );
        if is_primitive {
            return None;
        }
        Some(quote! { <#row_ty>::__CARTEL_RESULT_FORMAT_CODES })
    }

    fn path_last_ident(&self) -> Option<String> {
        let Type::Path(TypePath { path, .. }) = self else {
            return None;
        };
        path.segments.last().map(|s| s.ident.to_string())
    }

    fn generic_inner(&self, expected: &str) -> Option<Type> {
        let Type::Path(TypePath { path, .. }) = self else {
            return None;
        };
        let last = path.segments.last()?;
        if last.ident != expected {
            return None;
        }
        let PathArguments::AngleBracketed(args) = &last.arguments else {
            return None;
        };
        let arg = args.args.first()?;
        let GenericArgument::Type(t) = arg else {
            return None;
        };
        Some(t.clone())
    }

    fn option_inner(&self) -> Option<Type> {
        self.generic_inner("Option")
    }

    fn is_vec_of(&self, elem: &str) -> bool {
        let Some(inner) = self.generic_inner("Vec") else {
            return false;
        };
        inner.path_last_ident().as_deref() == Some(elem)
    }

    fn is_vec_u8(&self) -> bool {
        self.is_vec_of("u8")
    }

    fn rewrite_to_p(&self) -> Type {
        use syn::visit_mut::{self, VisitMut};

        struct PRewriter;
        impl VisitMut for PRewriter {
            fn visit_type_reference_mut(&mut self, r: &mut syn::TypeReference) {
                if r.lifetime.is_none() {
                    r.lifetime = Some(syn::Lifetime::new("'p", Span::call_site()));
                } else if let Some(lt) = &mut r.lifetime
                    && lt.ident == "_"
                {
                    *lt = syn::Lifetime::new("'p", Span::call_site());
                }
                visit_mut::visit_type_reference_mut(self, r);
            }
        }

        let mut t = self.clone();
        PRewriter.visit_type_mut(&mut t);
        t
    }

    fn option_read(&self) -> syn::Result<proc_macro2::TokenStream> {
        let inner = self;
        if inner.is_vec_u8() {
            return Ok(quote! { __r.read_opt_bytes()?.map(|b| b.to_vec()) });
        }
        let Some(name) = inner.path_last_ident() else {
            return Err(syn::Error::new(
                inner.span(),
                "Option<T> only supported for primitive / Uuid / Vec<u8> T",
            ));
        };
        match name.as_str() {
            "bool" => Ok(quote! { __r.read_opt_bool()? }),
            "i32" => Ok(quote! { __r.read_opt_i32()? }),
            "i64" => Ok(quote! { __r.read_opt_i64()? }),
            "String" => Ok(quote! { __r.read_opt_text()?.map(|s| s.to_owned()) }),
            "Uuid" => Ok(quote! { __r.read_opt_uuid()?.map(::cartel_pg::Uuid::from_bytes) }),
            _ => Err(syn::Error::new(
                inner.span(),
                format!(
                    "Option<{name}> not supported (add a RowReader::read_opt_* method to extend)"
                ),
            )),
        }
    }
}

#[derive(Clone, Copy)]
pub(super) enum ArgForm {
    Move,
    Borrow,
    UuidIntoBytes,
    LtreeAsStr,
}

pub(super) struct ParamInfo {
    pub(super) oid: u32,
    pub(super) write_method: &'static str,
    pub(super) arg_form: ArgForm,
    pub(super) format_code: u16,
    pub(super) cast: Option<&'static str>,
}

impl ParamInfo {
    fn primitive(oid: u32, write_method: &'static str) -> Self {
        Self {
            oid,
            write_method,
            arg_form: ArgForm::Move,
            format_code: 1,
            cast: None,
        }
    }
    fn borrow(mut self) -> Self {
        self.arg_form = ArgForm::Borrow;
        self
    }
    fn arg_form(mut self, f: ArgForm) -> Self {
        self.arg_form = f;
        self
    }
    fn text_format(mut self) -> Self {
        self.format_code = 0;
        self
    }
    fn with_cast(mut self, cast: &'static str) -> Self {
        self.cast = Some(cast);
        self
    }
}

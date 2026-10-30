use crate::api::stmt;
use std::{result, string, vec};

pub(super) trait Value {
    fn bind<'value>(
        &'value self,
        binder: &mut stmt::Binder<'_, 'value>,
        index: i32,
    ) -> result::Result<(), crate::Error>;
}

trait Integer: Copy {
    fn widen(self) -> i64;
}

impl Integer for i32 {
    fn widen(self) -> i64 {
        i64::from(self)
    }
}

impl Integer for i64 {
    fn widen(self) -> i64 {
        self
    }
}

impl<T: Value> Value for Option<T> {
    fn bind<'value>(
        &'value self,
        binder: &mut stmt::Binder<'_, 'value>,
        index: i32,
    ) -> result::Result<(), crate::Error> {
        match self {
            Some(value) => value.bind(binder, index),
            None => binder.bind_null(index),
        }
    }
}

impl<T: Integer> Value for T {
    fn bind<'value>(
        &'value self,
        binder: &mut stmt::Binder<'_, 'value>,
        index: i32,
    ) -> result::Result<(), crate::Error> {
        binder.bind_integer(index, (*self).widen())
    }
}

impl Value for u64 {
    fn bind<'value>(
        &'value self,
        binder: &mut stmt::Binder<'_, 'value>,
        index: i32,
    ) -> result::Result<(), crate::Error> {
        if *self > i64::MAX as u64 {
            return Err(crate::Error::ParameterU64 { value: *self });
        }
        binder.bind_integer(index, *self as i64)
    }
}

impl Value for usize {
    fn bind<'value>(
        &'value self,
        binder: &mut stmt::Binder<'_, 'value>,
        index: i32,
    ) -> result::Result<(), crate::Error> {
        if *self > i64::MAX as usize {
            return Err(crate::Error::ParameterUsize { value: *self });
        }
        binder.bind_integer(index, *self as i64)
    }
}

impl Value for f64 {
    fn bind<'value>(
        &'value self,
        binder: &mut stmt::Binder<'_, 'value>,
        index: i32,
    ) -> result::Result<(), crate::Error> {
        binder.bind_real(index, *self)
    }
}

impl Value for bool {
    fn bind<'value>(
        &'value self,
        binder: &mut stmt::Binder<'_, 'value>,
        index: i32,
    ) -> result::Result<(), crate::Error> {
        binder.bind_integer(index, i64::from(*self))
    }
}

impl Value for &str {
    fn bind<'value>(
        &'value self,
        binder: &mut stmt::Binder<'_, 'value>,
        index: i32,
    ) -> result::Result<(), crate::Error> {
        binder.bind_text(index, self)
    }
}

impl Value for &string::String {
    fn bind<'value>(
        &'value self,
        binder: &mut stmt::Binder<'_, 'value>,
        index: i32,
    ) -> result::Result<(), crate::Error> {
        binder.bind_text(index, self.as_str())
    }
}

impl Value for &[u8] {
    fn bind<'value>(
        &'value self,
        binder: &mut stmt::Binder<'_, 'value>,
        index: i32,
    ) -> result::Result<(), crate::Error> {
        binder.bind_blob(index, self)
    }
}

impl Value for &vec::Vec<u8> {
    fn bind<'value>(
        &'value self,
        binder: &mut stmt::Binder<'_, 'value>,
        index: i32,
    ) -> result::Result<(), crate::Error> {
        binder.bind_blob(index, self.as_slice())
    }
}

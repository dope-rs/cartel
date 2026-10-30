use crate::api::{bind::atoms, stmt};
use std::{marker, mem, result};

pub struct Limit;
pub struct Offset;

#[repr(transparent)]
pub struct Bound<K> {
    value: i64,
    kind: marker::PhantomData<K>,
}

impl Bound<Limit> {
    pub const fn new(value: i64) -> Self {
        Self {
            value,
            kind: marker::PhantomData,
        }
    }
}

impl Bound<Offset> {
    pub const fn new(value: i64) -> Self {
        Self {
            value,
            kind: marker::PhantomData,
        }
    }
}

impl atoms::Value for Bound<Limit> {
    fn bind<'value>(
        &'value self,
        binder: &mut stmt::Binder<'_, 'value>,
        index: i32,
    ) -> result::Result<(), crate::Error> {
        if self.value < 0 {
            return Err(crate::Error::NegativeLimit { value: self.value });
        }
        binder.bind_integer(index, self.value)
    }
}

impl atoms::Value for Bound<Offset> {
    fn bind<'value>(
        &'value self,
        binder: &mut stmt::Binder<'_, 'value>,
        index: i32,
    ) -> result::Result<(), crate::Error> {
        if self.value < 0 {
            return Err(crate::Error::NegativeOffset { value: self.value });
        }
        binder.bind_integer(index, self.value)
    }
}

const _: () = {
    assert!(mem::size_of::<Bound<Limit>>() == mem::size_of::<i64>());
    assert!(mem::size_of::<Bound<Offset>>() == mem::size_of::<i64>());
};

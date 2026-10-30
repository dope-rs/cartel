use std::{convert, marker};

pub(in crate::dsl) struct Never<T>(convert::Infallible, marker::PhantomData<fn(T) -> T>);

impl<T> Never<T> {
    pub(in crate::dsl) fn infer<R, A>(self, args: A) -> R {
        match (self, args) {}
    }
}

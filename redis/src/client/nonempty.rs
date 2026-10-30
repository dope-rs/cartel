use std::slice;

/// A slice whose non-empty invariant was checked once at its input boundary.
#[derive(Clone, Copy, Eq, PartialEq)]
#[repr(transparent)]
pub struct Slice<'a, T>(&'a [T]);

impl<'a, T> Slice<'a, T> {
    pub const fn from_ref(value: &'a T) -> Self {
        Self(slice::from_ref(value))
    }

    pub const fn from_pair(values: &'a [T; 2]) -> Self {
        Self(values)
    }

    pub fn new(values: &'a [T]) -> Result<Self, Error> {
        if values.is_empty() {
            Err(Error)
        } else {
            Ok(Self(values))
        }
    }

    pub const fn as_slice(&self) -> &'a [T] {
        self.0
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Error;

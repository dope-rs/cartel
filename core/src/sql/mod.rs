use core::{convert, marker, str};
use sealed::Sealed;

pub mod raw;
mod sealed;
pub mod tokens;

/// The exact field type carried only by generated, uninhabited query checks.
pub struct Target<T>(convert::Infallible, marker::PhantomData<fn(T) -> T>);

/// Keeps ordinary Rust assignment coercions for every non-text field.
pub trait Assign<T>: Sealed<Target<T>> {
    fn assign(self, value: T);
}

impl<T> Assign<T> for Target<T> {
    fn assign(self, _: T) {
        match self.0 {}
    }
}

impl Assign<&str> for Target<String> {
    fn assign(self, _: &str) {
        match self.0 {}
    }
}

impl<T> Sealed<Target<T>> for Target<T> {}
impl Sealed<Target<&str>> for Target<String> {}

/// SQL text assembled from complete UTF-8 fragments.
pub struct Text<const N: usize> {
    bytes: [u8; N],
}

impl<const N: usize> Text<N> {
    const fn new(bytes: [u8; N]) -> Self {
        Self { bytes }
    }

    pub const fn as_str(&self) -> &str {
        // SAFETY: `Parts::concat` copies complete `str` fragments, and the
        // private constructor is its only source.
        unsafe { str::from_utf8_unchecked(&self.bytes) }
    }
}

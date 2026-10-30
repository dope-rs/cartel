use std::{mem, slice};
use subtle::ConstantTimeEq as _;
use zeroize;

#[repr(transparent)]
#[derive(Clone)]
pub(super) struct Secret32(zeroize::Zeroizing<[u8; 32]>);

impl Secret32 {
    pub(super) fn new(bytes: [u8; 32]) -> Self {
        Self(zeroize::Zeroizing::new(bytes))
    }

    pub(super) fn as_array(&self) -> &[u8; 32] {
        &self.0
    }

    pub(super) fn copy(bytes: &[u8; 32]) -> Self {
        Self::new(*bytes)
    }

    pub(super) fn iter_mut(&mut self) -> slice::IterMut<'_, u8> {
        self.0.iter_mut()
    }

    pub(super) fn iter(&self) -> slice::Iter<'_, u8> {
        self.0.iter()
    }
}

impl PartialEq for Secret32 {
    fn eq(&self, other: &Self) -> bool {
        bool::from(self.as_array().ct_eq(other.as_array()))
    }
}

impl Eq for Secret32 {}

const _: () = assert!(mem::size_of::<Secret32>() == 32);

use std::mem;
use subtle::ConstantTimeEq as _;
use zeroize;

#[repr(transparent)]
pub(super) struct Secret32(zeroize::Zeroizing<[u8; 32]>);

impl Secret32 {
    pub(super) fn new(bytes: [u8; 32]) -> Self {
        Self(zeroize::Zeroizing::new(bytes))
    }

    pub(super) fn as_array(&self) -> &[u8; 32] {
        &self.0
    }

    pub(super) fn as_array_mut(&mut self) -> &mut [u8; 32] {
        &mut self.0
    }
}

impl PartialEq for Secret32 {
    fn eq(&self, other: &Self) -> bool {
        bool::from(self.as_array().ct_eq(other.as_array()))
    }
}

impl Eq for Secret32 {}

const _: () = assert!(mem::size_of::<Secret32>() == 32);

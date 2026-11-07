#[derive(Clone, Copy, Eq, PartialEq)]
pub(crate) struct Layout {
    bytes: usize,
}

impl Layout {
    pub(crate) const MIN: Self = Self { bytes: 64 << 10 };
    pub(crate) const PREFIX_BYTES: usize = super::raw::Arena::BYTES;

    pub(crate) const fn new(bytes: usize) -> Option<Self> {
        if bytes < Self::MIN.bytes || !bytes.is_multiple_of(64) {
            return None;
        }
        Some(Self { bytes })
    }

    pub(crate) const fn backing(self) -> Option<usize> {
        self.bytes.checked_add(Self::PREFIX_BYTES)
    }

    pub(crate) const fn bytes(self) -> usize {
        self.bytes
    }

    pub(crate) const fn capacity(self) -> usize {
        self.units() * Self::UNIT_BYTES
    }

    pub(in crate::runtime::heap) const fn units(self) -> usize {
        self.bytes / (Self::UNIT_BYTES + size_of::<u8>())
    }

    pub(in crate::runtime::heap) const UNIT_BYTES: usize = 32;
}

const _: () = assert!(size_of::<Layout>() == size_of::<usize>());

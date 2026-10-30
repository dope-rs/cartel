use std::cmp::Ordering;

#[derive(Clone, Copy, Default, Eq, PartialEq)]
pub(crate) struct Batch(u64);

impl Batch {
    pub(crate) const INITIAL: Self = Self(0);

    pub(crate) const fn next(self) -> Self {
        Self(self.0.wrapping_add(1))
    }

    pub(crate) const fn relative_to(self, expected: Self) -> Ordering {
        let distance = self.0.wrapping_sub(expected.0);
        if distance == 0 {
            Ordering::Equal
        } else if distance <= u64::MAX / 2 {
            Ordering::Greater
        } else {
            Ordering::Less
        }
    }
}

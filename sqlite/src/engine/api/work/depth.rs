use crate::config;
use o3::base::num::bounded;
use std::{cell, mem, num, result};

pub(crate) type Count = bounded::NonZeroUsize<1, { config::MAX_SAVEPOINTS }>;

pub(crate) struct Depth(pub(super) cell::Cell<Option<Count>>);

#[must_use = "a checked savepoint depth must be opened or abandoned"]
pub(crate) struct Permit(Count);

impl Depth {
    pub(crate) const fn new() -> Self {
        Self(cell::Cell::new(None))
    }

    pub(crate) fn check(&self, capacity: Count) -> result::Result<Permit, crate::Error> {
        let current = match self.0.get() {
            Some(current) => current.into_count(),
            None => bounded::Usize::ZERO,
        };
        if let Some(required) = capacity.after(current) {
            Ok(Permit(required))
        } else {
            Err(crate::Error::Savepoints {
                required: num::NonZero::<usize>::MIN
                    .saturating_add(current.get())
                    .get(),
                available: capacity.get(),
            })
        }
    }

    pub(crate) fn open(&self, permit: Permit) -> Count {
        self.0.set(Some(permit.0));
        permit.0
    }

    pub(crate) fn reset(&self) {
        self.0.set(None);
    }
}

const _: () = assert!(mem::size_of::<Option<Permit>>() == mem::size_of::<usize>());

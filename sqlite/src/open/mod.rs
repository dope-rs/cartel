use dope_core::driver::settings::{claim, errors};
mod error;
pub mod journal;

use crate::{api, catalogs, conn, runtime};
use dope_core::driver::settings;
use dope_runtime::{executor::boot, topology};
use std::marker;

pub use error::Error;

/// Owns cleanup errors delivered after an installation releases its native resources.
#[derive(Debug, Default)]
pub struct Completion {
    errors: Option<(crate::Error, Option<crate::Error>)>,
}

impl Completion {
    pub const fn new() -> Self {
        Self { errors: None }
    }

    /// Takes every retained error in observation order, allowing this sink to be reused.
    pub fn take(&mut self) -> Result<(), (crate::Error, Option<crate::Error>)> {
        match self.errors.take() {
            None => Ok(()),
            Some(errors) => Err(errors),
        }
    }

    pub(crate) fn admit(&mut self) -> Result<&mut Self, Error> {
        if self.errors.is_some() {
            return Err(Error::Unobserved);
        }
        Ok(self)
    }

    pub(crate) fn complete(&mut self, errors: (crate::Error, Option<crate::Error>)) {
        self.errors = Some(errors);
    }
}

pub struct Plan<I> {
    catalog: marker::PhantomData<fn() -> I>,
}

impl<I> Plan<I> {
    pub(crate) const fn new() -> Self {
        Self {
            catalog: marker::PhantomData,
        }
    }
}

impl<I> settings::Resource for Plan<I> {
    fn include_in(&self, needs: claim::Needs) -> Result<claim::Needs, errors::Claim> {
        Ok(needs)
    }
}

impl<I: catalogs::Catalog> Plan<I> {
    /// Delivers retained cleanup errors to `report` after this installation is dropped.
    pub fn install<'local, 'scope, 'life, 'p, 'd, Q>(
        self,
        stage: &topology::Boot<'_, 'scope, 'life, 'p, 'd, runtime::Layout<'_, I>, Q>,
        report: &'local mut Completion,
    ) -> impl boot::Root<'d, Output = Result<conn::Installed<'local, 'd, I>, Error>>
    + use<'local, 'd, I, Q>
    where
        'd: 'p,
        'p: 'life,
        'life: 'scope + 'local,
    {
        api::Open::new(stage.pinned(), report)
    }
}

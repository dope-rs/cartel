use crate::{api, catalogs, runtime};
use api::stmt;
use runtime::slot;

pub(super) struct Failing<'local, 'd: 'local, I: catalogs::Catalog> {
    pub(super) prepare: slot::Prepare<'local, 'd, I>,
    pub(super) error: crate::Error,
}

pub(super) enum Failure {
    Pending,
    Done,
}

impl<I: catalogs::Catalog> Failing<'_, '_, I> {
    pub(super) fn advance(&mut self) -> Failure {
        match self.prepare.retire_one() {
            stmt::Inventory::Empty => Failure::Done,
            stmt::Inventory::Populated => Failure::Pending,
        }
    }
}

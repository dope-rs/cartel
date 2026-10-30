use crate::{api, catalogs, config};
use api::{stmt, tx::save::Live, work};
use std::{mem, result};
use stmt::{
    access::{Access, Authority, Retry},
    target,
};
use work::guard::{self, call};

/// Settles forgotten descendants before controlling the owned savepoint.
pub(super) struct Release<'live> {
    source: &'live work::Work,
    depth: work::Count,
    rollback: bool,
}

pub(super) struct Completion<'live> {
    release: Option<work::raw::Release<'live>>,
    done: bool,
}

impl<'live> Release<'live> {
    pub(super) fn new<I: catalogs::Catalog>(
        live: &'live Live<'_, '_, '_, I>,
        rollback: bool,
    ) -> Self {
        Self {
            source: live.source,
            depth: live.depth,
            rollback,
        }
    }
}

impl<'live, I: catalogs::Catalog> target::Target<I> for Release<'live> {
    type Access = Completion<'live>;

    fn bind<'query, 'connection, P: api::Bind>(
        self,
        engine: &'query mut stmt::Engine<'connection, I>,
        params: P,
        config: &config::Config,
        _: &guard::Guard<'_, call::Call>,
    ) -> result::Result<(stmt::Bound<'query, 'connection, P>, Self::Access), crate::Error> {
        // SAFETY: this target borrows the Live owner paired with this State. Start
        // drained its pending work and admitted its Work guard before binding;
        // its owned depth remains live and this State loan excludes depth changes.
        let release = unsafe { work::raw::Release::new(&self.source.depth) };
        let done = release.depth() == self.depth;
        let (control, release) = if done && self.rollback {
            (stmt::Control::RollbackTo, None)
        } else {
            (stmt::Control::Release, Some(release))
        };
        let bound = engine.controls().bind(control, params, config)?;
        Ok((bound, Completion { release, done }))
    }
}

impl Access for Completion<'_> {
    type Output<T> = bool;

    const AUTHORIZATION: Authority = Authority::Control;

    fn retry(&self) -> Retry {
        Retry::Denied
    }

    fn complete<T>(self, _: &guard::Guard<'_, call::Call>, _: T) -> bool {
        if let Some(release) = self.release {
            release.close();
        }
        self.done
    }
}

const _: () = assert!(mem::size_of::<Release<'_>>() == 3 * mem::size_of::<usize>());

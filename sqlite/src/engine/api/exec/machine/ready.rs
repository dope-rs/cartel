use crate::{api, catalogs};
use api::{exec, stmt};
use exec::machine::{self, call};
use std::{mem, result};
use stmt::target::Target;

pub(crate) struct Ready<'query, 'connection: 'query, 'd: 'connection, I: catalogs::Catalog> {
    engine: &'query mut stmt::Engine<'connection, I>,
    state: call::State<'query, 'd>,
}

impl<'query, 'connection: 'query, 'd: 'connection, I: catalogs::Catalog>
    Ready<'query, 'connection, 'd, I>
{
    pub(crate) const fn new(
        engine: &'query mut stmt::Engine<'connection, I>,
        state: call::State<'query, 'd>,
    ) -> Self {
        Self { engine, state }
    }

    pub(super) fn bind<P: api::Bind, T: Target<I>>(
        self,
        target: T,
        params: P,
    ) -> result::Result<machine::Active<'query, 'connection, 'd, P, T::Access>, crate::Error> {
        let Self { engine, state } = self;
        let (bound, access) = target.bind(engine, params, state.config(), &state.guard)?;
        Ok(machine::Active::new(bound, state, access))
    }
}

const _: () = {
    let word = mem::size_of::<usize>();
    assert!(mem::size_of::<Ready<'static, 'static, 'static, catalogs::Empty>>() == 4 * word);
};

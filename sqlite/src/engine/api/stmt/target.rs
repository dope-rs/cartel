use crate::{api, catalogs, config};
use api::stmt::{self, access};
use api::work;
use std::result;
use work::guard::{self, call};

pub(crate) trait Target<I: catalogs::Catalog>: Sized {
    type Access: access::Access;

    fn bind<'query, 'connection, P: api::Bind>(
        self,
        engine: &'query mut stmt::Engine<'connection, I>,
        params: P,
        config: &config::Config,
        guard: &guard::Guard<'_, call::Call>,
    ) -> result::Result<(stmt::Bound<'query, 'connection, P>, Self::Access), crate::Error>;
}

impl<I, Q> Target<I> for catalogs::Statement<I, Q>
where
    I: catalogs::Catalog + catalogs::HasQuery<Q>,
    Q: catalogs::TypedQuery,
{
    type Access = access::Plain;

    fn bind<'query, 'connection, P: api::Bind>(
        self,
        engine: &'query mut stmt::Engine<'connection, I>,
        params: P,
        config: &config::Config,
        _: &guard::Guard<'_, call::Call>,
    ) -> result::Result<(stmt::Bound<'query, 'connection, P>, Self::Access), crate::Error> {
        Ok((
            stmt::Bound::new(engine.statements.lease(self), params, config, Q::PARAMETERS)?,
            access::Plain,
        ))
    }
}

impl<I: catalogs::Catalog> Target<I> for stmt::Control {
    type Access = access::Control;

    fn bind<'query, 'connection, P: api::Bind>(
        self,
        engine: &'query mut stmt::Engine<'connection, I>,
        params: P,
        config: &config::Config,
        _: &guard::Guard<'_, call::Call>,
    ) -> result::Result<(stmt::Bound<'query, 'connection, P>, Self::Access), crate::Error> {
        Ok((
            engine.controls().bind(self, params, config)?,
            access::Control::new(self.retry()),
        ))
    }
}

use dope::{
    core::driver::route::KeyTag,
    manifold::{
        client::{close, conn, session::turns},
        service::bindings::endpoint,
    },
};

use crate::{
    port::{
        self, Disposition,
        pool::{backend, key},
        replies,
    },
    protocol::{self, logic},
    schema,
};

pub(crate) struct Control<'p, 'g, 'r, 's, 'b, 'c, 'd, I: schema::QuerySet, const ID: u8> {
    port: &'p port::Port<'p, 'g, 'r, 's, 'b, 'c, 'd, I, ID>,
}

impl<'p, 'g, 'r, 's, 'b, 'c, 'd, I: schema::QuerySet, const ID: u8>
    Control<'p, 'g, 'r, 's, 'b, 'c, 'd, I, ID>
{
    pub(in crate::port) const fn new(
        port: &'p port::Port<'p, 'g, 'r, 's, 'b, 'c, 'd, I, ID>,
    ) -> Self {
        Self { port }
    }

    pub(crate) fn activate(
        &self,
        entry: &conn::Activation<'_, 'd, KeyTag<ID>>,
    ) -> Result<(), close::Directive> {
        let token = entry.connection();
        let ready = entry.ready();
        if matches!(
            self.port.shared.ready.bind(token, ready),
            protocol::runtime::ready::Bind::Rejected
        ) {
            return Err(self.reject());
        }
        self.port.lane(token.lane()).bind_retire(ready);
        Ok(())
    }

    fn reject(&self) -> close::Directive {
        let reason =
            close::Directive::cut_final(close::Reason::Local(close::LocalOrigin::ResourceFailure));
        self.port.shared.reject(reason.reason());
        reason
    }

    pub(crate) fn try_install(
        &self,
        token: conn::Id<'d, KeyTag<ID>>,
        key: key::Key,
    ) -> Result<(), protocol::Reason> {
        let Some(slot) = self.port.conn(token) else {
            return Err(protocol::Reason::BackendKeyStale);
        };
        backend::Backend::new(slot).try_install(token, key)
    }

    pub(crate) fn cancel(
        &self,
        token: conn::Id<'d, KeyTag<ID>>,
    ) -> Option<(backend::Bound<'p, 'd, ID>, i32)> {
        backend::Backend::new(self.port.conn(token)?).bind(token)
    }

    pub(crate) fn try_ready(
        &self,
        token: conn::Id<'d, KeyTag<ID>>,
    ) -> Result<(), protocol::Reason> {
        self.port.shared.ready.try_ready(token)?;
        if self.port.shared.ready.count() == self.port.capacity() {
            self.port.shared.signals.clear_open();
        }
        Ok(())
    }

    pub(crate) fn deactivate(
        &self,
        drain: turns::Drained<
            'd,
            KeyTag<ID>,
            endpoint::State<
                'p,
                'd,
                port::Port<'p, 'g, 'r, 's, 'b, 'c, 'd, I, ID>,
                logic::Session,
                KeyTag<ID>,
                { port::CONNECTOR_CONNECTION_LIMIT },
            >,
        >,
    ) {
        let token = drain.connection();
        self.port.shared.waiters().wake_all();
        let Some(slot) = self.port.conn(token) else {
            return;
        };
        if let Err(error) = slot.try_retire(token) {
            self.port.shared.fail(error);
        }
    }

    pub(crate) fn wake(&self, token: conn::Id<'d, KeyTag<ID>>) {
        if let Some(slot) = self.port.conn(token) {
            slot.wake();
        }
    }

    pub(crate) fn fail(&self, token: conn::Id<'d, KeyTag<ID>>, cause: close::Reason) {
        self.port.shared.wake_close(
            token,
            replies::Failure::from_close(cause),
            Disposition::Reconnect,
        );
    }

    pub(crate) fn reconnect(&self, token: conn::Id<'d, KeyTag<ID>>, cause: replies::Failure) {
        self.port
            .shared
            .wake_close(token, cause, Disposition::Reconnect);
    }

    pub(crate) fn try_fail(
        &self,
        token: conn::Id<'d, KeyTag<ID>>,
        cause: close::Reason,
    ) -> Option<replies::Failure> {
        self.port.shared.try_close(
            token,
            replies::Failure::from_close(cause),
            Disposition::Reconnect,
        )
    }
}

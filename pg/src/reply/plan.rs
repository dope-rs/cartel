use crate::{
    protocol::frame::Payload,
    reply::{self, server},
};
use o3::buffer::{self, hold};

pub(crate) struct Plan<'input, 'd, R: buffer::Cursor> {
    payload: Payload<'input, 'd, R>,
    class: reply::Class,
    scope: Scope,
}

#[derive(Clone, Copy)]
pub(crate) enum Scope {
    Command,
    Connection,
    Unknown,
}

impl<'input, 'd, R: buffer::Cursor> Plan<'input, 'd, R> {
    pub(crate) fn as_slice(&self) -> &[u8] {
        self.payload.as_slice()
    }

    pub(crate) const fn scope(&self) -> Scope {
        self.scope
    }

    pub(crate) const fn class(&self) -> reply::Class {
        self.class
    }

    pub(crate) fn parse(payload: Payload<'input, 'd, R>) -> Result<Self, crate::Error>
    where
        R: hold::raw::Retain<'d>,
    {
        let (scope, class) =
            server::Fields::try_classify(payload.wire()).map_err(crate::Error::Protocol)?;
        Ok(Self {
            payload,
            class,
            scope,
        })
    }

    pub(crate) fn try_build(self) -> Result<(server::Diagnostic<'d>, reply::Class), hold::Error>
    where
        R: hold::raw::Retain<'d>,
    {
        let Self {
            payload,
            class,
            scope: _,
        } = self;
        let payload = payload
            .into_inner()
            .bind(cartel_core::memory::RETENTION)
            .try_into()?;
        Ok((server::Diagnostic { payload }, class))
    }
}

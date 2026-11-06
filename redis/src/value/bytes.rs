use o3::buffer::resident;
use o3::mem::rc;

use std::ops;

use cartel_core::reply::retain;

use crate::{
    protocol::parser,
    value::{self, reply, source, wire},
};

/// Driver-retained Redis bytes whose clones and slices remain zero-copy.
pub struct Bytes<'arena, 'credits, 'region> {
    inner: resident::Shared<'region>,
    credit: Option<retain::Bytes<'arena, 'credits, 'region>>,
}

impl<'arena, 'credits, 'region> Bytes<'arena, 'credits, 'region> {
    pub(super) fn new(
        inner: resident::Shared<'region>,
        credit: Option<retain::Bytes<'arena, 'credits, 'region>>,
    ) -> Self {
        let credit = credit.filter(|_| !inner.is_empty());
        Self { inner, credit }
    }

    pub(crate) fn try_decode<'s, const TYPE_NAME: bool, const ID: u8>(
        reply: value::Reply<'s, 'arena, 'credits, 'region>,
    ) -> Result<Self, reply::Failure<crate::Error<'region, ID>, Self>> {
        wire::Wire::with(reply, |wire| {
            let value = match wire.try_next() {
                Ok(value) => value,
                Err(error) => return Err(wire.failure(error)),
            };
            let span = match value {
                parser::Value::Bulk(span) => span,
                parser::Value::Status(span) if TYPE_NAME => span,
                actual => {
                    let expected = if TYPE_NAME {
                        reply::Expectation::TypeName
                    } else {
                        reply::Expectation::Bulk
                    };
                    return Err(wire.failure(reply::Fault::expected(expected, actual)));
                }
            };
            Ok(wire.project(span))
        })
    }
}

impl source::Sealed for Bytes<'_, '_, '_> {}

impl source::Bytes for Bytes<'_, '_, '_> {
    fn as_slice(&self) -> &[u8] {
        self.inner.as_slice()
    }

    fn try_clone(&self) -> Result<Self, rc::Error> {
        Ok(Self {
            inner: self.inner.try_clone()?,
            credit: self
                .credit
                .as_ref()
                .map(retain::Bytes::try_clone)
                .transpose()?,
        })
    }
}

impl AsRef<[u8]> for Bytes<'_, '_, '_> {
    fn as_ref(&self) -> &[u8] {
        self.inner.as_slice()
    }
}

impl ops::Deref for Bytes<'_, '_, '_> {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.inner.as_slice()
    }
}

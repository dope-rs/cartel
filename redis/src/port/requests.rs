use std::num;

use o3::{alloc, buffer::pool};

use crate::{client::settings, request, request::encode};

pub(super) struct Requests {
    direct: pool::Pool,
    bulk: Option<pool::Pool>,
}

impl Requests {
    pub(super) fn try_new(config: settings::Config) -> Result<Self, alloc::Error> {
        let direct = pool::Pool::try_from(config.request_pool())?;
        let bulk = config.bulk_pool().map(pool::Pool::try_from).transpose()?;
        Ok(Self { direct, bulk })
    }

    pub(super) fn frame(&self) -> Result<pool::Cursor, request::Error> {
        let buf = self.direct.cursor().ok_or(request::Error::Entries {
            required: num::NonZero::<usize>::MIN,
        })?;
        Ok(buf)
    }

    pub(super) fn sized(&self, length: encode::Length) -> Result<encode::Writer, request::Error> {
        match encode::Writer::try_acquire(&self.direct, length) {
            Err(request::Error::Capacity(error)) => match self.bulk.as_ref() {
                Some(pool) => encode::Writer::try_acquire(pool, length),
                None => Err(request::Error::Capacity(error)),
            },
            result => result,
        }
    }
}

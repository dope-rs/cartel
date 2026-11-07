use std::num;

use cartel_core::frame::Sensitivity;
use o3::{alloc, buffer::pool};

use crate::{client::settings, request, request::encode};

pub(crate) struct Requests {
    direct: pool::Pool,
    bulk: Option<pool::Pool>,
}

impl Requests {
    pub(super) fn try_new(config: settings::Config) -> Result<Self, alloc::Error> {
        let direct = pool::Pool::try_from(config.request_pool())?;
        let bulk = config.bulk_pool().map(pool::Pool::try_from).transpose()?;
        Ok(Self { direct, bulk })
    }

    pub(super) fn frame(&self) -> Result<pool::Span, request::Error> {
        let buf = self.direct.acquire().ok_or(request::Error::Entries {
            required: num::NonZero::<usize>::MIN,
        })?;
        Ok(buf)
    }

    pub(crate) fn bulk(
        &self,
        required: num::NonZero<usize>,
    ) -> Result<&pool::Pool, request::Error> {
        use o3::buffer::write::consume;

        let pool = self
            .bulk
            .as_ref()
            .ok_or_else(|| request::Error::Capacity(consume::Error::empty(required)))?;
        consume::Error::check(required.get(), pool.capacity().get() as usize)
            .map_err(request::Error::Capacity)?;
        Ok(pool)
    }

    pub(super) fn sized(
        &self,
        length: encode::Length,
    ) -> Result<pool::Limit<pool::Span, Sensitivity>, request::Error> {
        match Self::acquire(&self.direct, length) {
            Err(request::Error::Capacity(error)) => match self.bulk.as_ref() {
                Some(pool) => Self::acquire(pool, length),
                None => Err(request::Error::Capacity(error)),
            },
            result => result,
        }
    }

    fn acquire(
        pool: &pool::Pool,
        length: encode::Length,
    ) -> Result<pool::Limit<pool::Span, Sensitivity>, request::Error> {
        pool::Limit::try_acquire(pool, length.get(), Sensitivity::Plain).map_err(|(_, cause)| {
            match cause {
                pool::Error::Capacity(cause) => request::Error::Capacity(cause),
                pool::Error::Empty => request::Error::Entries {
                    required: num::NonZero::<usize>::MIN,
                },
            }
        })
    }
}

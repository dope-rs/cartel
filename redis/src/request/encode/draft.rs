use std::num;

use cartel_core::frame::{Buffer, Sensitivity};
use o3::{
    buffer::pool,
    mem::{self, lease},
};

use crate::request;

pub(super) struct Draft {
    pub(super) out: pool::Limit<pool::Cursor, Sensitivity>,
}

impl Draft {
    pub(super) fn try_new(
        cursor: pool::Cursor,
        length: usize,
        kind: Sensitivity,
    ) -> Result<mem::Lease<Self, lease::Open>, request::Error> {
        let out = match pool::Limit::try_from(cursor, length, kind) {
            Ok(out) => out,
            Err((mut cursor, kind, cause)) => {
                kind.clear(cursor.as_mut());
                return Err(request::Error::Capacity(cause));
            }
        };
        Ok(mem::Lease::open(Self { out }, ()))
    }

    pub(super) fn try_acquire(
        pool: &pool::Pool,
        length: usize,
    ) -> Result<mem::Lease<Self, lease::Open>, request::Error> {
        let out = pool::Limit::try_acquire(pool, length, Sensitivity::Plain).map_err(
            |(Sensitivity::Plain | Sensitivity::Secret, cause)| match cause {
                pool::Error::Capacity(cause) => request::Error::Capacity(cause),
                pool::Error::Empty => request::Error::Entries {
                    required: num::NonZero::<usize>::MIN,
                },
            },
        )?;
        Ok(mem::Lease::open(Self { out }, ()))
    }

    pub(super) fn finish(self) -> Buffer {
        let (cursor, kind) = self.out.into_parts();
        Buffer::new(cursor, kind)
    }
}

impl mem::Return for Draft {
    type Item = ();

    fn return_item(self, (): Self::Item) {
        drop(self.finish());
    }
}

const _: () = assert!(size_of::<Draft>() == 4 * size_of::<usize>());

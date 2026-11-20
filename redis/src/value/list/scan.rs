use crate::value::{
    self,
    list::{self, bulks},
    reply, source,
};

/// A SCAN cursor and its borrowed key array sharing one retained reply.
pub struct Scan<'s, 'arena, 'credits, 'region> {
    cursor: u64,
    keys: bulks::Array<'s, 'arena, 'credits, 'region>,
}

impl source::Sealed for Scan<'_, '_, '_, '_> {}

impl<'s, 'arena, 'credits, 'region> source::Scan for Scan<'s, 'arena, 'credits, 'region> {
    type Bulks = bulks::Array<'s, 'arena, 'credits, 'region>;

    fn cursor(&self) -> u64 {
        self.cursor
    }

    fn keys(&self) -> &Self::Bulks {
        &self.keys
    }
}

impl<'s, 'arena, 'credits, 'region> Scan<'s, 'arena, 'credits, 'region> {
    pub(crate) fn try_decode<const ID: u8>(
        reply: value::Reply<'s, 'arena, 'credits, 'region>,
    ) -> Result<
        Self,
        reply::Failure<crate::Error<'region, ID>, value::Bytes<'arena, 'credits, 'region>>,
    > {
        let (cursor, keys) = list::Lease::scan(reply)?;
        Ok(Self {
            cursor,
            keys: bulks::Array::try_new(keys)?,
        })
    }
}

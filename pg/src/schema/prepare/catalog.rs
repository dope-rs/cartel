use std::marker::PhantomData;

use crate::schema::{self, desc, groups};

/// The immutable preparation metadata matched to this exact query set.
pub(crate) struct Catalog<I>(PhantomData<fn(I) -> I>);

/// An admitted position in `I::PREPARE`, retained across protocol transitions.
pub(crate) struct Cursor<I> {
    group: u32,
    query: u32,
    catalog: PhantomData<fn(I) -> I>,
}

impl<I> Copy for Cursor<I> {}

impl<I> Clone for Cursor<I> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<I: schema::QuerySet> Catalog<I> {
    pub(crate) fn try_new() -> Result<Self, crate::Error> {
        if !<I::Groups as groups::List>::matches(I::PREPARE.groups) {
            return Err(crate::Error::QueryMetadata);
        }
        Ok(Self(PhantomData))
    }

    pub(crate) fn first(self) -> schema::Step<I> {
        Self::at(0, 0)
    }

    fn at(mut group: u32, mut query: u32) -> schema::Step<I> {
        let groups = I::PREPARE.groups;
        while (group as usize) < groups.len() {
            if groups[group as usize].get(query as usize).is_some() {
                return schema::Step::Query(Cursor {
                    group,
                    query,
                    catalog: PhantomData,
                });
            }
            group += 1;
            query = 0;
        }
        schema::Step::Complete
    }
}

impl<I: schema::QuerySet> Cursor<I> {
    pub(crate) fn metadata(self) -> schema::QueryMeta<'static> {
        // SAFETY: only this catalog module constructs Cursor<I>, after selecting
        // this position from the same immutable I::PREPARE metadata. The invariant
        // type identity prevents exchanging it for another query set's catalog.
        unsafe {
            *I::PREPARE
                .groups
                .get_unchecked(self.group as usize)
                .get_unchecked(self.query as usize)
        }
    }

    pub(crate) fn next(self) -> schema::Step<I> {
        Catalog::<I>::at(self.group, self.query + 1)
    }

    pub(crate) fn validate(
        self,
        payload: &[u8],
        registry: desc::Registry,
    ) -> Result<(), crate::Error> {
        let input = desc::Input {
            payload,
            registry,
        };
        <I::Groups as groups::List>::validate(self.group, self.query, input)
    }
}

const _: () = assert!(size_of::<Cursor<()>>() == 2 * size_of::<u32>());

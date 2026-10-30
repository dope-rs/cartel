use crate::protocol::parser::{frame, storage};
use crate::value::source::Bulks as _;
use crate::value::{self, list, list::pair, reply, source};
use cartel_core::reply::retain;
use o3::{depot::array, mem};
use std::{iter, marker, ops::ControlFlow};

pub struct Rejected<B, E> {
    pub value: B,
    pub error: E,
}

/// A validated bulk-string array retaining one frame and one parser-tape lease.
pub struct Array<'s, 'arena, 'credits, 'region>(
    pub(super) list::Lease<'s, 'arena, 'credits, 'region, storage::Bulk<1>>,
);

impl source::Sealed for Array<'_, '_, '_, '_> {}

impl<'s, 'arena, 'credits, 'region> Array<'s, 'arena, 'credits, 'region> {
    pub(super) fn try_new<const ID: u8>(
        lease: list::Lease<'s, 'arena, 'credits, 'region, storage::Raw>,
    ) -> Result<
        Self,
        reply::Failure<crate::Error<'region, ID>, value::Bytes<'arena, 'credits, 'region>>,
    > {
        lease.bulks::<1, ID>().map(Self)
    }
}

impl<'s, 'arena, 'credits, 'region> source::Bulks for Array<'s, 'arena, 'credits, 'region> {
    type Bytes = value::Bytes<'arena, 'credits, 'region>;
    type Chunks<const N: usize> = Chunks<'s, 'arena, 'credits, 'region, N>;
    type Exact<const N: usize> = Exact<'s, 'arena, 'credits, 'region, N>;
    type Iter<'a>
        = ArrayIter<'a, 's, 'arena, 'credits, 'region>
    where
        Self: 'a;

    fn len(&self) -> usize {
        self.0.len()
    }

    fn is_empty(&self) -> bool {
        self.0.len() == 0
    }

    fn iter(&self) -> Self::Iter<'_> {
        ArrayIter(self.0.stored.iter(), marker::PhantomData)
    }

    fn try_chunks<const N: usize>(
        self,
    ) -> Result<Self::Chunks<N>, mem::Rejected<Self, storage::Length>> {
        let list::Lease { stored, keep } = self.0;
        match stored.try_chunks::<N>() {
            Ok(stored) => Ok(Chunks(list::Lease { stored, keep })),
            Err(rejected) => Err(rejected.map(|stored| Self(list::Lease { stored, keep }))),
        }
    }

    fn try_exact<const N: usize>(
        self,
    ) -> Result<Self::Exact<N>, mem::Rejected<Self, storage::Length>> {
        let list::Lease { stored, keep } = self.0;
        match stored.try_exact::<N>() {
            Ok(stored) => Ok(Exact { stored, keep }),
            Err(rejected) => Err(rejected.map(|stored| Self(list::Lease { stored, keep }))),
        }
    }

    fn try_visit<E>(
        self,
        mut inspect: impl FnMut(&[u8]) -> ControlFlow<E>,
    ) -> Result<Self, Rejected<Self::Bytes, E>> {
        self.try_visit_at(|_, value| inspect(value))
    }

    fn try_drain<E>(
        self,
        mut inspect: impl FnMut(&[u8]) -> ControlFlow<E>,
    ) -> Result<(), Rejected<Self::Bytes, E>> {
        self.try_visit(&mut inspect).map(drop)
    }

    fn try_visit_at<E>(
        self,
        mut inspect: impl FnMut(usize, &[u8]) -> ControlFlow<E>,
    ) -> Result<Self, Rejected<Self::Bytes, E>> {
        let list::Lease { stored, keep } = self.0;
        stored.with(|frame, nodes| {
            let mut cursor = nodes.cursor();
            let mut index = 0;
            while let Some(batch) = cursor.batch::<1>() {
                let read = nodes.read(batch);
                cursor = read.cursor;
                let [value] = read.value;
                match inspect(index, frame.slice(value)) {
                    ControlFlow::Continue(()) => {}
                    ControlFlow::Break(error) => {
                        let (credit, _) = keep.into_parts();
                        return Err(Rejected {
                            value: value::Bytes::new(frame.project(value), credit),
                            error,
                        });
                    }
                }
                index += 1;
            }
            Ok(Self(list::Lease {
                stored: frame.store(nodes),
                keep,
            }))
        })
    }

    fn try_drain_at<E>(
        self,
        mut inspect: impl FnMut(usize, &[u8]) -> ControlFlow<E>,
    ) -> Result<(), Rejected<Self::Bytes, E>> {
        self.try_visit_at(&mut inspect).map(drop)
    }
}

impl<'s, 'arena, 'credits, 'region, const ID: u8> value::Decode<'s, 'arena, 'credits, 'region, ID>
    for Array<'s, 'arena, 'credits, 'region>
{
    type Output = Self;

    fn decode(
        reply: value::Reply<'s, 'arena, 'credits, 'region>,
    ) -> Result<
        Self,
        reply::Failure<crate::Error<'region, ID>, value::Bytes<'arena, 'credits, 'region>>,
    > {
        Self::try_new(list::Lease::top(reply)?)
    }
}

pub struct ArrayIter<'a, 's, 'arena, 'credits, 'region>(
    frame::Iter<'a, 's, 'region, storage::Bulk<1>>,
    marker::PhantomData<(&'arena (), &'credits ())>,
);

impl<'a> Iterator for ArrayIter<'a, '_, '_, '_, '_> {
    type Item = &'a [u8];

    fn next(&mut self) -> Option<Self::Item> {
        self.0.next().map(|[value]| value)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let len = self.0.remaining() as usize;
        (len, Some(len))
    }
}

impl iter::FusedIterator for ArrayIter<'_, '_, '_, '_, '_> {}

impl<'a, 's, 'arena, 'credits, 'region> IntoIterator for &'a Array<'s, 'arena, 'credits, 'region> {
    type Item = &'a [u8];
    type IntoIter = ArrayIter<'a, 's, 'arena, 'credits, 'region>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

/// A validated bulk-string array whose members may be nil.
pub struct Optional<'s, 'arena, 'credits, 'region>(
    list::Lease<'s, 'arena, 'credits, 'region, storage::Maybe>,
);

impl source::Sealed for Optional<'_, '_, '_, '_> {}

impl<'s, 'arena, 'credits, 'region> source::Optional for Optional<'s, 'arena, 'credits, 'region> {
    type Bytes = value::Bytes<'arena, 'credits, 'region>;

    fn len(&self) -> usize {
        self.0.len()
    }

    fn is_empty(&self) -> bool {
        self.0.len() == 0
    }

    fn try_array<const N: usize>(&self) -> Result<[Option<&[u8]>; N], reply::Decode<Self::Bytes>> {
        self.0
            .stored
            .try_array::<N>()
            .map_err(|error| reply::Decode::ArrayLength {
                expected: N,
                actual: error.actual,
            })
    }
}

impl<'s, 'arena, 'credits, 'region, const ID: u8> value::Decode<'s, 'arena, 'credits, 'region, ID>
    for Optional<'s, 'arena, 'credits, 'region>
{
    type Output = Self;

    fn decode(
        reply: value::Reply<'s, 'arena, 'credits, 'region>,
    ) -> Result<
        Self,
        reply::Failure<crate::Error<'region, ID>, value::Bytes<'arena, 'credits, 'region>>,
    > {
        list::Lease::top(reply)?.optional().map(Self)
    }
}

const _: [(); 10 * size_of::<usize>()] = [(); size_of::<Array<'_, '_, '_, '_>>()];
const _: [(); 10 * size_of::<usize>()] = [(); size_of::<Option<Array<'_, '_, '_, '_>>>()];
const _: [(); 10 * size_of::<usize>()] = [(); size_of::<Optional<'_, '_, '_, '_>>()];
const _: [(); 10 * size_of::<usize>()] = [(); size_of::<Option<Optional<'_, '_, '_, '_>>>()];

/// A validated array grouped into nonempty, fixed-width chunks.
pub struct Chunks<'s, 'arena, 'credits, 'region, const N: usize>(
    pub(super) list::Lease<'s, 'arena, 'credits, 'region, storage::Bulk<N>>,
);

/// A validated array carrying its exact cardinality and original reply owners.
pub struct Exact<'s, 'arena, 'credits, 'region, const N: usize> {
    stored: frame::Stored<'s, 'region, storage::Exact<'s, N>>,
    keep: retain::Held<'arena, 'credits, 'region>,
}

#[derive(Debug)]
enum Selection<E, D, const N: usize> {
    Value { index: array::Vacancy<N>, error: E },
    Domain(D),
}

/// Selects a statically bounded member or an application error.
#[derive(Debug)]
pub struct Select<E, D, const N: usize>(Selection<E, D, N>);

impl<E, D, const N: usize> Select<E, D, N> {
    pub const fn at<const I: usize>(error: E) -> Self {
        let index = array::Vacancy::<N>::at::<I>();
        Self(Selection::Value { index, error })
    }

    pub const fn domain(error: D) -> Self {
        const { assert!(N != 0 && N <= u32::MAX as usize) };
        Self(Selection::Domain(error))
    }

    fn into_failure<'id, 'arena, 'credits, 'region>(
        self,
        frame: frame::Owner<'id, 'region>,
        keep: retain::Held<'arena, 'credits, 'region>,
        mut spans: [frame::Selection<'id>; N],
    ) -> pair::VisitError<value::Bytes<'arena, 'credits, 'region>, E, D> {
        match self.0 {
            Selection::Value { index, error } => {
                let span = *index.get_mut(&mut spans);
                pair::VisitError::Value {
                    value: value::Bytes::new(frame.project(span), keep.into_parts().0),
                    error,
                }
            }
            Selection::Domain(error) => pair::VisitError::Domain(error),
        }
    }
}

impl<const N: usize> source::Sealed for Chunks<'_, '_, '_, '_, N> {}

impl<'s, 'arena, 'credits, 'region, const N: usize> source::Chunks<N>
    for Chunks<'s, 'arena, 'credits, 'region, N>
{
    type Bytes = value::Bytes<'arena, 'credits, 'region>;
    type Bulks = Array<'s, 'arena, 'credits, 'region>;

    fn len(&self) -> usize {
        self.0.len()
    }

    fn is_empty(&self) -> bool {
        self.0.len() == 0
    }

    fn into_bulks(self) -> Self::Bulks {
        let list::Lease { stored, keep } = self.0;
        Array(list::Lease {
            stored: stored.into_bulks(),
            keep,
        })
    }

    fn try_visit<E, D>(
        self,
        mut inspect: impl FnMut([&[u8]; N]) -> Result<(), Select<E, D, N>>,
    ) -> Result<Self, pair::VisitError<Self::Bytes, E, D>> {
        let list::Lease { stored, keep } = self.0;
        stored.with(|frame, nodes| {
            let mut cursor = nodes.cursor();
            while let Some(batch) = cursor.batch::<N>() {
                let read = nodes.read(batch);
                cursor = read.cursor;
                let spans = read.value;
                let values = spans.map(|span| frame.slice(span));
                if let Err(error) = inspect(values) {
                    return Err(error.into_failure(frame, keep, spans));
                }
            }
            Ok(Self(list::Lease {
                stored: frame.store(nodes),
                keep,
            }))
        })
    }
}

impl<const N: usize> source::Sealed for Exact<'_, '_, '_, '_, N> {}

impl<'s, 'arena, 'credits, 'region, const N: usize> source::Exact<N>
    for Exact<'s, 'arena, 'credits, 'region, N>
{
    type Bytes = value::Bytes<'arena, 'credits, 'region>;
    type Bulks = Array<'s, 'arena, 'credits, 'region>;

    fn as_array(&self) -> [&[u8]; N] {
        self.stored.array()
    }

    fn into_bulks(self) -> Self::Bulks {
        Array(list::Lease {
            stored: self.stored.into_bulks(),
            keep: self.keep,
        })
    }

    fn try_map<R, E, D>(
        self,
        inspect: impl FnOnce([&[u8]; N]) -> Result<R, Select<E, D, N>>,
    ) -> Result<R, pair::VisitError<Self::Bytes, E, D>> {
        self.stored.with(|frame, nodes| {
            let spans = nodes.read();
            let values = spans.map(|span| frame.slice(span));
            match inspect(values) {
                Ok(value) => Ok(value),
                Err(error) => Err(error.into_failure(frame, self.keep, spans)),
            }
        })
    }
}

const _: () = {
    assert!(size_of::<Chunks<'_, '_, '_, '_, 2>>() == size_of::<Array<'_, '_, '_, '_>>());
    assert!(
        size_of::<Exact<'_, '_, '_, '_, 2>>() + size_of::<usize>()
            == size_of::<Array<'_, '_, '_, '_>>()
    );
};

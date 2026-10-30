use crate::protocol::parser::{frame, storage};
use crate::value::source::{Chunks as _, Pairs as _, Scored as _, ScoredText as _};
use crate::value::{self, list, list::bulks, reply, source};
use cartel_core::reply::retain;
use std::{iter, marker, mem, str};

pub enum VisitError<B, E, D> {
    Value { value: B, error: E },
    Domain(D),
}

pub enum Select<E, D> {
    First(E),
    Second(E),
    Domain(D),
}

pub struct Rejected<'s, 'arena, 'credits, 'region, E> {
    frame: (
        frame::Selected<'region, 2>,
        Option<retain::Bytes<'arena, 'credits, 'region>>,
    ),
    index: usize,
    error: E,
    source: marker::PhantomData<storage::Region<'s, storage::Bulk<2>>>,
}

impl<E> source::Sealed for Rejected<'_, '_, '_, '_, E> {}

impl<E> source::PairRejected for Rejected<'_, '_, '_, '_, E> {
    type Error = E;

    fn index(&self) -> usize {
        self.index
    }

    fn error(&self) -> &E {
        &self.error
    }

    fn values(&self) -> (&[u8], &[u8]) {
        let [first, second] = self.frame.0.values();
        (first, second)
    }
}

/// Validated alternating bulk strings exposed as borrowed key/value pairs.
pub struct BulkPairs<'s, 'arena, 'credits, 'region>(
    list::Lease<'s, 'arena, 'credits, 'region, storage::Bulk<2>>,
);

impl source::Sealed for BulkPairs<'_, '_, '_, '_> {}

impl<'s, 'arena, 'credits, 'region> source::Pairs for BulkPairs<'s, 'arena, 'credits, 'region> {
    type Bytes = value::Bytes<'arena, 'credits, 'region>;
    type Iter<'a>
        = Iter<'a, 's, 'arena, 'credits, 'region>
    where
        Self: 'a;
    type Rejected<E> = Rejected<'s, 'arena, 'credits, 'region, E>;

    fn len(&self) -> usize {
        self.0.len()
    }

    fn is_empty(&self) -> bool {
        self.0.len() == 0
    }

    fn iter(&self) -> Self::Iter<'_> {
        Iter(self.0.stored.iter(), marker::PhantomData)
    }

    fn try_check<E>(
        self,
        mut check: impl FnMut(&[u8], &[u8]) -> Result<(), E>,
    ) -> Result<Self, Self::Rejected<E>> {
        let list::Lease { stored, keep } = self.0;
        stored.with(|frame, nodes| {
            let mut cursor = nodes.cursor();
            let mut index = 0;
            while let Some(batch) = cursor.batch::<2>() {
                let read = nodes.read(batch);
                cursor = read.cursor;
                let [first, second] = read.value;
                if let Err(error) = check(frame.slice(first), frame.slice(second)) {
                    return Err(Rejected {
                        frame: (frame.select(read.value), keep.into_parts().0),
                        index,
                        error,
                        source: marker::PhantomData,
                    });
                }
                index += 1;
            }
            Ok(Self(list::Lease {
                stored: frame.store(nodes),
                keep,
            }))
        })
    }

    fn try_visit<E, D>(
        self,
        mut inspect: impl FnMut(&[u8], &[u8]) -> Result<(), Select<E, D>>,
    ) -> Result<Self, VisitError<Self::Bytes, E, D>> {
        bulks::Chunks(self.0)
            .try_visit(|[first, second]| {
                inspect(first, second).map_err(|error| match error {
                    Select::First(error) => bulks::Select::at::<0>(error),
                    Select::Second(error) => bulks::Select::at::<1>(error),
                    Select::Domain(error) => bulks::Select::domain(error),
                })
            })
            .map(|chunks| Self(chunks.0))
    }

    fn try_drain<E, D>(
        self,
        mut inspect: impl FnMut(&[u8], &[u8]) -> Result<(), Select<E, D>>,
    ) -> Result<(), VisitError<Self::Bytes, E, D>> {
        self.try_visit(&mut inspect).map(drop)
    }
}

impl<'s, 'arena, 'credits, 'region, const ID: u8> value::Decode<'s, 'arena, 'credits, 'region, ID>
    for BulkPairs<'s, 'arena, 'credits, 'region>
{
    type Output = Self;

    fn decode(
        reply: value::Reply<'s, 'arena, 'credits, 'region>,
    ) -> Result<
        Self,
        reply::Failure<crate::Error<'region, ID>, value::Bytes<'arena, 'credits, 'region>>,
    > {
        list::Lease::top(reply)?.bulks::<2, ID>().map(Self)
    }
}

pub struct Iter<'a, 's, 'arena, 'credits, 'region>(
    frame::Iter<'a, 's, 'region, storage::Bulk<2>>,
    marker::PhantomData<(&'arena (), &'credits ())>,
);

impl<'a> Iterator for Iter<'a, '_, '_, '_, '_> {
    type Item = (&'a [u8], &'a [u8]);

    fn next(&mut self) -> Option<Self::Item> {
        self.0.next().map(|[first, second]| (first, second))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let len = self.0.remaining() as usize / 2;
        (len, Some(len))
    }
}

impl iter::FusedIterator for Iter<'_, '_, '_, '_, '_> {}

impl<'a, 's, 'arena, 'credits, 'region> IntoIterator
    for &'a BulkPairs<'s, 'arena, 'credits, 'region>
{
    type Item = (&'a [u8], &'a [u8]);
    type IntoIter = Iter<'a, 's, 'arena, 'credits, 'region>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

/// Validated member/score replies exposed without retaining each member.
pub struct ScoredBulks<'s, 'arena, 'credits, 'region>(
    list::Lease<'s, 'arena, 'credits, 'region, storage::Scored>,
);

/// Invalid member text together with the unchanged scored reply owner.
pub struct TextError<S> {
    pub value: S,
    pub index: usize,
    pub error: str::Utf8Error,
}

impl source::Sealed for ScoredBulks<'_, '_, '_, '_> {}

impl<'s, 'arena, 'credits, 'region> source::Scored for ScoredBulks<'s, 'arena, 'credits, 'region> {
    type Text = ScoredText<'s, 'arena, 'credits, 'region>;
    type Iter<'a>
        = ScoredIter<'a, 's, 'arena, 'credits, 'region>
    where
        Self: 'a;

    fn try_text(self) -> Result<Self::Text, TextError<Self>> {
        for (index, (member, _)) in self.iter().enumerate() {
            if let Err(error) = str::from_utf8(member) {
                return Err(TextError {
                    value: self,
                    index,
                    error,
                });
            }
        }
        Ok(ScoredText(self))
    }

    fn iter(&self) -> Self::Iter<'_> {
        ScoredIter(self.0.stored.iter(), marker::PhantomData)
    }
}

/// Scored members whose UTF-8 proof retains the original immutable frame and tape.
#[repr(transparent)]
pub struct ScoredText<'s, 'arena, 'credits, 'region>(ScoredBulks<'s, 'arena, 'credits, 'region>);

impl source::Sealed for ScoredText<'_, '_, '_, '_> {}

impl<'s, 'arena, 'credits, 'region> source::ScoredText
    for ScoredText<'s, 'arena, 'credits, 'region>
{
    type Iter<'a>
        = TextIter<'a, 's, 'arena, 'credits, 'region>
    where
        Self: 'a;

    fn iter(&self) -> Self::Iter<'_> {
        TextIter(self.0.iter())
    }
}

impl<'s, 'arena, 'credits, 'region, const ID: u8> value::Decode<'s, 'arena, 'credits, 'region, ID>
    for ScoredBulks<'s, 'arena, 'credits, 'region>
{
    type Output = Self;

    fn decode(
        reply: value::Reply<'s, 'arena, 'credits, 'region>,
    ) -> Result<
        Self,
        reply::Failure<crate::Error<'region, ID>, value::Bytes<'arena, 'credits, 'region>>,
    > {
        list::Lease::top(reply)?.scored().map(Self)
    }
}

pub struct ScoredIter<'a, 's, 'arena, 'credits, 'region>(
    frame::Iter<'a, 's, 'region, storage::Scored>,
    marker::PhantomData<(&'arena (), &'credits ())>,
);

impl<'a> Iterator for ScoredIter<'a, '_, '_, '_, '_> {
    type Item = (&'a [u8], f64);

    fn next(&mut self) -> Option<Self::Item> {
        self.0.next()
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let len = self.0.remaining() as usize / 2;
        (len, Some(len))
    }
}

impl iter::FusedIterator for ScoredIter<'_, '_, '_, '_, '_> {}

impl<'a, 's, 'arena, 'credits, 'region> IntoIterator
    for &'a ScoredBulks<'s, 'arena, 'credits, 'region>
{
    type Item = (&'a [u8], f64);
    type IntoIter = ScoredIter<'a, 's, 'arena, 'credits, 'region>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

pub struct TextIter<'a, 's, 'arena, 'credits, 'region>(
    ScoredIter<'a, 's, 'arena, 'credits, 'region>,
);

impl<'a> Iterator for TextIter<'a, '_, '_, '_, '_> {
    type Item = (&'a str, f64);

    fn next(&mut self) -> Option<Self::Item> {
        let (member, score) = self.0.next()?;
        // SAFETY: construction validated every member of this unchanged retained frame.
        Some((unsafe { str::from_utf8_unchecked(member) }, score))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.0.size_hint()
    }
}

impl iter::FusedIterator for TextIter<'_, '_, '_, '_, '_> {}

impl<'a, 's, 'arena, 'credits, 'region> IntoIterator
    for &'a ScoredText<'s, 'arena, 'credits, 'region>
{
    type Item = (&'a str, f64);
    type IntoIter = TextIter<'a, 's, 'arena, 'credits, 'region>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

const _: [(); 10 * mem::size_of::<usize>()] = [(); mem::size_of::<BulkPairs<'_, '_, '_, '_>>()];
const _: [(); 10 * mem::size_of::<usize>()] =
    [(); mem::size_of::<Option<BulkPairs<'_, '_, '_, '_>>>()];
const _: [(); 10 * mem::size_of::<usize>()] = [(); mem::size_of::<ScoredBulks<'_, '_, '_, '_>>()];
const _: [(); 10 * mem::size_of::<usize>()] =
    [(); mem::size_of::<Option<ScoredBulks<'_, '_, '_, '_>>>()];
const _: () = {
    assert!(
        mem::size_of::<ScoredText<'_, '_, '_, '_>>()
            == mem::size_of::<ScoredBulks<'_, '_, '_, '_>>()
    );
    assert!(
        mem::size_of::<Option<ScoredText<'_, '_, '_, '_>>>()
            == mem::size_of::<ScoredText<'_, '_, '_, '_>>()
    );
};

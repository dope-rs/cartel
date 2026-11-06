use crate::value::{self, list::bulks, list::pair, reply};
use crate::{
    client,
    protocol::{self, parser::storage},
};
use o3::mem::{self, rc};
use std::{
    fmt, iter,
    ops::{self, ControlFlow},
};

mod sealed;
pub(crate) use sealed::Sealed;

/// A retained driver failure with a borrowed diagnostic projection.
pub trait Control: Sealed {
    type Diagnostic<'a>: fmt::Debug
    where
        Self: 'a;

    fn diagnostic(&self) -> Self::Diagnostic<'_>;
}

/// Retained immutable bytes and their exact retention credits.
pub trait Bytes: Sealed + AsRef<[u8]> + ops::Deref<Target = [u8]> + Sized {
    fn as_slice(&self) -> &[u8];
    fn try_clone(&self) -> Result<Self, rc::Error>;
}

/// A command failure retaining its original cause and response owners.
pub trait Error: Sealed + Sized {
    type Bytes: Bytes;
    type DriverError: Control;

    fn kind(&self) -> reply::Kind;
    fn diagnostic(&self) -> impl fmt::Debug;
}

/// Validated bulk strings whose iteration borrows one retained reply.
pub trait Bulks: Sealed + Sized {
    type Bytes: Bytes;
    type Chunks<const N: usize>: Chunks<N, Bulks = Self, Bytes = Self::Bytes>;
    type Exact<const N: usize>: Exact<N, Bulks = Self, Bytes = Self::Bytes>;
    type Iter<'a>: Iterator<Item = &'a [u8]>
    where
        Self: 'a;

    fn len(&self) -> usize;
    fn is_empty(&self) -> bool;
    fn iter(&self) -> Self::Iter<'_>;
    fn try_chunks<const N: usize>(
        self,
    ) -> Result<Self::Chunks<N>, mem::Rejected<Self, storage::Length>>;
    fn try_exact<const N: usize>(
        self,
    ) -> Result<Self::Exact<N>, mem::Rejected<Self, storage::Length>>;
    fn try_visit<E>(
        self,
        inspect: impl FnMut(&[u8]) -> ControlFlow<E>,
    ) -> Result<Self, bulks::Rejected<Self::Bytes, E>>;
    fn try_drain<E>(
        self,
        inspect: impl FnMut(&[u8]) -> ControlFlow<E>,
    ) -> Result<(), bulks::Rejected<Self::Bytes, E>>;
    fn try_visit_at<E>(
        self,
        inspect: impl FnMut(usize, &[u8]) -> ControlFlow<E>,
    ) -> Result<Self, bulks::Rejected<Self::Bytes, E>>;
    fn try_drain_at<E>(
        self,
        inspect: impl FnMut(usize, &[u8]) -> ControlFlow<E>,
    ) -> Result<(), bulks::Rejected<Self::Bytes, E>>;
}

/// A retained bulk array whose length is a multiple of its nonzero chunk width.
pub trait Chunks<const N: usize>: Sealed + Sized {
    type Bytes: Bytes;
    type Bulks: Bulks<Bytes = Self::Bytes>;

    fn len(&self) -> usize;
    fn is_empty(&self) -> bool;
    fn into_bulks(self) -> Self::Bulks;
    fn try_visit<E, D>(
        self,
        inspect: impl FnMut([&[u8]; N]) -> Result<(), bulks::Select<E, D, N>>,
    ) -> Result<Self, pair::VisitError<Self::Bytes, E, D>>;
}

/// One retained, exactly sized group of bulk strings.
pub trait Exact<const N: usize>: Sealed + Sized {
    type Bytes: Bytes;
    type Bulks: Bulks<Bytes = Self::Bytes>;

    fn as_array(&self) -> [&[u8]; N];
    fn into_bulks(self) -> Self::Bulks;
    fn try_map<R, E, D>(
        self,
        inspect: impl FnOnce([&[u8]; N]) -> Result<R, bulks::Select<E, D, N>>,
    ) -> Result<R, pair::VisitError<Self::Bytes, E, D>>;
}

/// Validated optional bulk strings projected with an exact width check.
pub trait Optional: Sealed + Sized {
    type Bytes: Bytes;

    fn len(&self) -> usize;
    fn is_empty(&self) -> bool;
    fn iter(&self) -> impl ExactSizeIterator<Item = Option<&[u8]>>;
    fn try_array<const N: usize>(&self) -> Result<[Option<&[u8]>; N], reply::Decode<Self::Bytes>>;
}

/// Validated pairs whose failures retain the selected original bytes.
pub trait Pairs: Sealed + Sized {
    type Bytes: Bytes;
    type Iter<'a>: iter::FusedIterator<Item = (&'a [u8], &'a [u8])>
    where
        Self: 'a;
    type Rejected<E>: PairRejected<Error = E>;

    fn len(&self) -> usize;
    fn is_empty(&self) -> bool;
    fn iter(&self) -> Self::Iter<'_>;
    fn try_check<E>(
        self,
        inspect: impl FnMut(&[u8], &[u8]) -> Result<(), E>,
    ) -> Result<Self, Self::Rejected<E>>;
    fn try_visit<E, D>(
        self,
        inspect: impl FnMut(&[u8], &[u8]) -> Result<(), pair::Select<E, D>>,
    ) -> Result<Self, pair::VisitError<Self::Bytes, E, D>>;
    fn try_drain<E, D>(
        self,
        inspect: impl FnMut(&[u8], &[u8]) -> Result<(), pair::Select<E, D>>,
    ) -> Result<(), pair::VisitError<Self::Bytes, E, D>>;
}

/// A rejected pair preserving both borrowed values and the application error.
pub trait PairRejected: Sealed {
    type Error;

    fn index(&self) -> usize;
    fn error(&self) -> &Self::Error;
    fn values(&self) -> (&[u8], &[u8]);
}

/// A SCAN cursor and its retained validated key array.
pub trait Scan: Sealed {
    type Bulks: Bulks;

    fn cursor(&self) -> u64;
    fn keys(&self) -> &Self::Bulks;
}

/// An immutable RESP reply retaining its frame, tape, and credits.
pub trait Reply: Sealed {
    fn reader(&self) -> impl Reader<'_>;
}

/// A cursor whose returned events borrow the original retained reply.
pub trait Reader<'a>: Sealed {
    fn next_event(&mut self) -> Option<value::Event<'a>>;
    fn remaining(&self) -> usize;
    fn finish(self) -> Result<(), protocol::Error>;
}

/// Validated scores retaining their immutable frame, parser tape, and credits.
pub trait Scored: Sealed + Sized {
    type Text: ScoredText;
    type Iter<'a>: Iterator<Item = (&'a [u8], f64)>
    where
        Self: 'a;

    fn iter(&self) -> Self::Iter<'_>;
    fn try_text(self) -> Result<Self::Text, pair::TextError<Self>>;
}

/// Validated text and scores borrowed from the same retained immutable reply.
pub trait ScoredText: Sealed {
    type Iter<'a>: Iterator<Item = (&'a str, f64)>
    where
        Self: 'a;

    fn iter(&self) -> Self::Iter<'_>;
}

/// Validated optional coordinates retained in their original parser storage.
pub trait Coordinates: Sealed {
    type Iter<'a>: ExactSizeIterator<Item = Option<client::GeoCoord>> + iter::FusedIterator
    where
        Self: 'a;

    fn len(&self) -> usize;
    fn is_empty(&self) -> bool;
    fn iter(&self) -> Self::Iter<'_>;
}

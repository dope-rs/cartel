use crate::protocol::value::decode;
use crate::protocol::value::decode::checked;
use crate::reply::{Error, server};
use crate::{api, port::replies, schema};
use cartel_core::reply;
use dope_fiber::abi;
use dope_fiber::context;
use dope_fiber::wait;
use o3::depot::array::init;
use std::{marker, mem, ops::ControlFlow, task::Poll};

use crate::api::{
    task,
    traits::{self, sealed::Sealed},
};
use reply::usage;
use task::stream;

pub mod copy;
pub mod group;
pub mod source;

use group::{Diagnostics, Outcome, Position, Sync};

use source::Payload as _;

/// The exact retained owners returned by one query capability.
pub trait Storage {
    type Payload: source::Payload;
    type Batch: source::Batch;
    type Diagnostics: group::Diagnostics;
}

pub(in crate::api) type StoredRow<S, Q> = Row<<S as Storage>::Payload, Q>;
pub(in crate::api) type StoredCollection<S, Q> = Collection<<S as Storage>::Batch, Q>;

pub(in crate::api) type FirstResult<'d, S, Q, const ID: u8> =
    Result<Option<StoredRow<S, Q>>, Error<'d, ID>>;

pub(in crate::api) type GroupResult<T, E, D, const N: usize> =
    Result<[T; N], GroupFailure<T, E, D, N>>;

/// One row retaining its original immutable payload and reply credits.
pub struct Row<P, Q>
where
    Q: schema::TypedQuery,
{
    payload: P,
    marker: marker::PhantomData<Q>,
}

/// Owns immutable retained rows whose complete static field description was validated.
#[repr(transparent)]
pub struct Validated<T>(T);

impl<P: source::Payload, Q> Row<P, Q>
where
    Q: schema::TypedQuery,
{
    pub(crate) fn new(data: P) -> Self {
        Self {
            payload: data,
            marker: marker::PhantomData,
        }
    }

    pub(in crate::api) fn optional<E>(data: Result<Option<P>, E>) -> Result<Option<Self>, E> {
        Ok(data?.map(Self::new))
    }

    pub(in crate::api) fn required<E: From<crate::Error>>(
        data: Result<Option<P>, E>,
    ) -> Result<Self, E> {
        Ok(Self::new(data?.ok_or(crate::Error::NotFound)?))
    }

    pub fn decode(&self) -> Result<<Q::Row as schema::Row>::View<'_>, crate::Error> {
        let mut reader = decode::RowReader::new(self.payload.wire(), Q::N_RESULT_COLS)?;
        let row = <Q::Row as schema::Row>::decode(&mut reader)?;
        reader.finish()?;
        Ok(row)
    }

    pub fn into_owned(self) -> Result<Q::Row, crate::Error> {
        let mut reader = decode::RowReader::new(self.payload.wire(), Q::N_RESULT_COLS)?;
        let row = <Q::Row as schema::Row>::decode_owned(&mut reader)?;
        reader.finish()?;
        Ok(row)
    }

    pub fn validate(self) -> Result<Validated<Self>, crate::Error>
    where
        Q::Row: schema::Project,
        <Q::Row as schema::Project>::Fields: checked::Fields,
    {
        checked::Row::<<Q::Row as schema::Project>::Fields>::validate(
            self.payload.wire(),
            Q::N_RESULT_COLS,
        )?;
        Ok(Validated(self))
    }
}

impl<P: source::Payload, Q> Validated<Row<P, Q>>
where
    Q: schema::TypedQuery,
    Q::Row: schema::Project,
    <Q::Row as schema::Project>::Fields: checked::Fields,
{
    pub fn view<'row>(&'row self) -> <Q::Row as schema::Row>::View<'row> {
        // SAFETY: this owner keeps the exact validated immutable payload retained.
        let fields = unsafe {
            checked::Row::<<Q::Row as schema::Project>::Fields>::new(self.0.payload.as_slice())
        }
        .read();
        <Q::Row as schema::Project>::project(fields)
    }

    pub fn payload_len(&self) -> usage::Bytes {
        usage::Bytes::new(self.0.payload.as_slice().len())
    }
}

/// Completed rows retaining their original batch and retirement owner.
pub struct Collection<B, Q>
where
    Q: schema::TypedQuery,
{
    batch: B,
    marker: marker::PhantomData<Q>,
}

impl<B: source::Batch, Q> Collection<B, Q>
where
    Q: schema::TypedQuery,
{
    pub(in crate::api) fn from_result<E>(batch: Result<B, E>) -> Result<Self, E> {
        Ok(Self {
            batch: batch?,
            marker: marker::PhantomData,
        })
    }

    pub fn into_owned(self) -> Result<Vec<Q::Row>, crate::Error> {
        self.batch.try_map(|row, slot| {
            let mut reader = decode::RowReader::new(row.wire(), Q::N_RESULT_COLS)?;
            let done = slot.insert(<Q::Row as schema::Row>::decode_owned(&mut reader)?);
            reader.finish()?;
            Ok(done)
        })
    }

    pub fn validate(self) -> Result<Validated<Self>, crate::Error>
    where
        Q::Row: schema::Project,
        <Q::Row as schema::Project>::Fields: checked::Fields,
    {
        for row in self.batch.iter() {
            checked::Row::<<Q::Row as schema::Project>::Fields>::validate(
                row.wire(),
                Q::N_RESULT_COLS,
            )?;
        }
        Ok(Validated(self))
    }
}

impl<B: source::Batch, Q> Validated<Collection<B, Q>>
where
    Q: schema::TypedQuery,
    Q::Row: schema::Project,
    <Q::Row as schema::Project>::Fields: checked::Fields,
{
    pub fn iter<'rows>(&'rows self) -> Iter<'rows, B, Q> {
        Iter {
            rows: self.0.batch.iter(),
            marker: marker::PhantomData,
        }
    }

    pub fn len(&self) -> reply::Length {
        self.0.batch.len()
    }

    pub fn payload_len(&self) -> usage::Bytes {
        self.0.batch.payload_len()
    }
}

pub struct Iter<'rows, B: source::Batch + 'rows, Q: schema::TypedQuery> {
    rows: B::Iter<'rows>,
    marker: marker::PhantomData<Q>,
}

impl<'rows, B: source::Batch + 'rows, Q> Iterator for Iter<'rows, B, Q>
where
    Q: schema::TypedQuery,
    Q::Row: schema::Project,
    <Q::Row as schema::Project>::Fields: checked::Fields,
{
    type Item = <Q::Row as schema::Row>::View<'rows>;

    fn next(&mut self) -> Option<Self::Item> {
        let row = self.rows.next()?;
        // SAFETY: the iterator borrows only the validated owner's immutable batch.
        let fields =
            unsafe { checked::Row::<<Q::Row as schema::Project>::Fields>::new(row.as_slice()) }
                .read();
        Some(<Q::Row as schema::Project>::project(fields))
    }
}

pub struct RunStream<'app, 'g, 'r, 's, 'b, 'c, 'd: 'app, B: ?Sized, I, Q, const ID: u8 = 0>
where
    I: schema::QuerySet + 'd,
    Q: schema::TypedQuery,
{
    pub(super) state: stream::Stream<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, api::ExtractOne<ID>, ID>,
    pub(super) marker: marker::PhantomData<Q>,
}

impl<B: ?Sized, I: schema::QuerySet, Q: schema::TypedQuery, const ID: u8> Sealed
    for RunStream<'_, '_, '_, '_, '_, '_, '_, B, I, Q, ID>
{
}

impl<'app, 'g, 'r, 's, 'b, 'c, 'd: 'app, B: ?Sized, I, Q, const ID: u8> traits::Stream<'d, Q, ID>
    for RunStream<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, Q, ID>
where
    I: schema::QuerySet + 'd,
    Q: schema::TypedQuery,
{
    type Payload = replies::Payload<'b, 'c, 'd>;

    fn next_row(
        &mut self,
    ) -> impl abi::Fiber<'d, Output = Result<Option<Row<Self::Payload, Q>>, Error<'d, ID>>> + '_
    {
        self.state.next(Row::<_, Q>::optional)
    }
}

#[pin_project::pin_project]
pub struct Next<'app, 'g, 'r, 's, 'b, 'c, 'd: 'app, B: ?Sized, I, const ID: u8 = 0>
where
    I: schema::QuerySet + 'd,
{
    pub(super) client: api::Client<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, ID>,
    #[pin]
    pub(super) waiter: wait::Waiter<'app, 'd>,
}

impl<'app, 'g, 'r, 's, 'b, 'c, 'd: 'app, B: ?Sized, I: schema::QuerySet + 'd, const ID: u8>
    abi::Fiber<'d> for Next<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, ID>
{
    type Output = Result<crate::reply::Notice<'d>, Error<'d, ID>>;
    fn poll(call: context::PollCall<'_, '_, 'd, Self>) -> Poll<Self::Output> {
        let (this, mut cx) = call.into_parts();
        let this = this.project();
        let waiter = this.waiter.as_ref();
        let s = &this.client.port.shared;
        if let Some(n) = s.notifications.pop(cx.as_mut().region_token()) {
            waiter.unregister();
            return Poll::Ready(Ok(n));
        }
        if let Some(error) = s.failure() {
            waiter.unregister();
            return Poll::Ready(Err(error));
        }
        match s.notifications.try_register(waiter, cx.as_ref()) {
            Ok(()) => Poll::Pending,
            Err(error) => {
                waiter.unregister();
                Poll::Ready(Err(crate::reply::Error::Wait(error)))
            }
        }
    }
}

/// Every observed outcome and diagnostic from one unsuccessful Sync group.
///
/// `cause` retains admission, capacity, timeout or transport failures alongside
/// command and server failures, even when all commands completed before timeout.
#[derive(Debug)]
pub struct GroupFailure<T, E, D, const N: usize> {
    pub(crate) outcomes: [Outcome<T, E>; N],
    pub(crate) diagnostics: D,
    pub(crate) sync: Sync,
    pub(crate) sync_error: Option<E>,
    pub(crate) cause: Option<E>,
}

impl<T, E, D, const N: usize> GroupFailure<T, E, D, N> {
    pub fn outcomes(&self) -> &[Outcome<T, E>; N] {
        &self.outcomes
    }

    pub fn diagnostics(&self) -> &D {
        &self.diagnostics
    }

    pub fn sync(&self) -> Sync {
        self.sync
    }

    /// A failure that ended admission or the caller's wait, including timeout.
    /// Any failure already posted to the Sync response remains separately available.
    pub fn cause(&self) -> Option<&E> {
        self.cause.as_ref()
    }

    /// The exact failure posted to the terminal Sync response, if observed.
    /// This is preserved even when the caller's deadline also expired.
    pub fn sync_error(&self) -> Option<&E> {
        self.sync_error.as_ref()
    }
}

impl<T, E, D: Diagnostics, const N: usize> GroupFailure<T, E, D, N> {
    /// Borrows the first exact server diagnostic associated with this command.
    pub fn diagnostic(&self, index: usize) -> Option<server::Fields<'_>> {
        self.diagnostics
            .iter()
            .find(|entry| entry.position == Position::Command(index))
            .map(|entry| entry.fields)
    }
}

impl<P: source::Payload, E: From<crate::Error>, D: Diagnostics, const N: usize>
    GroupFailure<Option<P>, E, D, N>
{
    /// Each insertion takes the matching payload; Full leaves only `Completed(None)`.
    /// Vacant owns fewer than N rows. Restoration over N outcome slots stops only
    /// on prefix exhaustion, leaving no unread owner to release.
    pub(in crate::api) fn required<Q>(mut self) -> GroupResult<Row<P, Q>, E, D, N>
    where
        Q: schema::TypedQuery,
    {
        if self.sync == Sync::Complete
            && self.sync_error.is_none()
            && self.cause.is_none()
            && self.diagnostics.is_empty()
        {
            let mut storage = mem::MaybeUninit::uninit();
            let mut rows = init::Guard::<Row<P, Q>, N>::new(&mut storage);
            loop {
                let vacant = match rows.next() {
                    ControlFlow::Break(full) => {
                        mem::forget(self.outcomes);
                        return Ok(full.into_array());
                    }
                    ControlFlow::Continue(vacant) => vacant,
                };
                let payload = match vacant.get_mut(&mut self.outcomes) {
                    Outcome::Completed(payload) => payload.take(),
                    _ => None,
                };
                match payload {
                    Some(payload) => rows = vacant.insert(Row::new(payload)),
                    None => {
                        let mut prefix = vacant.into_iter();
                        for outcome in &mut self.outcomes {
                            let Some(row) = prefix.next() else {
                                break;
                            };
                            *outcome = Outcome::Completed(Some(row.payload));
                        }
                        mem::forget(prefix);
                        break;
                    }
                }
            }
        }
        let outcomes = self.outcomes.map(|outcome| match outcome {
            Outcome::Completed(Some(payload)) => Outcome::Completed(Row::new(payload)),
            Outcome::Completed(None) => Outcome::Failed(crate::Error::NotFound.into()),
            Outcome::Failed(error) => Outcome::Failed(error),
            Outcome::Interrupted(error) => Outcome::Interrupted(error),
            Outcome::Server => Outcome::Server,
            Outcome::Skipped => Outcome::Skipped,
            Outcome::Unobserved => Outcome::Unobserved,
        });
        Err(GroupFailure {
            outcomes,
            diagnostics: self.diagnostics,
            sync: self.sync,
            sync_error: self.sync_error,
            cause: self.cause,
        })
    }
}

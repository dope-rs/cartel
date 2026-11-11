use crate::protocol::value::decode;
use crate::protocol::value::decode::checked;
use crate::reply::{Error, server};
use crate::{api, port::replies, schema};
use cartel_core::reply;
use dope_fiber::abi;
use dope_fiber::context;
use dope_fiber::wait;
use std::{marker, task::Poll};

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

pub(in crate::api) type FirstResult<'d, P, Q, const ID: u8, const SCALAR: bool = false> =
    Result<Option<Row<P, Q, SCALAR>>, Error<'d, ID>>;

pub(in crate::api) type GroupResult<'d, T, D, const N: usize, const ID: u8> =
    Result<[T; N], GroupFailure<'d, T, D, N, ID>>;

/// One row retaining its original immutable payload and reply credits.
pub struct Row<P, Q, const SCALAR: bool = false>
where
    Q: schema::TypedQuery,
{
    payload: P,
    marker: marker::PhantomData<Q>,
}

/// Owns immutable retained rows whose complete static field description was validated.
#[repr(transparent)]
pub struct Validated<T>(T);

impl<P: source::Payload, Q, const SCALAR: bool> Row<P, Q, SCALAR>
where
    Q: schema::TypedQuery<Result = schema::result::Rows<SCALAR>>,
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
}

impl<P: source::Payload, Q> Row<P, Q>
where
    Q: schema::TypedQuery,
{
    pub(in crate::api) fn required<E: From<crate::Error>>(
        data: Result<Option<P>, E>,
    ) -> Result<Self, E>
    where
        Q: schema::TypedQuery<Result = schema::result::Rows>,
    {
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
}

impl<P: source::Payload, Q> Row<P, Q, true>
where
    Q: schema::TypedQuery<Result = schema::result::Rows<true>>,
{
    pub fn decode(&self) -> Result<Option<<Q::Row as schema::Row>::View<'_>>, crate::Error> {
        decode::RowReader::nullable(self.payload.wire(), <Q::Row as schema::Row>::decode)
    }

    pub fn into_owned(self) -> Result<Option<Q::Row>, crate::Error> {
        decode::RowReader::nullable(self.payload.wire(), <Q::Row as schema::Row>::decode_owned)
    }
}

impl<P: source::Payload, Q, const SCALAR: bool> Row<P, Q, SCALAR>
where
    Q: schema::TypedQuery,
{
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

impl<P: source::Payload, Q, const SCALAR: bool> Validated<Row<P, Q, SCALAR>>
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

pub struct RunStream<
    'app,
    'g,
    'r,
    's,
    'b,
    'c,
    'd: 'app,
    B: ?Sized,
    I,
    Q,
    const ID: u8 = 0,
    R = api::Pooled,
> where
    I: schema::QuerySet + 'd,
    Q: schema::TypedQuery,
{
    pub(super) state: stream::Stream<
        'app,
        'g,
        'r,
        's,
        'b,
        'c,
        'd,
        B,
        I,
        api::ExtractOne<ID>,
        ID,
        R,
        marker::PhantomData<api::ExtractOne<ID>>,
    >,
    pub(super) marker: marker::PhantomData<Q>,
}

impl<B: ?Sized, I: schema::QuerySet, Q: schema::TypedQuery, const ID: u8, R> Sealed
    for RunStream<'_, '_, '_, '_, '_, '_, '_, B, I, Q, ID, R>
{
}

impl<'app, 'g, 'r, 's, 'b, 'c, 'd: 'app, B: ?Sized, I, Q, const ID: u8, R> traits::Stream<'d, Q, ID>
    for RunStream<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, Q, ID, R>
where
    I: schema::QuerySet + 'd,
    R: Copy + Into<Option<api::Target<'d, ID>>>,
    Q: schema::TypedQuery<Result = schema::result::Rows>,
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
    pub(super) client: api::Context<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, ID>,
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
pub struct GroupFailure<'d, T, D, const N: usize, const ID: u8 = 0> {
    pub(crate) outcomes: [Outcome<T, replies::ReplyFailure<'d>>; N],
    pub(crate) diagnostics: D,
    pub(crate) sync: Option<Result<(), Error<'d, ID>>>,
    pub(crate) cause: Option<Error<'d, ID>>,
}

impl<'d, T, D, const N: usize, const ID: u8> GroupFailure<'d, T, D, N, ID> {
    pub fn outcomes(&self) -> &[Outcome<T, replies::ReplyFailure<'d>>; N] {
        &self.outcomes
    }

    pub fn diagnostics(&self) -> &D {
        &self.diagnostics
    }

    pub fn sync(&self) -> Sync {
        match self.sync {
            Some(Ok(())) => Sync::Complete,
            _ => Sync::Unobserved,
        }
    }

    /// A failure that ended admission or the caller's wait, including timeout.
    /// Any failure already posted to the Sync response remains separately available.
    pub fn cause(&self) -> Option<&Error<'d, ID>> {
        self.cause.as_ref()
    }

    /// The exact failure posted to the terminal Sync response, if observed.
    /// This is preserved even when the caller's deadline also expired.
    pub fn sync_error(&self) -> Option<&Error<'d, ID>> {
        self.sync.as_ref()?.as_ref().err()
    }
}

impl<T, D: Diagnostics, const N: usize, const ID: u8> GroupFailure<'_, T, D, N, ID> {
    /// Borrows the first exact server diagnostic associated with this command.
    pub fn diagnostic(&self, index: usize) -> Option<server::Fields<'_>> {
        self.diagnostics
            .iter()
            .find(|entry| entry.position == Position::Command(index))
            .map(|entry| entry.fields)
    }
}

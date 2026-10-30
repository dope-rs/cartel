use o3::buffer::write::consume::scope;
use o3::buffer::{self, hold, resident};
use resident::staging;

use std::{mem, num};

use crate::client::settings::init::Step;
use codec::staging::{Buffered, Limit, Policy};
use codec::storage;
use dope::manifold::client::session::{codec, response};

pub mod parser;
use parser::{frame, limit, storage::Storage};

#[derive(Clone, Copy, Debug)]
pub enum Number {
    Empty { offset: u32 },
    Missing { offset: u32 },
    Byte { offset: u32, actual: u8 },
    Overflow { offset: u32 },
}

#[derive(Clone, Copy, Debug)]
pub enum Line {
    Status,
    Server,
    Integer,
    Bulk,
    Array,
}

#[derive(Clone, Copy, Debug)]
pub enum Truncated {
    Value {
        offset: num::NonZero<u32>,
        depth: num::NonZero<u8>,
        remaining: num::NonZero<u32>,
    },
    Line {
        kind: Line,
        start: num::NonZero<u32>,
        actual: num::NonZero<u32>,
    },
    Bulk {
        actual: num::NonZero<u32>,
        required: num::NonZero<u32>,
    },
}

#[derive(Clone, Copy, Debug)]
pub enum Error {
    Number {
        line: Line,
        cause: Number,
    },
    Length {
        line: Line,
        actual: i64,
    },
    Marker {
        offset: u32,
        actual: u8,
    },
    Depth {
        actual: num::NonZero<u8>,
        maximum: num::NonZero<u8>,
        declared: num::NonZero<u32>,
    },
    Terminator {
        offset: u32,
        expected: u8,
        actual: u8,
    },
    Values {
        remaining: num::NonZero<u32>,
    },
    /// EOF was requested with bytes that no decode call has inspected.
    Unparsed {
        actual: num::NonZero<u32>,
    },
    Truncated(Truncated),
}

#[derive(Clone, Copy)]
pub enum Failure {
    Invalid(Error),
    FrameCapacity(limit::Exceeded<limit::Frame>),
    ValueCapacity(limit::Exceeded<limit::Value>),
}

const _: [(); 8] = [(); mem::size_of::<Number>()];
const _: [(); 1] = [(); mem::size_of::<Line>()];
const _: [(); 12] = [(); mem::size_of::<Truncated>()];
const _: [(); 16] = [(); mem::size_of::<Error>()];
const _: [(); 24] = [(); mem::size_of::<Failure>()];

#[derive(Clone, Copy, Default)]
pub(crate) enum Phase {
    #[default]
    Offline,
    Init(Step),
    Ready,
}

#[derive(Default)]
pub struct ConnState {
    phase: Phase,
}

impl ConnState {
    pub(crate) fn reset(&mut self) {
        self.phase = Phase::Offline;
    }

    pub(crate) fn begin_init(&mut self, step: Step) {
        self.phase = Phase::Init(step);
    }

    pub(crate) const fn phase(&self) -> Phase {
        self.phase
    }

    pub(crate) fn mark_ready(&mut self) {
        self.phase = Phase::Ready;
    }
}

#[derive(Clone, Copy)]
pub struct Codec<'a, 'd> {
    frame: limit::Bound<limit::Frame>,
    values: limit::Bound<limit::Value>,
    store: &'a Storage<'d>,
    keep: hold::Policy,
}

impl<'a, 'd> Codec<'a, 'd> {
    #[doc(hidden)]
    pub fn new(
        frame: limit::Bound<limit::Frame>,
        values: limit::Bound<limit::Value>,
        store: &'a Storage<'d>,
        keep: hold::Policy,
    ) -> Self {
        Self {
            frame,
            values,
            store,
            keep,
        }
    }
}

impl<'storage, 'g> codec::Codec for Codec<'storage, 'g> {
    type Head<'input, 'd, R: buffer::Cursor>
        = Result<frame::Parsed<'input, 'd, 'storage, 'g, R>, Failure>
    where
        R: 'input,
        'd: 'input;
    type Error = scope::Error;
    type Staging = Buffered;

    fn staging(&self) -> <Self::Staging as Policy>::Spec {
        const WARM: usize = 16 * 1024;
        let frame = self.frame.get();
        staging::Plan::new(frame.min(WARM), frame)
    }
}

/// One decoded RESP frame that outlives its exact decode input.
///
/// Retention is part of decode, so a frame the connection cannot keep inside
/// its resident budget reports that exact failure instead of decoding.
pub enum Decoded<'storage, 'd, 'g> {
    /// A retained frame and its parser charge until reply admission or discard.
    Frame(
        frame::Stored<'storage, 'd>,
        parser::storage::Lane<'storage, 'g>,
    ),
    /// The frame violated the protocol or exceeded a configured limit.
    Rejected(Failure),
    /// The frame could not be retained inside the resident budget.
    Retain(hold::Error),
}

impl<'storage, 'g> codec::Stateful for Codec<'storage, 'g> {
    type State<'d> = parser::Parser<'storage, 'g>;
    type Storage<S> = S;
    type Preview<'a> = Result<&'a [u8], &'a Failure>;
    type Outcome<'d> = Decoded<'storage, 'd, 'g>;

    fn state<'d>(&self, _: resident::Budget<'d>) -> parser::Parser<'storage, 'g> {
        parser::Parser::new(self.store)
    }

    fn storage<'d, S>(&self, source: S, _: &Self::State<'d>) -> S {
        source
    }

    fn source<S: buffer::raw::Source>(storage: &S) -> &S {
        storage
    }

    fn edit<S>(storage: &mut S) -> storage::Edit<'_, S> {
        storage::Edit::new(storage)
    }

    fn restart<'a, 'd, S>(&self, storage: &'a mut S, _: &Self::State<'d>) -> storage::Edit<'a, S> {
        Self::edit(storage)
    }

    fn decode<'d, S, T, F>(
        &self,
        storage: &mut S,
        state: &mut Self::State<'d>,
        budget: &resident::Budget<'d>,
        limit: Limit,
        admit: impl FnOnce(Result<Self::Preview<'_>, &Self::Error>) -> codec::Admission<F>,
    ) -> codec::Step<T>
    where
        S: buffer::Cursor + hold::raw::Retain<'d> + buffer::raw::Consume,
        T: response::Outcome,
        F: FnOnce(Result<Self::Outcome<'d>, Self::Error>, &mut Self::State<'d>) -> T::Accepted,
    {
        codec::Input::commit(storage, budget, limit, |input| {
            let need = input.need_more(None);
            let parsed = match state.scan(&input, self.frame, self.values) {
                parser::Scan::Source(error) => {
                    state.reset();
                    return Err(match admit(Err(&error)) {
                        codec::Admission::RetryInput => T::retry_input(),
                        codec::Admission::Ready(emit) => T::accept(emit(Err(error), state)),
                    });
                }
                parser::Scan::Blocked => return Ok(codec::Parse::Blocked),
                parser::Scan::Pending => {
                    return Ok(match need {
                        Some(need) => codec::Parse::More(need),
                        None => codec::Parse::CapacityExhausted,
                    });
                }
                parser::Scan::Complete(frame, consumed) => (Ok(frame), consumed),
                parser::Scan::Invalid(error, consumed) => {
                    state.reset();
                    (Err(Failure::Invalid(error)), consumed)
                }
                parser::Scan::FrameCapacity(error, consumed) => {
                    state.reset();
                    (Err(Failure::FrameCapacity(error)), consumed)
                }
                parser::Scan::ValueCapacity(error, consumed) => {
                    state.reset();
                    (Err(Failure::ValueCapacity(error)), consumed)
                }
                parser::Scan::RetainCapacity => return Ok(codec::Parse::CapacityExhausted),
            };
            let (head, consumed) = parsed;
            let emit = match admit(Ok(head.as_ref().map(frame::Parsed::as_slice))) {
                codec::Admission::RetryInput => {
                    state.reset();
                    return Ok(codec::Parse::Item {
                        head: T::retry_input(),
                        consumed,
                    });
                }
                codec::Admission::Ready(emit) => emit,
            };
            let decoded = match head {
                Ok(frame) => match frame.into_stored(self.keep) {
                    Ok((frame, lane)) => Decoded::Frame(frame, lane),
                    Err((_, error)) => Decoded::Retain(error),
                },
                Err(failure) => Decoded::Rejected(failure),
            };
            Ok(codec::Parse::Item {
                head: T::accept(emit(Ok(decoded), state)),
                consumed,
            })
        })
    }

    fn finish<'slot, 'id, 'd, S, T, F>(
        &self,
        mut owner: storage::Owner<'slot, 'id, S, Self::State<'d>>,
        budget: &resident::Budget<'d>,
        limit: Limit,
        admit: impl FnOnce(Result<Self::Preview<'_>, &Self::Error>) -> codec::Admission<F>,
    ) -> (
        storage::Owner<'slot, 'id, S, Self::State<'d>>,
        Result<codec::Finish<T>, T>,
    )
    where
        S: buffer::Cursor + hold::raw::Retain<'d> + buffer::raw::Consume,
        T: response::Outcome,
        F: FnOnce(Result<Self::Outcome<'d>, Self::Error>, &mut Self::State<'d>) -> T::Accepted,
    {
        let (storage, state) = owner.parts();
        let result = codec::Input::with(storage, budget, limit, |input| {
            let Some(actual) = num::NonZero::<usize>::new(input.len()) else {
                state.reset();
                return Ok(codec::Finish::Empty);
            };
            let failure = state.finish(actual, self.frame);
            Ok(codec::Finish::Item(match admit(Ok(Err(&failure))) {
                codec::Admission::RetryInput => T::retry_input(),
                codec::Admission::Ready(emit) => {
                    state.reset();
                    T::accept(emit(Ok(Decoded::Rejected(failure)), state))
                }
            }))
        });
        (owner, result)
    }
}

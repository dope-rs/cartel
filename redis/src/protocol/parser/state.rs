use dope::core::io::transfer;
use dope::manifold::client::session::codec;
use o3::buffer::write::consume::scope;
use o3::{buffer, depot::array::copy};
use std::{mem, num};

use crate::protocol::{self, parser};
use parser::{frame, limit, number, storage};

#[derive(Default)]
enum Mode {
    #[default]
    Start,
    Marker(limit::End),
    Line {
        kind: protocol::Line,
        pending: Pending,
    },
    Bulk {
        end: limit::Body,
        slot: limit::Slot,
    },
}

struct Pending {
    start: limit::End,
    slot: limit::Slot,
}

enum Restore {
    Line(protocol::Line, limit::End),
    Bulk(limit::Body),
}

enum Completion {
    Root,
    Nested,
}

pub(super) struct State {
    scan: usize,
    left: copy::Vec<num::NonZero<u32>, { parser::MAX_DEPTH }>,
    mode: Mode,
}

impl Mode {
    fn cursor(&self) -> usize {
        match self {
            Self::Start => 0,
            Self::Marker(end) => end.get(),
            Self::Line { pending, .. } => pending.start.get(),
            Self::Bulk { end, .. } => end.start() as usize,
        }
    }

    fn line(&mut self, kind: protocol::Line, start: limit::End, slot: limit::Slot) {
        *self = Self::Line {
            kind,
            pending: Pending { start, slot },
        };
    }

    fn bulk(&mut self, end: limit::Body, slot: limit::Slot) {
        *self = Self::Bulk { end, slot };
    }

    fn restore(&mut self, restore: Restore, slot: limit::Slot) {
        match restore {
            Restore::Line(kind, start) => self.line(kind, start, slot),
            Restore::Bulk(end) => self.bulk(end, slot),
        }
    }
}

impl State {
    pub(super) fn new() -> Self {
        Self {
            scan: 0,
            left: copy::Vec::new(),
            mode: Mode::Start,
        }
    }

    pub(super) fn reset(&mut self) {
        self.scan = 0;
        self.left = copy::Vec::new();
        self.mode = Mode::Start;
    }

    fn write<'id, 'a, 'input, 'd, 'storage, 'g, R: buffer::Cursor>(
        mode: &mut Mode,
        write: Result<storage::Stored<'a, 'storage, 'g>, storage::Error>,
        restore: Restore,
    ) -> Result<storage::Stored<'a, 'storage, 'g>, parser::Scan<'id, 'input, 'd, 'storage, 'g, R>>
    where
        'd: 'input,
    {
        match write {
            Ok(stored) => Ok(stored),
            Err(storage::Error::Blocked(slot)) => {
                mode.restore(restore, slot);
                Err(parser::Scan::Blocked)
            }
            Err(storage::Error::Full(slot)) => {
                mode.restore(restore, slot);
                Err(parser::Scan::RetainCapacity)
            }
        }
    }

    fn consumed<'id, 'input, 'd, 'storage, 'g, R: buffer::Cursor>(
        buf: &codec::Input<'id, 'input, 'd, R>,
        amount: num::NonZero<usize>,
        build: impl FnOnce(
            scope::End<'id, { transfer::MAX_BYTES as u32 }>,
        ) -> parser::Scan<'id, 'input, 'd, 'storage, 'g, R>,
    ) -> parser::Scan<'id, 'input, 'd, 'storage, 'g, R>
    where
        'd: 'input,
    {
        match buf.try_consumed(amount) {
            Ok(consumed) => build(consumed),
            Err(cause) => parser::Scan::Source(cause),
        }
    }

    fn finish_value(&mut self, end: limit::End) -> Completion {
        loop {
            let Some(left) = self.left.last_mut() else {
                return Completion::Root;
            };
            let Some(next) = num::NonZero::<u32>::new(left.get() - 1) else {
                self.left.pop();
                continue;
            };
            *left = next;
            self.mode = Mode::Marker(end);
            return Completion::Nested;
        }
    }

    /// The retained inline-array extent bounds parents by MAX_DEPTH - 1.
    pub(super) fn finish(&self, actual: limit::End) -> protocol::Error {
        match &self.mode {
            Mode::Start => protocol::Error::Unparsed {
                actual: actual.count(),
            },
            Mode::Marker(end) => match self.left.split_last() {
                Some((left, parents)) => protocol::Error::Truncated(protocol::Truncated::Value {
                    offset: end.count(),
                    depth: num::NonZero::<u8>::MIN.saturating_add(parents.len() as u8),
                    remaining: *left,
                }),
                None => protocol::Error::Unparsed {
                    actual: actual.count(),
                },
            },
            Mode::Line { kind, pending } => protocol::Error::Truncated(protocol::Truncated::Line {
                kind: *kind,
                start: pending.start.count(),
                actual: actual.count(),
            }),
            Mode::Bulk { end, .. } => protocol::Error::Truncated(protocol::Truncated::Bulk {
                actual: actual.count(),
                required: end.end().count(),
            }),
        }
    }

    pub(super) fn scan<'id, 'input, 'd, 'storage, 'g, R: buffer::Cursor>(
        &mut self,
        builder: &mut storage::Builder<'storage, 'g>,
        buf: &codec::Input<'id, 'input, 'd, R>,
        byte_max: limit::Bound<limit::Frame>,
        node_max: limit::Bound<limit::Value>,
    ) -> parser::Scan<'id, 'input, 'd, 'storage, 'g, R>
    where
        'd: 'input,
    {
        let bytes: &[u8] = buf;
        if self.mode.cursor() > bytes.len() || self.scan > bytes.len() {
            self.reset();
            builder.reset();
        }
        loop {
            match mem::take(&mut self.mode) {
                mode @ (Mode::Start | Mode::Marker(_)) => {
                    let cursor = mode.cursor();
                    self.mode = mode;
                    let Some(marker) = bytes.get(cursor) else {
                        return parser::Scan::Pending;
                    };
                    let end = match byte_max.next(cursor) {
                        Ok(end) => end,
                        Err(error) => {
                            return Self::consumed(buf, byte_max.consumed(), |consumed| {
                                parser::Scan::FrameCapacity(error, consumed)
                            });
                        }
                    };
                    let slot = match node_max.next(builder.len()) {
                        Ok(slot) => slot,
                        Err(error) => {
                            return Self::consumed(buf, end.consumed(), |consumed| {
                                parser::Scan::ValueCapacity(error, consumed)
                            });
                        }
                    };
                    let offset = end.count().get() - 1;
                    let marker = *marker;
                    self.mode = Mode::Marker(end);
                    self.scan = end.get();
                    let kind = match marker {
                        b'+' => protocol::Line::Status,
                        b'-' => protocol::Line::Server,
                        b':' => protocol::Line::Integer,
                        b'$' => protocol::Line::Bulk,
                        b'*' => protocol::Line::Array,
                        marker => {
                            return Self::consumed(buf, end.consumed(), |consumed| {
                                parser::Scan::Invalid(
                                    protocol::Error::Marker {
                                        offset,
                                        actual: marker,
                                    },
                                    consumed,
                                )
                            });
                        }
                    };
                    self.mode.line(kind, end, slot);
                }
                Mode::Line { kind, pending } => {
                    let Some(line) = limit::Line::find(bytes, pending.start, self.scan) else {
                        self.scan = bytes.len().saturating_sub(1).max(pending.start.get());
                        self.mode = Mode::Line { kind, pending };
                        return match byte_max.check(bytes.len()) {
                            Ok(()) => parser::Scan::Pending,
                            Err(error) => Self::consumed(buf, byte_max.consumed(), |consumed| {
                                parser::Scan::FrameCapacity(error, consumed)
                            }),
                        };
                    };
                    let (end, body) = match byte_max.line(line) {
                        Ok(found) => found,
                        Err(error) => {
                            self.mode = Mode::Line { kind, pending };
                            return Self::consumed(buf, byte_max.consumed(), |consumed| {
                                parser::Scan::FrameCapacity(error, consumed)
                            });
                        }
                    };
                    let start = pending.start;
                    let slot = pending.slot;
                    let line = line.value();
                    let value = match kind {
                        protocol::Line::Status => {
                            if line == b"OK" {
                                parser::Kind::Ok
                            } else {
                                let span = parser::Span::terminated(body);
                                parser::Kind::Status(span)
                            }
                        }
                        protocol::Line::Server => {
                            let span = parser::Span::terminated(body);
                            parser::Kind::Error(span)
                        }
                        protocol::Line::Integer => {
                            let integer = match number::Number::new(line, body).signed() {
                                Ok(integer) => integer,
                                Err(cause) => {
                                    self.mode.line(kind, start, slot);
                                    return Self::consumed(buf, end.consumed(), |consumed| {
                                        parser::Scan::Invalid(
                                            protocol::Error::Number {
                                                line: protocol::Line::Integer,
                                                cause,
                                            },
                                            consumed,
                                        )
                                    });
                                }
                            };
                            parser::Kind::Integer(integer)
                        }
                        protocol::Line::Bulk => {
                            let length = match number::Number::new(line, body)
                                .length(protocol::Line::Bulk)
                            {
                                Ok(length) => length,
                                Err(error) => {
                                    self.mode.line(kind, start, slot);
                                    return Self::consumed(buf, end.consumed(), |consumed| {
                                        parser::Scan::Invalid(error, consumed)
                                    });
                                }
                            };
                            match length {
                                number::Length::Nil => parser::Kind::Nil,
                                number::Length::Count(length) => {
                                    let finish = match byte_max.body(end, length) {
                                        Ok(finish) => finish,
                                        Err(error) => {
                                            self.mode.line(kind, start, slot);
                                            return Self::consumed(
                                                buf,
                                                end.consumed(),
                                                |consumed| {
                                                    parser::Scan::FrameCapacity(error, consumed)
                                                },
                                            );
                                        }
                                    };
                                    self.mode.bulk(finish, slot);
                                    continue;
                                }
                            }
                        }
                        protocol::Line::Array => {
                            let count = match number::Number::new(line, body)
                                .length(protocol::Line::Array)
                            {
                                Ok(count) => count,
                                Err(error) => {
                                    self.mode.line(kind, start, slot);
                                    return Self::consumed(buf, end.consumed(), |consumed| {
                                        parser::Scan::Invalid(error, consumed)
                                    });
                                }
                            };
                            match count {
                                number::Length::Nil => parser::Kind::Nil,
                                number::Length::Count(count) => {
                                    let count = match node_max.array(builder.len(), count) {
                                        Ok(count) => count,
                                        Err(error) => {
                                            self.mode.line(kind, start, slot);
                                            return Self::consumed(
                                                buf,
                                                end.consumed(),
                                                |consumed| {
                                                    parser::Scan::ValueCapacity(error, consumed)
                                                },
                                            );
                                        }
                                    };
                                    let nested = match count.nonzero() {
                                        Some(declared) => match self.left.vacancy() {
                                            Some(vacant) => Some((vacant, declared)),
                                            None => {
                                                self.mode.line(kind, start, slot);
                                                return Self::consumed(
                                                    buf,
                                                    end.consumed(),
                                                    |consumed| {
                                                        parser::Scan::Invalid(
                                                            protocol::Error::Depth {
                                                                actual: limit::OVER_DEPTH,
                                                                maximum: limit::DEPTH,
                                                                declared,
                                                            },
                                                            consumed,
                                                        )
                                                    },
                                                );
                                            }
                                        },
                                        None => None,
                                    };
                                    let stored = match Self::write(
                                        &mut self.mode,
                                        builder.try_push(
                                            slot,
                                            parser::Kind::Array { len: count.get() },
                                        ),
                                        Restore::Line(kind, start),
                                    ) {
                                        Ok(stored) => stored,
                                        Err(scan) => return scan,
                                    };
                                    let Some((vacant, left)) = nested else {
                                        if matches!(self.finish_value(end), Completion::Root) {
                                            return complete(self, stored, buf, end.consumed());
                                        }
                                        continue;
                                    };
                                    vacant.insert(left);
                                    self.mode = Mode::Marker(end);
                                    continue;
                                }
                            }
                        }
                    };
                    let stored = match Self::write(
                        &mut self.mode,
                        builder.try_push(slot, value),
                        Restore::Line(kind, start),
                    ) {
                        Ok(stored) => stored,
                        Err(scan) => return scan,
                    };
                    if matches!(self.finish_value(end), Completion::Root) {
                        return complete(self, stored, buf, end.consumed());
                    }
                }
                Mode::Bulk { end, slot } => {
                    let offset = end.stop();
                    let Some(&[cr, lf]) = bytes.get(offset as usize..end.get()) else {
                        self.mode.bulk(end, slot);
                        return parser::Scan::Pending;
                    };
                    if cr != b'\r' || lf != b'\n' {
                        let (offset, expected, actual) = if cr != b'\r' {
                            (offset, b'\r', cr)
                        } else {
                            (offset + 1, b'\n', lf)
                        };
                        self.mode.bulk(end, slot);
                        return Self::consumed(buf, end.consumed(), |consumed| {
                            parser::Scan::Invalid(
                                protocol::Error::Terminator {
                                    offset,
                                    expected,
                                    actual,
                                },
                                consumed,
                            )
                        });
                    }
                    let span = parser::Span::terminated(end);
                    let stored = match Self::write(
                        &mut self.mode,
                        builder.try_push(slot, parser::Kind::Bulk(span)),
                        Restore::Bulk(end),
                    ) {
                        Ok(stored) => stored,
                        Err(scan) => return scan,
                    };
                    if matches!(self.finish_value(end.end()), Completion::Root) {
                        return complete(self, stored, buf, end.consumed());
                    }
                }
            }
        }
    }
}

fn complete<'id, 'input, 'd, 'storage, 'g, R: buffer::Cursor>(
    state: &mut State,
    stored: storage::Stored<'_, 'storage, 'g>,
    buf: &codec::Input<'id, 'input, 'd, R>,
    count: num::NonZero<usize>,
) -> parser::Scan<'id, 'input, 'd, 'storage, 'g, R>
where
    'd: 'input,
{
    let (frame, consumed) = match buf.try_prefix(count) {
        Ok(prefix) => prefix,
        Err(error) => return parser::Scan::Source(error),
    };
    let storage::Finished { nodes, lane } = stored.finish();
    state.reset();
    parser::Scan::Complete(frame::Parsed { frame, nodes, lane }, consumed)
}

const _: [(); 16] = [(); mem::size_of::<Mode>()];
const _: [(); 288] = [(); mem::size_of::<State>()];

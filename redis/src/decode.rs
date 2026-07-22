use o3::buffer::Shared;

use crate::Error;
use crate::protocol;
use crate::value::Value;

pub const MAX_DEPTH: usize = 64;

#[derive(Clone, Copy, Default)]
enum Mode {
    #[default]
    Marker,
    Line(LineKind),
    Bulk {
        payload_end: usize,
        end: usize,
    },
}

#[derive(Clone, Copy)]
enum LineKind {
    Status,
    Error,
    Integer,
    Bulk,
    Array,
}

pub struct ParseState {
    cursor: usize,
    scan: usize,
    values: usize,
    depth: usize,
    remaining: [usize; MAX_DEPTH],
    mode: Mode,
}

impl Default for ParseState {
    fn default() -> Self {
        Self {
            cursor: 0,
            scan: 0,
            values: 0,
            depth: 0,
            remaining: [0; MAX_DEPTH],
            mode: Mode::Marker,
        }
    }
}

impl ParseState {
    fn reset(&mut self) {
        self.cursor = 0;
        self.scan = 0;
        self.values = 0;
        self.depth = 0;
        self.mode = Mode::Marker;
    }

    fn finish_value(&mut self) -> bool {
        loop {
            if self.depth == 0 {
                return true;
            }
            let remaining = &mut self.remaining[self.depth - 1];
            *remaining -= 1;
            if *remaining != 0 {
                self.mode = Mode::Marker;
                return false;
            }
            self.depth -= 1;
        }
    }
}

pub struct Scanned {
    frame: Shared,
    value_count: usize,
}

impl Scanned {
    pub fn frame_len(&self) -> usize {
        self.frame.len()
    }

    pub fn value_count(&self) -> usize {
        self.value_count
    }

    pub fn into_value(self) -> Result<Value, Error> {
        build_value(&self.frame)
    }
}

pub enum Scan {
    Pending,
    Complete(Scanned, usize),
    Invalid(protocol::Error, usize),
    FrameCapacity(usize),
    ValueCapacity(usize),
}

pub fn scan(
    state: &mut ParseState,
    buf: &Shared,
    frame_capacity: usize,
    value_capacity: usize,
) -> Scan {
    let bytes = buf.as_slice();
    if state.cursor > bytes.len() || state.scan > bytes.len() {
        state.reset();
    }
    loop {
        match state.mode {
            Mode::Marker => {
                if state.cursor == bytes.len() {
                    return Scan::Pending;
                }
                if state.cursor >= frame_capacity {
                    return Scan::FrameCapacity(state.cursor.max(1).min(bytes.len()));
                }
                state.values = match state.values.checked_add(1) {
                    Some(values) if values <= value_capacity => values,
                    _ => return Scan::ValueCapacity((state.cursor + 1).min(bytes.len())),
                };
                let marker = bytes[state.cursor];
                state.cursor += 1;
                state.scan = state.cursor;
                state.mode = match marker {
                    b'+' => Mode::Line(LineKind::Status),
                    b'-' => Mode::Line(LineKind::Error),
                    b':' => Mode::Line(LineKind::Integer),
                    b'$' => Mode::Line(LineKind::Bulk),
                    b'*' => Mode::Line(LineKind::Array),
                    _ => {
                        return Scan::Invalid(
                            protocol::Error::UnknownMarker,
                            state.cursor.min(bytes.len()),
                        );
                    }
                };
            }
            Mode::Line(kind) => {
                let Some(line_end) = find_crlf_from(bytes, state.scan) else {
                    state.scan = bytes.len().saturating_sub(1).max(state.cursor);
                    if bytes.len() > frame_capacity {
                        return Scan::FrameCapacity(frame_capacity.max(1));
                    }
                    return Scan::Pending;
                };
                let end = line_end + 2;
                if end > frame_capacity {
                    return Scan::FrameCapacity(end.min(bytes.len()).max(1));
                }
                let line = &bytes[state.cursor..line_end];
                match kind {
                    LineKind::Status | LineKind::Error => {
                        state.cursor = end;
                        if state.finish_value() {
                            return complete(state, buf);
                        }
                    }
                    LineKind::Integer => {
                        if parse_signed(line).is_err() {
                            return Scan::Invalid(protocol::Error::InvalidInteger, end);
                        }
                        state.cursor = end;
                        if state.finish_value() {
                            return complete(state, buf);
                        }
                    }
                    LineKind::Bulk => {
                        let length = match parse_length(line) {
                            Ok(length) => length,
                            Err(error) => return Scan::Invalid(error, end),
                        };
                        if length == -1 {
                            state.cursor = end;
                            if state.finish_value() {
                                return complete(state, buf);
                            }
                            continue;
                        }
                        let Ok(length) = usize::try_from(length) else {
                            return Scan::FrameCapacity(end);
                        };
                        let Some(payload_end) = end.checked_add(length) else {
                            return Scan::FrameCapacity(end);
                        };
                        let Some(frame_end) = payload_end.checked_add(2) else {
                            return Scan::FrameCapacity(end);
                        };
                        if frame_end > frame_capacity {
                            return Scan::FrameCapacity(end);
                        }
                        state.mode = Mode::Bulk {
                            payload_end,
                            end: frame_end,
                        };
                    }
                    LineKind::Array => {
                        let count = match parse_length(line) {
                            Ok(count) => count,
                            Err(error) => return Scan::Invalid(error, end),
                        };
                        state.cursor = end;
                        if count == -1 || count == 0 {
                            if state.finish_value() {
                                return complete(state, buf);
                            }
                            continue;
                        }
                        let Ok(count) = usize::try_from(count) else {
                            return Scan::ValueCapacity(end);
                        };
                        if state
                            .values
                            .checked_add(count)
                            .is_none_or(|minimum| minimum > value_capacity)
                        {
                            return Scan::ValueCapacity(end);
                        }
                        if state.depth == MAX_DEPTH {
                            return Scan::Invalid(protocol::Error::NestingDepth, end);
                        }
                        state.remaining[state.depth] = count;
                        state.depth += 1;
                        state.mode = Mode::Marker;
                    }
                }
            }
            Mode::Bulk { payload_end, end } => {
                if bytes.len() < end {
                    return Scan::Pending;
                }
                if bytes[payload_end] != b'\r' || bytes[payload_end + 1] != b'\n' {
                    return Scan::Invalid(protocol::Error::BulkTerminator, end);
                }
                state.cursor = end;
                if state.finish_value() {
                    return complete(state, buf);
                }
            }
        }
    }
}

fn complete(state: &mut ParseState, buf: &Shared) -> Scan {
    let consumed = state.cursor;
    let value_count = state.values;
    let frame = buf.slice(..consumed);
    state.reset();
    Scan::Complete(Scanned { frame, value_count }, consumed)
}

fn find_crlf_from(bytes: &[u8], mut cursor: usize) -> Option<usize> {
    while cursor + 1 < bytes.len() {
        if bytes[cursor] == b'\r' && bytes[cursor + 1] == b'\n' {
            return Some(cursor);
        }
        cursor += 1;
    }
    None
}

fn parse_length(bytes: &[u8]) -> Result<i64, protocol::Error> {
    let value = parse_signed(bytes).map_err(|_| protocol::Error::InvalidInteger)?;
    if value < -1 {
        return Err(protocol::Error::InvalidLength);
    }
    Ok(value)
}

fn parse_signed(bytes: &[u8]) -> Result<i64, protocol::Error> {
    if bytes.is_empty() {
        return Err(protocol::Error::InvalidInteger);
    }
    let mut cursor = 0;
    let negative = bytes[0] == b'-';
    if negative {
        cursor = 1;
        if cursor == bytes.len() {
            return Err(protocol::Error::InvalidInteger);
        }
    }
    let mut value = 0i64;
    while cursor < bytes.len() {
        let digit = bytes[cursor].wrapping_sub(b'0');
        if digit > 9 {
            return Err(protocol::Error::InvalidInteger);
        }
        value = if negative {
            value
                .checked_mul(10)
                .and_then(|value| value.checked_sub(i64::from(digit)))
        } else {
            value
                .checked_mul(10)
                .and_then(|value| value.checked_add(i64::from(digit)))
        }
        .ok_or(protocol::Error::InvalidInteger)?;
        cursor += 1;
    }
    Ok(value)
}

fn build_value(frame: &Shared) -> Result<Value, Error> {
    let (value, consumed) = build_at(frame, 0, 0)?;
    if consumed != frame.len() {
        return Err(Error::Protocol(protocol::Error::TrailingBytes));
    }
    Ok(value)
}

fn build_at(frame: &Shared, offset: usize, depth: usize) -> Result<(Value, usize), Error> {
    if depth > MAX_DEPTH || offset >= frame.len() {
        return Err(Error::Protocol(protocol::Error::NestingDepth));
    }
    let bytes = frame.as_slice();
    match bytes[offset] {
        b'+' => {
            let start = offset + 1;
            let line_end = find_crlf_from(bytes, start)
                .ok_or(Error::Protocol(protocol::Error::TrailingBytes))?;
            let value = if bytes[start..line_end] == *b"OK" {
                Value::Ok
            } else {
                Value::Status(frame.slice(start..line_end))
            };
            Ok((value, line_end + 2))
        }
        b'-' => {
            let start = offset + 1;
            let line_end = find_crlf_from(bytes, start)
                .ok_or(Error::Protocol(protocol::Error::TrailingBytes))?;
            Ok((Value::Error(frame.slice(start..line_end)), line_end + 2))
        }
        b':' => {
            let start = offset + 1;
            let line_end = find_crlf_from(bytes, start)
                .ok_or(Error::Protocol(protocol::Error::TrailingBytes))?;
            let value = parse_signed(&bytes[start..line_end]).map_err(Error::Protocol)?;
            Ok((Value::Integer(value), line_end + 2))
        }
        b'$' => {
            let start = offset + 1;
            let line_end = find_crlf_from(bytes, start)
                .ok_or(Error::Protocol(protocol::Error::TrailingBytes))?;
            let length = parse_length(&bytes[start..line_end]).map_err(Error::Protocol)?;
            if length == -1 {
                return Ok((Value::Nil, line_end + 2));
            }
            let payload_start = line_end + 2;
            let length = usize::try_from(length)
                .map_err(|_| Error::Protocol(protocol::Error::InvalidLength))?;
            let payload_end = payload_start + length;
            Ok((
                Value::Bulk(frame.slice(payload_start..payload_end)),
                payload_end + 2,
            ))
        }
        b'*' => {
            let start = offset + 1;
            let line_end = find_crlf_from(bytes, start)
                .ok_or(Error::Protocol(protocol::Error::TrailingBytes))?;
            let count = parse_length(&bytes[start..line_end]).map_err(Error::Protocol)?;
            if count == -1 {
                return Ok((Value::Nil, line_end + 2));
            }
            let count = usize::try_from(count)
                .map_err(|_| Error::Protocol(protocol::Error::InvalidLength))?;
            let mut values = Vec::with_capacity(count);
            let mut cursor = line_end + 2;
            for _ in 0..count {
                let (value, next) = build_at(frame, cursor, depth + 1)?;
                values.push(value);
                cursor = next;
            }
            Ok((Value::Array(values), cursor))
        }
        _ => Err(Error::Protocol(protocol::Error::UnknownMarker)),
    }
}

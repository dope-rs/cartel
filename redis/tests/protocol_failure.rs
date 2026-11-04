use cartel_redis::{
    protocol, protocol::parser, protocol::parser::capacity, protocol::parser::storage::Storage,
    value, value::source::Reader as _,
};
use common::{
    protocol::config, protocol::finish_chunk, protocol::new_codec, protocol::parse_chunk,
    protocol::staging_limit, protocol::try_with_budget, protocol::with_reply, protocol::with_state,
    rejected,
};
use dope::manifold::client::session::codec::Stateful as _;
use dope::manifold::client::session::{codec, response};
use o3::buffer::{storage::Shared, storage::range, write::consume::scope};
use std::cell::Cell;

pub mod common;

#[test]
fn source_reuse() {
    let bytes = b"$10\r\nabcdefghij\r\n";
    let expected = scope::Error::Range(range::Error::new(0..bytes.len(), bytes.len()));
    let mut input = rejected::Cursor::new(Shared::try_copy(bytes).expect("first input"));
    let next = Shared::try_copy(b"$1\r\nx\r\n").expect("second input");
    let store =
        Storage::try_new(capacity::Capacity::try_new(1, 2, 2).expect("capacity")).expect("storage");
    let codec = new_codec(config(32, 2).expect("limits"), &store);
    try_with_budget(|budget| {
        let mut state = codec.state(budget);
        let limit = staging_limit(&codec).expect("staging");
        let emitted = Cell::new(false);
        let step: codec::Step<response::Direct> =
            codec.decode(&mut input, &mut state, &budget, limit, |preview| {
                assert_eq!(preview.err(), Some(&expected));
                codec::Admission::Ready(
                    |decoded: Result<protocol::Decoded<'_, '_, '_>, scope::Error>,
                     _: &mut parser::Parser<'_, '_>| {
                        emitted.set(true);
                        assert_eq!(decoded.err(), Some(expected));
                    },
                )
            });
        assert!(emitted.get());
        assert!(!step.has_consumed());
        assert_eq!(input.as_ref(), bytes);
        let (rest, step) = parse_chunk(&codec, &mut state, &next, |decoded| {
            let frame = match decoded {
                Ok(protocol::Decoded::Frame(frame, _)) => Some(frame),
                _ => None,
            }
            .expect("second frame");
            assert_eq!(frame.value_count().get(), 1);
            let mut reader = frame.reader();
            assert!(matches!(
                reader.next_event(),
                Some(value::Event::Bulk(b"x"))
            ));
            assert!(reader.finish().is_ok());
        })
        .expect("second decode");
        assert!(step.has_consumed());
        assert!(rest.is_empty());
    })
    .expect("budget");
}

#[test]
fn bounds_structure() {
    with_reply(b"*1000000000\r\n", 128, 8, |decoded| {
        assert!(
            matches!(
                decoded,
                Ok(protocol::Decoded::Rejected(protocol::Failure::ValueCapacity(error)))
                    if error.required().get() == 1_000_000_001 && error.capacity().get() == 8
            ),
            "reply must exceed value capacity"
        );
    })
    .expect("protocol fixture");
}

#[test]
fn bounds_line() {
    with_reply(b"+OK\r\n", 5, 1, |decoded| {
        assert!(matches!(decoded, Ok(protocol::Decoded::Frame(..))));
    })
    .expect("exact frame boundary");
    with_reply(b"+toolong\r\n", 8, 8, |decoded| {
        assert!(
            matches!(
                decoded,
                Ok(protocol::Decoded::Rejected(protocol::Failure::FrameCapacity(error)))
                    if error.required().get() == 10 && error.capacity().get() == 8
            ),
            "complete oversized line must retain its frame failure"
        );
    })
    .expect("protocol fixture");
}

#[test]
fn bounds_marker() {
    with_reply(b"*1\r\n+", 4, 2, |decoded| {
        assert!(matches!(
            decoded,
            Ok(protocol::Decoded::Rejected(protocol::Failure::FrameCapacity(error)))
                if error.required().get() == 5 && error.capacity().get() == 4
        ));
    })
    .expect("marker beyond frame boundary");
}

#[test]
fn bounds_bulk() {
    with_reply(b"$1000\r\n", 128, 8, |decoded| {
        assert!(
            matches!(
                decoded,
                Ok(protocol::Decoded::Rejected(protocol::Failure::FrameCapacity(error)))
                    if error.required().get() == 1009 && error.capacity().get() == 128
            ),
            "reply must exceed frame capacity"
        );
    })
    .expect("protocol fixture");
}

#[test]
fn structure_overflow() {
    with_reply(b"*18446744073709551615\r\n", 128, 8, |decoded| {
        assert!(
            matches!(
                decoded,
                Ok(protocol::Decoded::Rejected(protocol::Failure::ValueCapacity(error)))
                    if error.required().get() == u64::MAX as u128 + 1 && error.capacity().get() == 8
            ),
            "overflowing array must exceed value capacity"
        );
    })
    .expect("protocol fixture");
}

#[test]
fn bulk_overflow() {
    with_reply(b"$18446744073709551615\r\n", 128, 8, |decoded| {
        assert!(
            matches!(
                decoded,
                Ok(protocol::Decoded::Rejected(protocol::Failure::FrameCapacity(error)))
                    if error.required().get() == u64::MAX as u128 + 25 && error.capacity().get() == 128
            ),
            "overflowing bulk must exceed frame capacity"
        );
    })
    .expect("protocol fixture");
}

#[test]
fn retains_invalid_facts() {
    with_reply(b"?", 8, 1, |decoded| {
        assert!(matches!(
            decoded,
            Ok(protocol::Decoded::Rejected(protocol::Failure::Invalid(
                protocol::Error::Marker {
                    offset: 0,
                    actual: b'?',
                }
            )))
        ));
    })
    .expect("protocol fixture");
    with_reply(b":12x\r\n", 16, 1, |decoded| {
        assert!(matches!(
            decoded,
            Ok(protocol::Decoded::Rejected(protocol::Failure::Invalid(
                protocol::Error::Number {
                    line: protocol::Line::Integer,
                    cause: protocol::Number::Byte {
                        offset: 3,
                        actual: b'x',
                    },
                }
            )))
        ));
    })
    .expect("protocol fixture");
    with_reply(b":9223372036854775808\r\n", 32, 1, |decoded| {
        assert!(matches!(
            decoded,
            Ok(protocol::Decoded::Rejected(protocol::Failure::Invalid(
                protocol::Error::Number {
                    line: protocol::Line::Integer,
                    cause: protocol::Number::Overflow { offset: 19 },
                }
            )))
        ));
    })
    .expect("protocol fixture");
    with_reply(b"$-2\r\n", 16, 1, |decoded| {
        assert!(matches!(
            decoded,
            Ok(protocol::Decoded::Rejected(protocol::Failure::Invalid(
                protocol::Error::Length {
                    line: protocol::Line::Bulk,
                    actual: -2,
                }
            )))
        ));
    })
    .expect("protocol fixture");
    for (bytes, offset, expected, actual) in [
        (b"$1\r\nx!\n".as_slice(), 5, b'\r', b'!'),
        (b"$1\r\nx!!".as_slice(), 5, b'\r', b'!'),
        (b"$1\r\nx\r!".as_slice(), 6, b'\n', b'!'),
        (b"+a\n".as_slice(), 2, b'\r', b'\n'),
        (b"-ERR a\r!".as_slice(), 7, b'\n', b'!'),
    ] {
        with_reply(bytes, 16, 1, |decoded| {
            assert!(matches!(
                decoded,
                Ok(protocol::Decoded::Rejected(protocol::Failure::Invalid(
                    protocol::Error::Terminator {
                        offset: found,
                        expected: marker,
                        actual: byte,
                    }
                ))) if found == offset && marker == expected && byte == actual
            ));
        })
        .expect("protocol fixture");
    }

    let nested = b"*1\r\n".repeat(parser::MAX_DEPTH + 1);
    let len = nested.len();
    with_reply(&nested, len, parser::MAX_DEPTH + 2, |decoded| {
        assert!(matches!(
            decoded,
            Ok(protocol::Decoded::Rejected(protocol::Failure::Invalid(
                protocol::Error::Depth {
                    actual,
                    maximum,
                    declared,
                }
            ))) if actual.get() == 65 && maximum.get() == 64 && declared.get() == 1
        ));
    })
    .expect("protocol fixture");
}

#[test]
fn retains_truncated_facts() {
    let bytes = b"$3\r\nx";
    let frame = (Shared::try_copy(bytes)).expect("test operation must succeed");
    let capacity = (capacity::Capacity::try_new(1, 8, 8)).expect("test operation must succeed");
    let store = (Storage::try_new(capacity)).expect("test operation must succeed");
    let limits = config(128, 8).expect("valid codec limits");
    let codec = new_codec(limits, &store);
    let emitted = Cell::new(false);
    with_state(&codec, |mut state| {
        let (rest, step) = parse_chunk(&codec, &mut state, &frame, |_| emitted.set(true))
            .expect("protocol fixture");
        assert!(!step.has_consumed(), "a truncated bulk must not commit");
        assert_eq!(rest.as_slice(), bytes);
        assert!(!emitted.get(), "a truncated bulk must not decode");
        let ((_, _state), next) = finish_chunk(&codec, state, &frame, |decoded| {
            assert!(
                matches!(
                    decoded,
                    Ok(protocol::Decoded::Rejected(protocol::Failure::Invalid(
                        protocol::Error::Truncated(protocol::Truncated::Bulk { actual, required })
                    ))) if actual.get() == 5 && required.get() == 9
                ),
                "finish must retain the exact truncated bulk extent"
            );
        })
        .expect("protocol fixture");
        assert!(
            matches!(next, Ok(codec::Finish::Item(response::Direct::Committed))),
            "finish must deliver one truncated item"
        );
    })
    .expect("protocol fixture");
}

#[test]
fn rejects_unparsed_eof() {
    let capacity = capacity::Capacity::try_new(1, 1, 1).expect("parser capacity");
    let store = Storage::try_new(capacity).expect("parser storage");
    let codec = new_codec(config(4, 1).expect("codec limits"), &store);
    with_state(&codec, |mut state| {
        for bytes in [b":1\r\n".as_slice(), b":12\r\n".as_slice()] {
            let frame = Shared::try_copy(bytes).expect("input frame");
            let ((_, parser), next) = finish_chunk(&codec, state, &frame, |decoded| {
                assert!(match (bytes.len(), decoded) {
                    (
                        4,
                        Ok(protocol::Decoded::Rejected(protocol::Failure::Invalid(
                            protocol::Error::Unparsed { actual },
                        ))),
                    ) => actual.get() == 4,
                    (
                        5,
                        Ok(protocol::Decoded::Rejected(protocol::Failure::FrameCapacity(error))),
                    ) => error.required().get() == 5 && error.capacity().get() == 4,
                    _ => false,
                });
            })
            .expect("protocol fixture");
            state = parser;
            assert!(matches!(
                next,
                Ok(codec::Finish::Item(response::Direct::Committed))
            ));
        }
    })
    .expect("protocol fixture");
}

#[test]
fn eof_depth() {
    let input = Shared::try_copy(&b"*1\r\n".repeat(parser::MAX_DEPTH)).expect("input");
    let store = Storage::try_new(
        capacity::Capacity::try_new(1, parser::MAX_DEPTH + 1, 0).expect("capacity"),
    )
    .expect("storage");
    let codec = new_codec(config(512, parser::MAX_DEPTH + 1).expect("limits"), &store);
    with_state(&codec, |mut state| {
        let emitted = Cell::new(false);
        let (_, step) =
            parse_chunk(&codec, &mut state, &input, |_| emitted.set(true)).expect("decode");
        assert!(!step.has_consumed());
        assert!(!emitted.get());
        let ((_, _state), next) = finish_chunk(&codec, state, &input, |decoded| {
            assert!(matches!(decoded,
                Ok(protocol::Decoded::Rejected(protocol::Failure::Invalid(
                    protocol::Error::Truncated(protocol::Truncated::Value {
                        offset, depth, remaining,
                    })
                ))) if offset.get() as usize == input.len()
                    && depth.get() as usize == parser::MAX_DEPTH
                    && remaining.get() == 1
            ));
        })
        .expect("finish");
        assert!(matches!(
            next,
            Ok(codec::Finish::Item(response::Direct::Committed))
        ));
    })
    .expect("state");
}

use cartel_redis::value::source::Reader as _;
use cartel_redis::{
    protocol, protocol::parser::capacity, protocol::parser::storage::Storage, value,
};
use common::{
    protocol::config, protocol::new_codec, protocol::parse_chunk, protocol::with_reply,
    protocol::with_state,
};
use o3::buffer::storage::Shared;
use std::cell::Cell;

pub mod common;

#[test]
fn resumes_nested() {
    let bytes = b"*4\r\n+OK\r\n-ERR fixture\r\n$3\r\n\r\n\0\r\n:7\r\n";
    let parts = ((1..bytes.len())
        .map(|end| Shared::try_copy(&bytes[..end]))
        .collect::<Result<Vec<_>, _>>())
    .expect("fragment allocations");
    let frame = (Shared::try_copy(bytes)).expect("test operation must succeed");
    let capacity = (capacity::Capacity::try_new(1, 8, 8)).expect("test operation must succeed");
    let store = (Storage::try_new(capacity)).expect("test operation must succeed");
    let limits = config(128, 8).expect("valid codec limits");
    let codec = new_codec(limits, &store);
    let emitted = Cell::new(false);
    with_state(&codec, |mut state| {
        for part in &parts {
            let (rest, step) = parse_chunk(&codec, &mut state, part, |_| emitted.set(true))
                .expect("protocol fixture");
            assert!(!step.has_consumed(), "a partial reply must not commit");
            assert_eq!(rest.as_slice(), part.as_slice());
            assert!(!emitted.get(), "a partial reply must not decode");
        }
        let (rest, step) = parse_chunk(&codec, &mut state, &frame, |decoded| {
            emitted.set(true);
            let frame = match decoded {
                Ok(protocol::Decoded::Frame(frame, _)) => Some(frame),
                _ => None,
            }
            .expect("complete reply must decode");
            assert_eq!(frame.value_count().get(), 5);
            let mut partial = frame.reader();
            assert!(matches!(
                partial.next_event(),
                Some(value::Event::Array { len: 4 })
            ));
            assert!(matches!(
                partial.finish(),
                Err(protocol::Error::Values { remaining: rest }) if rest.get() == 4
            ));
            let mut reader = frame.reader();
            assert!(matches!(
                reader.next_event(),
                Some(value::Event::Array { len: 4 })
            ));
            assert!(matches!(reader.next_event(), Some(value::Event::Ok)));
            assert!(matches!(
                reader.next_event(),
                Some(value::Event::Error(b"ERR fixture"))
            ));
            assert!(matches!(
                reader.next_event(),
                Some(value::Event::Bulk(value)) if value == b"\r\n\0"
            ));
            assert!(matches!(
                reader.next_event(),
                Some(value::Event::Integer(7))
            ));
            assert!(reader.finish().is_ok(), "all reply values consumed");
        })
        .expect("protocol fixture");
        assert!(emitted.get(), "complete reply must decode");
        assert!(step.has_consumed(), "complete reply must commit its input");
        assert!(
            rest.is_empty(),
            "complete reply must consume its exact extent"
        );
    })
    .expect("protocol fixture");
}

#[test]
fn accepts_i64_min() {
    use protocol::{Line, Number};

    for (bytes, expected) in [
        (b":-9223372036854775808\r\n".as_slice(), Ok(i64::MIN)),
        (b":+0\r\n", Ok(0)),
        (b":+9223372036854775807\r\n", Ok(i64::MAX)),
        (
            b":+\r\n",
            Err((Line::Integer, Number::Missing { offset: 2 })),
        ),
        (
            b":+-1\r\n",
            Err((
                Line::Integer,
                Number::Byte {
                    offset: 2,
                    actual: b'-',
                },
            )),
        ),
        (
            b":+9223372036854775808\r\n",
            Err((Line::Integer, Number::Overflow { offset: 20 })),
        ),
        (
            b"$+0\r\n",
            Err((
                Line::Bulk,
                Number::Byte {
                    offset: 1,
                    actual: b'+',
                },
            )),
        ),
        (
            b"*+0\r\n",
            Err((
                Line::Array,
                Number::Byte {
                    offset: 1,
                    actual: b'+',
                },
            )),
        ),
    ] {
        with_reply(bytes, 64, 1, |decoded| {
            assert!(
                match (decoded, expected) {
                    (Ok(protocol::Decoded::Frame(frame, _)), Ok(expected)) => {
                        let mut reader = frame.reader();
                        matches!(reader.next_event(), Some(value::Event::Integer(actual)) if actual == expected)
                            && reader.finish().is_ok()
                    }
                    (
                        Ok(protocol::Decoded::Rejected(protocol::Failure::Invalid(
                            protocol::Error::Number { line, cause },
                        ))),
                        Err((expected_line, expected)),
                    ) => {
                        std::mem::discriminant(&line) == std::mem::discriminant(&expected_line)
                            && match (cause, expected) {
                                (
                                    Number::Missing { offset },
                                    Number::Missing { offset: expected },
                                )
                                | (
                                    Number::Overflow { offset },
                                    Number::Overflow { offset: expected },
                                ) => offset == expected,
                                (
                                    Number::Byte { offset, actual },
                                    Number::Byte {
                                        offset: expected,
                                        actual: byte,
                                    },
                                ) => offset == expected && actual == byte,
                                _ => false,
                            }
                    }
                    _ => false,
                },
                "integer and length sign contracts for {bytes:?}"
            );
        })
        .expect("protocol fixture");
    }
}

#[test]
fn decodes_coordinates() {
    with_reply(
        b"*2\r\n*2\r\n$5\r\n127.5\r\n$5\r\n37.25\r\n$-1\r\n",
        128,
        5,
        |decoded| {
            let frame = match decoded {
                Ok(protocol::Decoded::Frame(frame, _)) => Some(frame),
                _ => None,
            }
            .expect("coordinate reply must decode");
            let mut reader = frame.reader();
            assert!(matches!(
                reader.next_event(),
                Some(value::Event::Array { len: 2 })
            ));
            assert!(matches!(
                reader.next_event(),
                Some(value::Event::Array { len: 2 })
            ));
            assert!(matches!(
                reader.next_event(),
                Some(value::Event::Bulk(b"127.5"))
            ));
            assert!(matches!(
                reader.next_event(),
                Some(value::Event::Bulk(b"37.25"))
            ));
            assert!(matches!(reader.next_event(), Some(value::Event::Nil)));
            assert!(reader.next_event().is_none());
            assert!(reader.finish().is_ok(), "all reply values consumed");
        },
    )
    .expect("protocol fixture");
}

#[test]
fn decodes_arrays() {
    with_reply(
        b"*4\r\n$3\r\none\r\n$1\r\n1\r\n$3\r\ntwo\r\n$3\r\n2.5\r\n",
        128,
        5,
        |decoded| {
            let frame = match decoded {
                Ok(protocol::Decoded::Frame(frame, _)) => Some(frame),
                _ => None,
            }
            .expect("member-score reply must decode");
            let mut reader = frame.reader();
            assert!(matches!(
                reader.next_event(),
                Some(value::Event::Array { len: 4 })
            ));
            for bytes in [&b"one"[..], b"1", b"two", b"2.5"] {
                assert!(
                    matches!(reader.next_event(), Some(value::Event::Bulk(actual)) if actual == bytes)
                );
            }
            assert!(reader.next_event().is_none());
            assert!(reader.finish().is_ok(), "all reply values consumed");
        },
    )
    .expect("protocol fixture");
}

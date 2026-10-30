use cartel_redis::value::{self, source::Reader as _};
use cartel_redis::{protocol, protocol::parser::capacity, protocol::parser::storage::Storage};
use common::{protocol::config, protocol::new_codec, protocol::parse_chunk, protocol::with_state};
use o3::buffer::storage::Shared;

pub mod common;

#[test]
fn pairs_frames() {
    let first = Shared::try_copy(b"$3\r\none\r\n").expect("first input");
    let second = Shared::try_copy(b"$3\r\ntwo\r\n").expect("second input");
    let one = first[4..].as_ptr();
    let two = second[4..].as_ptr();
    let store =
        Storage::try_new(capacity::Capacity::try_new(2, 1, 2).expect("capacity")).expect("storage");
    let codec = new_codec(config(16, 1).expect("limits"), &store);
    with_state(&codec, |mut state| {
        let (_, step) = parse_chunk(&codec, &mut state, &first, |decoded| {
            let first = match decoded {
                Ok(protocol::Decoded::Frame(frame, _)) => Some(frame),
                _ => None,
            }
            .expect("first frame");
            with_state(&codec, |mut state| {
                let (_, step) = parse_chunk(&codec, &mut state, &second, |decoded| {
                    let second = match decoded {
                        Ok(protocol::Decoded::Frame(frame, _)) => Some(frame),
                        _ => None,
                    }
                    .expect("second frame");
                    let mut left = first.reader();
                    let mut right = second.reader();
                    assert!(matches!(left.next_event(), Some(value::Event::Bulk(bytes))
                        if bytes == b"one" && bytes.as_ptr() == one));
                    assert!(matches!(right.next_event(), Some(value::Event::Bulk(bytes))
                        if bytes == b"two" && bytes.as_ptr() == two));
                })
                .expect("second decode");
                assert!(step.has_consumed());
            })
            .expect("second state");
        })
        .expect("first decode");
        assert!(step.has_consumed());
    })
    .expect("first state");
}

#[test]
fn borrows_small_frame() {
    let mut input = vec![0; 256 * 1024];
    input[..10].copy_from_slice(b"+OK\r\n:17\r\n");
    let shared = (Shared::try_copy(&input)).expect("test operation must succeed");
    let capacity = (capacity::Capacity::try_new(1, 1, 1)).expect("test operation must succeed");
    let store = (Storage::try_new(capacity)).expect("test operation must succeed");
    let limits = config(256 * 1024, 1).expect("valid codec limits");
    let codec = new_codec(limits, &store);
    let source = shared.as_ptr();
    with_state(&codec, |mut state| {
        let mut emitted = false;
        let (rest, step) = parse_chunk(&codec, &mut state, &shared, |decoded| {
            emitted = true;
            let frame = match decoded {
                Ok(protocol::Decoded::Frame(frame, _)) => Some(frame),
                _ => None,
            }
            .expect("small frame must decode");
            assert_eq!(frame.as_slice().len(), 5);
            assert_eq!(frame.as_slice().as_ptr(), source);
        })
        .expect("protocol fixture");
        assert!(emitted, "small frame must decode");
        assert!(step.has_consumed(), "small frame must commit its input");
        assert_eq!(shared.len() - rest.len(), 5);
        assert_eq!(rest.as_slice(), &input[5..]);
    })
    .expect("protocol fixture");
}

#[test]
fn borrows_large_frame() {
    let body = 80 * 1024;
    let prefix = format!("${body}\r\n");
    let len = prefix.len() + body + 2;
    let mut input = vec![0; 256 * 1024];
    input[..prefix.len()].copy_from_slice(prefix.as_bytes());
    input[prefix.len()..prefix.len() + body].fill(b'x');
    input[len - 2..len].copy_from_slice(b"\r\n");
    input[len..len + 5].copy_from_slice(b":17\r\n");

    let shared = (Shared::try_copy(&input)).expect("test operation must succeed");
    let capacity = (capacity::Capacity::try_new(1, 1, 1)).expect("test operation must succeed");
    let store = (Storage::try_new(capacity)).expect("test operation must succeed");
    let limits = config(256 * 1024, 1).expect("valid codec limits");
    let codec = new_codec(limits, &store);
    let source = shared.as_ptr();
    with_state(&codec, |mut state| {
        let mut emitted = false;
        let (rest, step) = parse_chunk(&codec, &mut state, &shared, |decoded| {
            emitted = true;
            let frame = match decoded {
                Ok(protocol::Decoded::Frame(frame, _)) => Some(frame),
                _ => None,
            }
            .expect("large frame must decode");
            assert_eq!(frame.as_slice().len(), len);
            assert_eq!(frame.as_slice().as_ptr(), source);
        })
        .expect("protocol fixture");
        assert!(emitted, "large frame must decode");
        assert!(step.has_consumed(), "large frame must commit its input");
        assert_eq!(shared.len() - rest.len(), len);
        assert_eq!(rest.as_slice(), &input[len..]);
    })
    .expect("protocol fixture");
}

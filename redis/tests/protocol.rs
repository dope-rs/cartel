use cartel_redis::protocol::{Codec, Head};
use cartel_redis::{FromValue, GeoCoord, Value};
use dope::manifold::connector::Codec as _;
use o3::buffer::Shared;

fn parse(bytes: &[u8], frame_capacity: usize, value_capacity: usize) -> Head {
    let codec = Codec::new(frame_capacity, value_capacity);
    let mut state = Default::default();
    let frame = Shared::copy_from_slice(bytes);
    let (head, consumed) = codec.parse(&mut state, &frame).expect("complete frame");
    assert_eq!(consumed, bytes.len());
    head
}

#[test]
fn parser_resumes_partial_nested_frame() {
    let bytes = b"*2\r\n$3\r\nfoo\r\n:7\r\n";
    let codec = Codec::new(128, 8);
    let mut state = Default::default();
    for end in 1..bytes.len() {
        let partial = Shared::copy_from_slice(&bytes[..end]);
        assert!(codec.parse(&mut state, &partial).is_none());
    }
    let frame = Shared::copy_from_slice(bytes);
    let (Head::Reply(frame), consumed) = codec.parse(&mut state, &frame).expect("reply") else {
        panic!("expected reply")
    };
    assert_eq!(consumed, bytes.len());
    assert_eq!(frame.value_count(), 3);
    let Value::Array(values) = frame.into_value().expect("value") else {
        panic!("expected array")
    };
    assert!(matches!(&values[0], Value::Bulk(value) if value.as_slice() == b"foo"));
    assert!(matches!(values[1], Value::Integer(7)));
}

#[test]
fn parser_bounds_declared_structure_before_materialization() {
    let Head::Fatal(cartel_redis::Error::ResponseValueCapacity) = parse(b"*1000000000\r\n", 128, 8)
    else {
        panic!("expected value capacity error")
    };
}

#[test]
fn parser_bounds_declared_bulk_before_payload_arrives() {
    let Head::Fatal(cartel_redis::Error::ResponseFrameCapacity) = parse(b"$1000\r\n", 128, 8)
    else {
        panic!("expected frame capacity error")
    };
}

#[test]
fn parser_accepts_i64_minimum() {
    let Head::Reply(frame) = parse(b":-9223372036854775808\r\n", 64, 1) else {
        panic!("expected reply")
    };
    assert!(matches!(
        frame.into_value().expect("integer"),
        Value::Integer(i64::MIN)
    ));
}

#[test]
fn coordinates_use_the_shared_value_decoder() {
    let coordinates = Vec::<Option<GeoCoord>>::from_value(Value::Array(vec![
        Value::Array(vec![
            Value::Bulk(Shared::copy_from_slice(b"127.5")),
            Value::Bulk(Shared::copy_from_slice(b"37.25")),
        ]),
        Value::Nil,
    ]))
    .expect("coordinates");

    assert_eq!(coordinates, vec![Some(GeoCoord::new(127.5, 37.25)), None]);
    assert!(
        GeoCoord::from_value(Value::Array(vec![Value::Bulk(Shared::copy_from_slice(
            b"127.5",
        ))]))
        .is_err()
    );
}

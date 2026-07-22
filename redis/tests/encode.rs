#![allow(dead_code)]

#[path = "../src/encode.rs"]
mod encode;

fn encode_frame(encode: impl FnOnce(&mut Vec<u8>)) -> Vec<u8> {
    let mut buffer = Vec::with_capacity(256);
    encode(&mut buffer);
    buffer
}

#[test]
fn command_encoding_uses_resp_arrays() {
    assert_eq!(
        encode_frame(|buffer| encode::cmd_get(buffer, b"foo")),
        b"*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n",
    );
    assert_eq!(
        encode_frame(|buffer| encode::cmd_incrby(buffer, b"counter", -42)),
        b"*3\r\n$6\r\nINCRBY\r\n$7\r\ncounter\r\n$3\r\n-42\r\n",
    );
}

#[test]
fn option_encoding_preserves_command_stem() {
    assert_eq!(
        encode_frame(|buffer| encode::cmd_zrange_with_scores(buffer, b"lb", 0, -1)),
        b"*5\r\n$6\r\nZRANGE\r\n$2\r\nlb\r\n$1\r\n0\r\n$2\r\n-1\r\n$10\r\nWITHSCORES\r\n",
    );
}

#[test]
fn string_and_collection_commands_encode_every_argument() {
    assert_eq!(
        encode_frame(|buffer| encode::cmd_set_ex(buffer, b"k", b"v", 60)),
        b"*5\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n$2\r\nEX\r\n$2\r\n60\r\n",
    );
    assert_eq!(
        encode_frame(|buffer| encode::cmd_del(buffer, &[b"a", b"bb", b"ccc"])),
        b"*4\r\n$3\r\nDEL\r\n$1\r\na\r\n$2\r\nbb\r\n$3\r\nccc\r\n",
    );
    assert_eq!(
        encode_frame(|buffer| encode::cmd_raw(buffer, &[b"CLUSTER", b"SLOTS"])),
        b"*2\r\n$7\r\nCLUSTER\r\n$5\r\nSLOTS\r\n",
    );
}

#[test]
fn hash_and_set_commands_encode_repeated_arguments() {
    assert_eq!(
        encode_frame(|buffer| {
            encode::cmd_hset_pairs(buffer, b"u:1", &[(b"name", b"alice"), (b"age", b"30")])
        }),
        b"*6\r\n$4\r\nHSET\r\n$3\r\nu:1\r\n$4\r\nname\r\n$5\r\nalice\r\n$3\r\nage\r\n$2\r\n30\r\n",
    );
    assert_eq!(
        encode_frame(|buffer| encode::cmd_hmget(buffer, b"u:1", &[b"name", b"age"])),
        b"*4\r\n$5\r\nHMGET\r\n$3\r\nu:1\r\n$4\r\nname\r\n$3\r\nage\r\n",
    );
    assert_eq!(
        encode_frame(|buffer| encode::cmd_hincrby(buffer, b"counters", b"a", -5)),
        b"*4\r\n$7\r\nHINCRBY\r\n$8\r\ncounters\r\n$1\r\na\r\n$2\r\n-5\r\n",
    );
    assert_eq!(
        encode_frame(|buffer| encode::cmd_sadd(buffer, b"tags", &[b"a", b"b"])),
        b"*4\r\n$4\r\nSADD\r\n$4\r\ntags\r\n$1\r\na\r\n$1\r\nb\r\n",
    );
}

#[test]
fn sorted_set_and_list_commands_preserve_numeric_arguments() {
    assert_eq!(
        encode_frame(|buffer| encode::cmd_zadd(buffer, b"lb", 3.5, b"alice")),
        b"*4\r\n$4\r\nZADD\r\n$2\r\nlb\r\n$3\r\n3.5\r\n$5\r\nalice\r\n",
    );
    assert_eq!(
        encode_frame(|buffer| encode::cmd_lpush(buffer, b"q", &[b"x", b"y"])),
        b"*4\r\n$5\r\nLPUSH\r\n$1\r\nq\r\n$1\r\nx\r\n$1\r\ny\r\n",
    );
    assert_eq!(
        encode_frame(|buffer| encode::cmd_lrange(buffer, b"q", 0, -1)),
        b"*4\r\n$6\r\nLRANGE\r\n$1\r\nq\r\n$1\r\n0\r\n$2\r\n-1\r\n",
    );
}

#[test]
fn floating_arguments_cover_finite_and_non_finite_boundaries() {
    for (value, encoded) in [
        (-0.0, "0"),
        (f64::NAN, "nan"),
        (f64::INFINITY, "inf"),
        (f64::NEG_INFINITY, "-inf"),
        (f64::MAX, "1.7976931348623157e308"),
        (f64::from_bits(1), "5e-324"),
    ] {
        let frame = encode_frame(|buffer| encode::cmd_incrbyfloat(buffer, b"k", value));
        let expected = format!(
            "*3\r\n$11\r\nINCRBYFLOAT\r\n$1\r\nk\r\n${}\r\n{encoded}\r\n",
            encoded.len()
        );
        assert_eq!(frame, expected.as_bytes());
    }
}

#[test]
fn scan_and_key_commands_cover_optional_shapes() {
    assert_eq!(
        encode_frame(|buffer| encode::cmd_scan(buffer, 0, Some(b"user:*"), Some(100))),
        b"*6\r\n$4\r\nSCAN\r\n$1\r\n0\r\n$5\r\nMATCH\r\n$6\r\nuser:*\r\n$5\r\nCOUNT\r\n$3\r\n100\r\n",
    );
    assert_eq!(
        encode_frame(|buffer| encode::cmd_scan(buffer, 42, None, None)),
        b"*2\r\n$4\r\nSCAN\r\n$2\r\n42\r\n",
    );
    assert_eq!(
        encode_frame(|buffer| encode::cmd_type(buffer, b"k")),
        b"*2\r\n$4\r\nTYPE\r\n$1\r\nk\r\n",
    );
    assert_eq!(
        encode_frame(|buffer| encode::cmd_rename(buffer, b"a", b"b")),
        b"*3\r\n$6\r\nRENAME\r\n$1\r\na\r\n$1\r\nb\r\n",
    );
}

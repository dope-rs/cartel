const SCAN: &[u8] = b"*2\r\n$4\r\nSCAN\r\n$1\r\n0\r\n";
const EVAL: &[u8] = b"*3\r\n$4\r\nEVAL\r\n$1\r\n0\r\n$1\r\n0\r\n";

pub const SCRIPT: [(&[u8], &[u8]); 7] = [
    (SCAN, b"*2\r\n$20\r\n18446744073709551615\r\n*0\r\n"),
    (SCAN, b"*2\r\n$2\r\n-1\r\n*0\r\n"),
    (EVAL, b"$20\r\n18446744073709551615\r\n"),
    (EVAL, b"+18446744073709551615\r\n"),
    (EVAL, b"$20\r\n18446744073709551616\r\n"),
    (EVAL, b"$2\r\n-1\r\n"),
    (EVAL, b"$20\r\n-9223372036854775808\r\n"),
];

use dope_test::harness::peer;

use cartel_redis::client::ops::scripts::Invocation;
use cartel_redis::client::settings::init::Step;

const AUTH: &[u8] = b"*3\r\n$4\r\nAUTH\r\n$5\r\nalice\r\n$9\r\nswordfish\r\n";

const SELECT: &[u8] = b"*2\r\n$6\r\nSELECT\r\n$1\r\n3\r\n";

const SETNAME: &[u8] = b"*3\r\n$6\r\nCLIENT\r\n$7\r\nSETNAME\r\n$6\r\nworker\r\n";

const EVAL: &[u8] = b"*4\r\n$4\r\nEVAL\r\n$8\r\nreturn 0\r\n$1\r\n0\r\n$5\r\nscope\r\n";

pub struct Verify<'a>(pub [&'a [u8]; 1]);

impl<'a> Invocation for Verify<'a> {
    type Key = &'static [u8];
    type Argument = &'a [u8];

    fn script(&self) -> &[u8] {
        b"return 0"
    }

    fn keys(&self) -> &[Self::Key] {
        &[]
    }

    fn arguments(&self) -> &[Self::Argument] {
        &self.0
    }
}

pub const PING: &[u8] = b"*1\r\n$4\r\nPING\r\n";

pub const REJECTED: &[u8] = b"-ERR rejected handshake stage\r\n";
pub const DENIED: &[u8] = b"-ERR invalid credentials\r\n";

pub const REQUESTS: [(&[u8], Step); 4] = [
    (AUTH, Step::Authenticate),
    (SELECT, Step::Select),
    (SETNAME, Step::SetClientName),
    (EVAL, Step::Eval),
];

pub fn read_request(socket: &peer::Stream<'_>, len: usize) -> Result<Vec<u8>, peer::Error> {
    let mut bytes = vec![0; len];
    socket.read_exact(&mut bytes)?;
    Ok(bytes)
}

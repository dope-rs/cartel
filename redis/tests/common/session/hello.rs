use dope_test::harness::peer;

use cartel_redis::client::settings::init::Step;

const AUTH: &[u8] = b"*3\r\n$4\r\nAUTH\r\n$5\r\nalice\r\n$9\r\nswordfish\r\n";

const SELECT: &[u8] = b"*2\r\n$6\r\nSELECT\r\n$1\r\n3\r\n";

const SETNAME: &[u8] = b"*3\r\n$6\r\nCLIENT\r\n$7\r\nSETNAME\r\n$6\r\nworker\r\n";

pub const PING: &[u8] = b"*1\r\n$4\r\nPING\r\n";

pub const REJECTED: &[u8] = b"-ERR rejected handshake stage\r\n";
pub const DENIED: &[u8] = b"-ERR invalid credentials\r\n";

pub const REQUESTS: [(&[u8], Step); 3] = [
    (AUTH, Step::Authenticate),
    (SELECT, Step::Select),
    (SETNAME, Step::SetClientName),
];

pub fn read_request(socket: &peer::Stream<'_>, len: usize) -> Result<Vec<u8>, peer::Error> {
    let mut bytes = vec![0; len];
    socket.read_exact(&mut bytes)?;
    Ok(bytes)
}

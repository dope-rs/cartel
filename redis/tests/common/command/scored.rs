pub const REQUEST: &[u8] =
    b"*5\r\n$9\r\nZREVRANGE\r\n$5\r\nboard\r\n$1\r\n0\r\n$2\r\n-1\r\n$10\r\nWITHSCORES\r\n";

#[derive(Clone, Copy)]
pub enum Scored {
    Valid,
    Odd,
    Member,
    Score,
    ScoreEncoding,
    Text,
}

impl Scored {
    pub const fn reply<'a>(self) -> &'a [u8] {
        match self {
            Self::Valid => b"*4\r\n$5\r\nalice\r\n$4\r\n42.5\r\n$3\r\nbob\r\n$2\r\n-7\r\n",
            Self::Odd => b"*3\r\n$5\r\nalice\r\n$4\r\n42.5\r\n$3\r\nbob\r\n",
            Self::Member => b"*2\r\n+alice\r\n$4\r\n42.5\r\n",
            Self::Score => b"*2\r\n$5\r\nalice\r\n$3\r\nx.y\r\n",
            Self::ScoreEncoding => b"*2\r\n$5\r\nalice\r\n$2\r\n+\xff\r\n",
            Self::Text => b"*4\r\n$5\r\nalice\r\n$4\r\n42.5\r\n$1\r\n\xff\r\n$2\r\n-7\r\n",
        }
    }
}

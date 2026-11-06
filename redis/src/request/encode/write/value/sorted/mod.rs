use crate::request::encode;

impl encode::Encoder {
    pub(crate) fn float<const EXISTS: bool>(
        self,
        verb: &[u8],
        key: &[u8],
        value: f64,
        member: &[u8],
    ) -> Result<encode::Complete, crate::request::Error> {
        let mut buffer = ryu::Buffer::new();
        let value = encode::format_float(&mut buffer, value);
        if EXISTS {
            self.command(&[verb, key, b"XX", value, member])
        } else {
            self.command(&[verb, key, value, member])
        }
    }
}

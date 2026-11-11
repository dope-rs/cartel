mod pending;

pub(crate) use pending::Pending;

pub(crate) fn frame(bytes: &'static [u8]) -> dope::net::link::egress::NonEmpty<&'static [u8]> {
    dope::net::link::egress::NonEmpty::try_new(bytes).expect("nonempty frame")
}

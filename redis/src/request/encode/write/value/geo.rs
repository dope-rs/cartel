use crate::request::encode;

#[repr(transparent)]
pub(crate) struct Geo(encode::Encoder);

impl Geo {
    pub(crate) const fn new(out: encode::Encoder) -> Self {
        Self(out)
    }

    pub(crate) fn add(
        self,
        key: &[u8],
        lon: f64,
        lat: f64,
        member: &[u8],
    ) -> Result<encode::Complete, crate::request::Error> {
        let mut first = ryu::Buffer::new();
        let lon = encode::format_float(&mut first, lon);
        let mut second = ryu::Buffer::new();
        let lat = encode::format_float(&mut second, lat);
        self.0.command(&[b"GEOADD", key, lon, lat, member])
    }

    pub(crate) fn distance(
        self,
        key: &[u8],
        first: &[u8],
        second: &[u8],
        unit: Option<&[u8]>,
    ) -> Result<encode::Complete, crate::request::Error> {
        match unit {
            Some(unit) => self.0.command(&[b"GEODIST", key, first, second, unit]),
            None => self.0.command(&[b"GEODIST", key, first, second]),
        }
    }

    pub(crate) fn positions(
        self,
        key: &[u8],
        members: &[&[u8]],
    ) -> Result<encode::Complete, crate::request::Error> {
        self.0.slices(&[b"GEOPOS", key], members)
    }

    pub(crate) fn search(
        self,
        key: &[u8],
        lon: f64,
        lat: f64,
        radius: f64,
        unit: &[u8],
    ) -> Result<encode::Complete, crate::request::Error> {
        let mut first = ryu::Buffer::new();
        let lon = encode::format_float(&mut first, lon);
        let mut second = ryu::Buffer::new();
        let lat = encode::format_float(&mut second, lat);
        let mut distance = ryu::Buffer::new();
        let radius = encode::format_float(&mut distance, radius);
        self.0.command(&[
            b"GEOSEARCH",
            key,
            b"FROMLONLAT",
            lon,
            lat,
            b"BYRADIUS",
            radius,
            unit,
        ])
    }
}

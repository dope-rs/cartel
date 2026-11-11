use crate::api::{Commands, decode};
use crate::request::encode;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Unit {
    Meters,
    Kilometers,
    Miles,
    Feet,
}

impl Unit {
    const fn as_bytes(&self) -> &[u8] {
        match self {
            Self::Meters => b"m",
            Self::Kilometers => b"km",
            Self::Miles => b"mi",
            Self::Feet => b"ft",
        }
    }
}

#[derive(Clone, Copy)]
#[repr(transparent)]
pub struct Geo<C, const ID: u8 = 0>(pub(crate) C);

impl<'d, C: Commands<'d, ID>, const ID: u8> Geo<C, ID> {
    pub fn add(self, key: &[u8], lon: f64, lat: f64, member: &[u8]) -> C::Call<u64> {
        self.0.encode::<u64>(move |out| {
            let mut first = ryu::Buffer::new();
            let lon = encode::format_float(&mut first, lon);
            let mut second = ryu::Buffer::new();
            let lat = encode::format_float(&mut second, lat);
            out.command(&[b"GEOADD", key, lon, lat, member])
        })
    }

    pub fn distance(
        self,
        key: &[u8],
        first: &[u8],
        second: &[u8],
        unit: Option<Unit>,
    ) -> C::Call<Option<f64>> {
        self.0.encode::<Option<f64>>(move |out| {
            let unit = unit.as_ref().map(Unit::as_bytes);
            match unit {
                Some(unit) => out.command(&[b"GEODIST", key, first, second, unit]),
                None => out.command(&[b"GEODIST", key, first, second]),
            }
        })
    }

    pub fn positions(self, key: &[u8], members: &[&[u8]]) -> C::Call<decode::Coordinates> {
        self.0
            .encode::<decode::Coordinates>(move |out| out.slices(&[b"GEOPOS", key], members))
    }

    pub fn search(
        self,
        key: &[u8],
        lon: f64,
        lat: f64,
        radius: f64,
        unit: Unit,
    ) -> C::Call<decode::Bulks> {
        self.0.encode::<decode::Bulks>(move |out| {
            let unit = unit.as_bytes();
            let mut first = ryu::Buffer::new();
            let lon = encode::format_float(&mut first, lon);
            let mut second = ryu::Buffer::new();
            let lat = encode::format_float(&mut second, lat);
            let mut distance = ryu::Buffer::new();
            let radius = encode::format_float(&mut distance, radius);
            out.command(&[
                b"GEOSEARCH",
                key,
                b"FROMLONLAT",
                lon,
                lat,
                b"BYRADIUS",
                radius,
                unit,
            ])
        })
    }
}

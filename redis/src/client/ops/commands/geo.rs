use crate::api::{Client, decode};
use crate::request::encode::write::value::geo;

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

impl<'d, C: Client<'d, ID>, const ID: u8> Geo<C, ID> {
    pub fn add(self, key: &[u8], lon: f64, lat: f64, member: &[u8]) -> C::Call<u64> {
        self.0
            .encode::<u64>(move |out| geo::Geo::new(out).add(key, lon, lat, member))
    }

    pub fn distance(
        self,
        key: &[u8],
        first: &[u8],
        second: &[u8],
        unit: Option<Unit>,
    ) -> C::Call<Option<f64>> {
        self.0.encode::<Option<f64>>(move |out| {
            geo::Geo::new(out).distance(key, first, second, unit.as_ref().map(Unit::as_bytes))
        })
    }

    pub fn positions(self, key: &[u8], members: &[&[u8]]) -> C::Call<decode::Coordinates> {
        self.0
            .encode::<decode::Coordinates>(move |out| geo::Geo::new(out).positions(key, members))
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
            geo::Geo::new(out).search(key, lon, lat, radius, unit.as_bytes())
        })
    }
}

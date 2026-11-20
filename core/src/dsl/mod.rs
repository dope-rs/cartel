pub mod expr;
pub mod query;
pub mod window;

pub struct Scope(pub std::convert::Infallible);

impl Scope {
    pub fn value<T>(&self) -> T {
        match self.0 {}
    }

    pub fn rows<T>(&self, _: T) -> Vec<T> {
        match self.0 {}
    }

    pub fn stream<T>(&self, _: T) -> query::Stream<T> {
        match self.0 {}
    }

    pub fn target<T>(&self, _: &mut T) -> crate::sql::Target<T> {
        match self.0 {}
    }
}

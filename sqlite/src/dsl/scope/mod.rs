use crate::dsl::window;
use std::convert;

/// Uninhabited proof supplied only to generated query checks.
pub struct Scope(pub convert::Infallible);

impl Scope {
    pub fn target<T>(&self, _: &mut T) -> crate::sql::Target<T> {
        match self.0 {}
    }

    pub fn has_rows<S>(&self, _: S) -> bool {
        match self.0 {}
    }

    pub fn has_no_rows<S>(&self, _: S) -> bool {
        match self.0 {}
    }

    pub fn row_number(&self) -> window::Expr<i64> {
        match self.0 {}
    }
}

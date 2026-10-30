pub(super) mod never;

use crate::dsl::{expr, shape, window};
use std::convert;

/// Uninhabited proof supplied only to generated query checks.
pub struct Scope(pub convert::Infallible);

impl Scope {
    pub fn element<S: shape::ColSlice>(&self, _: &S) -> S::Elem {
        match self.0 {}
    }

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

    pub fn count<T>(&self, _: T) -> window::Expr<i64> {
        match self.0 {}
    }

    pub fn lag<T, R>(&self, _: T, _: i64) -> window::Expr<Option<R>> {
        match self.0 {}
    }

    pub fn lead<T, R>(&self, _: T, _: i64) -> window::Expr<Option<R>> {
        match self.0 {}
    }

    pub fn vector<S: AsRef<str>>(&self, _: S) -> expr::TsVector {
        match self.0 {}
    }

    pub fn query<S: AsRef<str>>(&self, _: S) -> expr::TsQuery {
        match self.0 {}
    }

    pub fn plain_query<S: AsRef<str>>(&self, _: S) -> expr::TsQuery {
        match self.0 {}
    }

    pub fn rank(&self, _: expr::TsVector, _: expr::TsQuery) -> f32 {
        match self.0 {}
    }

    pub fn array_len<T>(&self, _: T, _: i32) -> i32 {
        match self.0 {}
    }

    pub fn cardinality<T>(&self, _: T) -> i32 {
        match self.0 {}
    }
}

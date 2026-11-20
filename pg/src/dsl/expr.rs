use crate::dsl;
pub use cartel_core::dsl::expr::Text;
use std::convert::Infallible;

pub struct TsVector(Infallible);

pub struct TsQuery(Infallible);

impl TsVector {
    pub fn is_match(&self, scope: &dsl::Scope, _: TsQuery) -> bool {
        match scope.0 {}
    }
}

pub trait In<T>: Sized {
    fn is_in(&self, scope: &dsl::Scope, _: &[T]) -> bool {
        match scope.0 {}
    }
}

impl In<Self> for i64 {}

pub trait Array<T>: Sized {
    fn has_all(self, scope: &dsl::Scope, _: T) -> bool {
        match scope.0 {}
    }
}

impl Array<&[i32]> for Vec<i32> {}
impl Array<&[&str]> for Vec<String> {}

pub trait Ltree: Sized {
    fn is_below(&self, scope: &dsl::Scope, _: crate::Ltree) -> bool {
        match scope.0 {}
    }
}

impl Ltree for crate::Ltree {}

pub fn vector<S: AsRef<str>>(scope: &dsl::Scope, _: S) -> TsVector {
    match scope.0 {}
}

pub fn query<S: AsRef<str>>(scope: &dsl::Scope, _: S) -> TsQuery {
    match scope.0 {}
}

pub fn rank(scope: &dsl::Scope, _: TsVector, _: TsQuery) -> f32 {
    match scope.0 {}
}

pub fn array_len<T>(scope: &dsl::Scope, _: T, _: i32) -> i32 {
    match scope.0 {}
}

pub fn cardinality<T>(scope: &dsl::Scope, _: T) -> i32 {
    match scope.0 {}
}

pub fn count<T>(scope: &dsl::Scope, _: T) -> i64 {
    match scope.0 {}
}

pub fn lag<T, R>(scope: &dsl::Scope, _: T, _: i64) -> Option<R> {
    match scope.0 {}
}

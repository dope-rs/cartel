use crate::dsl::{self, scope::never::Never};

pub struct TsVector(Never<()>);

pub struct TsQuery(Never<()>);

impl TsVector {
    pub fn is_match(&self, scope: &dsl::Scope, _: TsQuery) -> bool {
        match scope.0 {}
    }
}

pub trait Text: Sized {
    fn is_like<S: AsRef<str>>(&self, scope: &dsl::Scope, _: S) -> bool {
        match scope.0 {}
    }

    fn is_ilike<S: AsRef<str>>(&self, scope: &dsl::Scope, _: S) -> bool {
        match scope.0 {}
    }

    fn is_not_like<S: AsRef<str>>(&self, scope: &dsl::Scope, _: S) -> bool {
        match scope.0 {}
    }

    fn is_not_ilike<S: AsRef<str>>(&self, scope: &dsl::Scope, _: S) -> bool {
        match scope.0 {}
    }

    fn is_regex<S: AsRef<str>>(&self, scope: &dsl::Scope, _: S) -> bool {
        match scope.0 {}
    }

    fn is_iregex<S: AsRef<str>>(&self, scope: &dsl::Scope, _: S) -> bool {
        match scope.0 {}
    }

    fn is_mismatch<S: AsRef<str>>(&self, scope: &dsl::Scope, _: S) -> bool {
        match scope.0 {}
    }

    fn is_imismatch<S: AsRef<str>>(&self, scope: &dsl::Scope, _: S) -> bool {
        match scope.0 {}
    }
}

impl Text for String {}

pub trait In<T>: Sized {
    fn is_in(&self, scope: &dsl::Scope, _: &[T]) -> bool {
        match scope.0 {}
    }

    fn is_not_in(&self, scope: &dsl::Scope, _: &[T]) -> bool {
        match scope.0 {}
    }
}

impl In<Self> for i64 {}

pub trait Array<T>: Sized {
    fn has_all(self, scope: &dsl::Scope, _: T) -> bool {
        match scope.0 {}
    }

    fn has_any(self, scope: &dsl::Scope, _: T) -> bool {
        match scope.0 {}
    }
}

impl Array<&[i32]> for Vec<i32> {}
impl Array<&[&str]> for Vec<String> {}

pub trait Ltree: Sized {
    fn is_below(&self, scope: &dsl::Scope, _: crate::Ltree) -> bool {
        match scope.0 {}
    }

    fn is_above(&self, scope: &dsl::Scope, _: crate::Ltree) -> bool {
        match scope.0 {}
    }
}

impl Ltree for crate::Ltree {}

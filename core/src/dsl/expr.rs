use crate::dsl;

pub trait Text: Sized {
    fn is_like<S: AsRef<str>>(&self, scope: &dsl::Scope, _: S) -> bool {
        match scope.0 {}
    }
}

impl Text for String {}

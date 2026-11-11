use crate::dsl::never::Never;

pub struct Group<T, K>(Never<(T, K)>);
pub struct Handle(Never<()>);

impl<T, K> Group<T, K> {
    pub fn count(self) -> Vec<(K, i64)> {
        self.0.infer(())
    }

    pub fn sum<F, U>(self, map: F) -> Vec<(K, Option<U>)>
    where
        F: FnOnce(T) -> U,
    {
        self.0.infer(map)
    }

    pub fn max<F, U>(self, map: F) -> Vec<(K, Option<U>)>
    where
        F: FnOnce(T) -> U,
    {
        self.0.infer(map)
    }

    pub fn having<F>(self, pred: F) -> Self
    where
        F: FnOnce(T, Handle) -> bool,
    {
        self.0.infer(pred)
    }

    pub fn order_by<F, U>(self, key: F) -> Self
    where
        F: FnOnce(T) -> U,
    {
        self.0.infer(key)
    }
}

impl Handle {
    pub fn count(self) -> i64 {
        self.0.infer(())
    }
}

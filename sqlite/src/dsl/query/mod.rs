use crate::dsl::{agg, scope::never::Never, write};

pub mod join;

pub struct Filter<T>(Never<T>);
pub struct Rows<T>(Never<T>);
pub struct Cte<T>(Never<T>);
pub struct Select<T>(Never<T>);
pub struct Stream<T>(Never<T>);

impl<T> Filter<T> {
    pub fn rows(self) -> Rows<T> {
        self.0.infer(())
    }

    pub fn count(self) -> i64 {
        self.0.infer(())
    }

    pub fn sum<F, U, R>(self, map: F) -> Option<R>
    where
        F: FnOnce(T) -> U,
    {
        self.0.infer(map)
    }

    pub fn avg<F, U, R>(self, map: F) -> Option<R>
    where
        F: FnOnce(T) -> U,
    {
        self.0.infer(map)
    }

    pub fn min<F, U, R>(self, map: F) -> Option<R>
    where
        F: FnOnce(T) -> U,
    {
        self.0.infer(map)
    }

    pub fn max<F, U, R>(self, map: F) -> Option<R>
    where
        F: FnOnce(T) -> U,
    {
        self.0.infer(map)
    }

    pub fn group_by<F, K>(self, key: F) -> agg::Group<T, K>
    where
        F: FnOnce(T) -> K,
    {
        self.0.infer(key)
    }

    pub fn update<F>(self, change: F) -> write::Update<T>
    where
        F: FnOnce(&mut T),
    {
        self.0.infer(change)
    }

    pub fn delete(self) -> write::Delete<T> {
        self.0.infer(())
    }

    pub fn union(self, other: Self) -> Self {
        self.0.infer(other)
    }

    pub fn union_all(self, other: Self) -> Self {
        self.0.infer(other)
    }

    pub fn intersect(self, other: Self) -> Self {
        self.0.infer(other)
    }

    pub fn except(self, other: Self) -> Self {
        self.0.infer(other)
    }

    pub fn cte(self) -> Cte<T> {
        self.0.infer(())
    }
}

impl<T> Rows<T> {
    pub fn one(self) -> T {
        self.0.infer(())
    }

    pub fn first(self) -> Option<T> {
        self.0.infer(())
    }

    pub fn all(self) -> Vec<T> {
        self.0.infer(())
    }

    pub fn stream(self) -> Stream<T> {
        self.0.infer(())
    }

    pub fn order_by<F, U>(self, key: F) -> Self
    where
        F: FnOnce(T) -> U,
    {
        self.0.infer(key)
    }

    pub fn order_desc<F, U>(self, key: F) -> Self
    where
        F: FnOnce(T) -> U,
    {
        self.0.infer(key)
    }

    pub fn limit(self, count: i64) -> Self {
        self.0.infer(count)
    }

    pub fn offset(self, count: i64) -> Self {
        self.0.infer(count)
    }

    pub fn distinct(self) -> Self {
        self.0.infer(())
    }

    pub fn select<F, R>(self, map: F) -> Select<R>
    where
        F: FnOnce(T) -> R,
    {
        self.0.infer(map)
    }
}

impl<T> Cte<T> {
    pub fn filter<F>(self, pred: F) -> Filter<T>
    where
        F: FnOnce(T) -> bool,
    {
        self.0.infer(pred)
    }
}

impl<T> Select<T> {
    pub fn one(self) -> T {
        self.0.infer(())
    }

    pub fn first(self) -> Option<T> {
        self.0.infer(())
    }

    pub fn all(self) -> Vec<T> {
        self.0.infer(())
    }
}

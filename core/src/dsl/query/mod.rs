use crate::dsl::{agg, never::Never, write};

pub mod join;

pub enum Projected {}
pub struct Query<T, Phase = ()>(Never<(T, Phase)>);
pub struct Stream<T>(Never<T>);

impl<T, Phase> Query<T, Phase> {
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
}

impl<T> Query<T> {
    pub fn filter<F>(self, pred: F) -> Self
    where
        F: FnOnce(T) -> bool,
    {
        self.0.infer(pred)
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

    pub fn distinct_on<F, U>(self, key: F) -> Self
    where
        F: FnOnce(T) -> U,
    {
        self.0.infer(key)
    }

    pub fn for_update(self) -> Self {
        self.0.infer(())
    }

    pub fn for_share(self) -> Self {
        self.0.infer(())
    }

    pub fn update<F>(self, update: F) -> write::Update<T>
    where
        F: FnOnce(&mut T),
    {
        self.0.infer(update)
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

    pub fn meet_all(self, other: Self) -> Self {
        self.0.infer(other)
    }

    pub fn except(self, other: Self) -> Self {
        self.0.infer(other)
    }

    pub fn except_all(self, other: Self) -> Self {
        self.0.infer(other)
    }

    pub fn cte(self) -> Self {
        self.0.infer(())
    }

    pub fn select<F, R>(self, map: F) -> Query<R, Projected>
    where
        F: FnOnce(T) -> R,
    {
        self.0.infer(map)
    }
}

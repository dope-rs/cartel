mod void;

use crate::dsl::{agg, query, write};
pub(super) use void::Void;

pub trait Fetch<T>: Void {
    fn one(self) -> T {
        self.infer(())
    }

    fn first(self) -> Option<T> {
        self.infer(())
    }

    fn all(self) -> Vec<T> {
        self.infer(())
    }

    fn stream(self) -> query::Stream<T> {
        self.infer(())
    }
}

impl<T> Fetch<T> for query::Filter<T> {}
impl<T> Fetch<T> for query::Cte<T> {}
impl<T> Fetch<T> for query::Select<T> {}

pub trait Reduce<T>: Void {
    fn count(self) -> i64 {
        self.infer(())
    }

    fn sum<F, U, R>(self, map: F) -> Option<R>
    where
        F: FnOnce(T) -> U,
    {
        self.infer(map)
    }

    fn avg<F, U, R>(self, map: F) -> Option<R>
    where
        F: FnOnce(T) -> U,
    {
        self.infer(map)
    }

    fn min<F, U, R>(self, map: F) -> Option<R>
    where
        F: FnOnce(T) -> U,
    {
        self.infer(map)
    }

    fn max<F, U, R>(self, map: F) -> Option<R>
    where
        F: FnOnce(T) -> U,
    {
        self.infer(map)
    }

    fn group_by<F, K>(self, key: F) -> agg::Group<T, K>
    where
        F: FnOnce(T) -> K,
    {
        self.infer(key)
    }
}

impl<T> Reduce<T> for query::Filter<T> {}

pub trait Arrange<T>: Void {
    fn order_by<F, U>(self, key: F) -> Self
    where
        F: FnOnce(T) -> U,
    {
        self.infer(key)
    }

    fn order_desc<F, U>(self, key: F) -> Self
    where
        F: FnOnce(T) -> U,
    {
        self.infer(key)
    }

    fn limit(self, count: i64) -> Self {
        self.infer(count)
    }

    fn offset(self, count: i64) -> Self {
        self.infer(count)
    }

    fn distinct(self) -> Self {
        self.infer(())
    }

    fn distinct_on<F, U>(self, key: F) -> Self
    where
        F: FnOnce(T) -> U,
    {
        self.infer(key)
    }

    fn for_update(self) -> Self {
        self.infer(())
    }

    fn for_share(self) -> Self {
        self.infer(())
    }
}

impl<T> Arrange<T> for query::Filter<T> {}

pub trait Change<T>: Void {
    fn update<F>(self, update: F) -> write::Update<T>
    where
        F: FnOnce(&mut T),
    {
        self.infer(update)
    }

    fn delete(self) -> write::Delete<T> {
        self.infer(())
    }
}

impl<T> Change<T> for query::Filter<T> {}

pub trait Set<T>: Void {
    fn union(self, other: Self) -> Self {
        self.infer(other)
    }

    fn union_all(self, other: Self) -> Self {
        self.infer(other)
    }

    fn intersect(self, other: Self) -> Self {
        self.infer(other)
    }

    fn meet_all(self, other: Self) -> Self {
        self.infer(other)
    }

    fn except(self, other: Self) -> Self {
        self.infer(other)
    }

    fn except_all(self, other: Self) -> Self {
        self.infer(other)
    }
}

impl<T> Set<T> for query::Filter<T> {}
impl<T> Set<T> for query::Cte<T> {}

pub trait Project<T>: Void {
    fn cte(self) -> query::Cte<T> {
        self.infer(())
    }

    fn select<F, R>(self, map: F) -> query::Select<R>
    where
        F: FnOnce(T) -> R,
    {
        self.infer(map)
    }
}

impl<T> Project<T> for query::Filter<T> {}

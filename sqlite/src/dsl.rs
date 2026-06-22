#![allow(unused_variables, clippy::unused_self, clippy::wrong_self_convention)]

use std::marker::PhantomData;

const STUB: &str = "cartel_sqlite: DSL stub only callable inside #[query] body";

pub struct FilterBuilder<T>(PhantomData<T>);

pub struct JoinBuilder2<P, J>(PhantomData<(P, J)>);
pub type JoinBuilder<P, J> = JoinBuilder2<P, J>;
pub struct JoinBuilder3<A, B, C>(PhantomData<(A, B, C)>);
pub struct JoinBuilder4<A, B, C, D>(PhantomData<(A, B, C, D)>);

pub struct Joined2Filter<A, B>(PhantomData<(A, B)>);
pub struct Joined3Filter<A, B, C>(PhantomData<(A, B, C)>);
pub struct Joined4Filter<A, B, C, D>(PhantomData<(A, B, C, D)>);

pub struct AggBuilder<T, K>(PhantomData<(T, K)>);

pub struct InsertBuilder<T>(PhantomData<T>);
pub struct UpdateBuilder<T>(PhantomData<T>);
pub struct DeleteBuilder<T>(PhantomData<T>);
pub struct ConflictTarget<T>(PhantomData<T>);
pub struct Cte<T>(PhantomData<T>);
pub struct SelectBuilder<R>(PhantomData<R>);
pub struct AggHandle(());

pub struct RowIter<T>(PhantomData<T>);

pub trait SourceRow {
    type Row;
}
impl<T> SourceRow for FilterBuilder<T> {
    type Row = T;
}
impl<T> SourceRow for Cte<T> {
    type Row = T;
}
impl<R> SourceRow for SelectBuilder<R> {
    type Row = R;
}

impl<T> FilterBuilder<T> {
    #[doc(hidden)]
    pub fn __new() -> Self {
        Self(PhantomData)
    }

    pub fn one(self) -> T {
        unreachable!("{STUB}")
    }
    pub fn first(self) -> Option<T> {
        unreachable!("{STUB}")
    }
    pub fn all(self) -> Vec<T> {
        unreachable!("{STUB}")
    }
    pub fn stream(self) -> RowIter<T> {
        unreachable!("{STUB}")
    }

    pub fn count(self) -> i64 {
        unreachable!("{STUB}")
    }
    pub fn sum<F, U, R>(self, f: F) -> Option<R>
    where
        F: FnOnce(T) -> U,
    {
        unreachable!("{STUB}")
    }
    pub fn avg<F, U, R>(self, f: F) -> Option<R>
    where
        F: FnOnce(T) -> U,
    {
        unreachable!("{STUB}")
    }
    pub fn min<F, U, R>(self, f: F) -> Option<R>
    where
        F: FnOnce(T) -> U,
    {
        unreachable!("{STUB}")
    }
    pub fn max<F, U, R>(self, f: F) -> Option<R>
    where
        F: FnOnce(T) -> U,
    {
        unreachable!("{STUB}")
    }

    pub fn group_by<F, K>(self, f: F) -> AggBuilder<T, K>
    where
        F: FnOnce(T) -> K,
    {
        unreachable!("{STUB}")
    }

    pub fn order_by<F, U>(self, f: F) -> Self
    where
        F: FnOnce(T) -> U,
    {
        unreachable!("{STUB}")
    }
    pub fn order_by_desc<F, U>(self, f: F) -> Self
    where
        F: FnOnce(T) -> U,
    {
        unreachable!("{STUB}")
    }
    pub fn limit(self, n: i64) -> Self {
        unreachable!("{STUB}")
    }
    pub fn offset(self, n: i64) -> Self {
        unreachable!("{STUB}")
    }
    pub fn distinct(self) -> Self {
        unreachable!("{STUB}")
    }

    pub fn update<F>(self, f: F) -> UpdateBuilder<T>
    where
        F: FnOnce(&mut T),
    {
        unreachable!("{STUB}")
    }
    pub fn delete(self) -> DeleteBuilder<T> {
        unreachable!("{STUB}")
    }

    pub fn union(self, other: Self) -> Self {
        unreachable!("{STUB}")
    }
    pub fn union_all(self, other: Self) -> Self {
        unreachable!("{STUB}")
    }
    pub fn intersect(self, other: Self) -> Self {
        unreachable!("{STUB}")
    }
    pub fn except(self, other: Self) -> Self {
        unreachable!("{STUB}")
    }

    pub fn cte(self) -> Cte<T> {
        unreachable!("{STUB}")
    }

    pub fn select<F, R>(self, f: F) -> SelectBuilder<R>
    where
        F: FnOnce(T) -> R,
    {
        unreachable!("{STUB}")
    }
}

impl<T, K> AggBuilder<T, K> {
    pub fn count(self) -> Vec<(K, i64)> {
        unreachable!("{STUB}")
    }
    pub fn sum<F, U>(self, f: F) -> Vec<(K, Option<U>)>
    where
        F: FnOnce(T) -> U,
    {
        unreachable!("{STUB}")
    }
    pub fn avg<F, U>(self, f: F) -> Vec<(K, Option<f64>)>
    where
        F: FnOnce(T) -> U,
    {
        unreachable!("{STUB}")
    }
    pub fn min<F, U>(self, f: F) -> Vec<(K, Option<U>)>
    where
        F: FnOnce(T) -> U,
    {
        unreachable!("{STUB}")
    }
    pub fn max<F, U>(self, f: F) -> Vec<(K, Option<U>)>
    where
        F: FnOnce(T) -> U,
    {
        unreachable!("{STUB}")
    }

    pub fn having<F>(self, f: F) -> Self
    where
        F: FnOnce(T, AggHandle) -> bool,
    {
        unreachable!("{STUB}")
    }

    pub fn order_by<F, U>(self, f: F) -> Self
    where
        F: FnOnce(T) -> U,
    {
        unreachable!("{STUB}")
    }
    pub fn order_by_desc<F, U>(self, f: F) -> Self
    where
        F: FnOnce(T) -> U,
    {
        unreachable!("{STUB}")
    }
    pub fn limit(self, n: i64) -> Self {
        unreachable!("{STUB}")
    }
    pub fn offset(self, n: i64) -> Self {
        unreachable!("{STUB}")
    }
}

impl<A, B> JoinBuilder2<A, B> {
    pub fn filter(self, f: impl FnOnce(A, B) -> bool) -> Joined2Filter<A, B> {
        unreachable!("{STUB}")
    }
    pub fn join<C>(self, f: impl FnOnce(A, B, C) -> bool) -> JoinBuilder3<A, B, C> {
        unreachable!("{STUB}")
    }
    pub fn left_join<C>(self, f: impl FnOnce(A, B, C) -> bool) -> JoinBuilder3<A, B, C> {
        unreachable!("{STUB}")
    }
    pub fn right_join<C>(self, f: impl FnOnce(A, B, C) -> bool) -> JoinBuilder3<A, B, C> {
        unreachable!("{STUB}")
    }
    pub fn full_join<C>(self, f: impl FnOnce(A, B, C) -> bool) -> JoinBuilder3<A, B, C> {
        unreachable!("{STUB}")
    }
}

impl<A, B, C> JoinBuilder3<A, B, C> {
    pub fn filter(self, f: impl FnOnce(A, B, C) -> bool) -> Joined3Filter<A, B, C> {
        unreachable!("{STUB}")
    }
    pub fn join<D>(self, f: impl FnOnce(A, B, C, D) -> bool) -> JoinBuilder4<A, B, C, D> {
        unreachable!("{STUB}")
    }
    pub fn left_join<D>(self, f: impl FnOnce(A, B, C, D) -> bool) -> JoinBuilder4<A, B, C, D> {
        unreachable!("{STUB}")
    }
    pub fn right_join<D>(self, f: impl FnOnce(A, B, C, D) -> bool) -> JoinBuilder4<A, B, C, D> {
        unreachable!("{STUB}")
    }
    pub fn full_join<D>(self, f: impl FnOnce(A, B, C, D) -> bool) -> JoinBuilder4<A, B, C, D> {
        unreachable!("{STUB}")
    }
}

impl<A, B, C, D> JoinBuilder4<A, B, C, D> {
    pub fn filter(self, f: impl FnOnce(A, B, C, D) -> bool) -> Joined4Filter<A, B, C, D> {
        unreachable!("{STUB}")
    }
}

impl<A, B> Joined2Filter<A, B> {
    pub fn one<R>(self) -> R {
        unreachable!("{STUB}")
    }
    pub fn first<R>(self) -> Option<R> {
        unreachable!("{STUB}")
    }
    pub fn all<R>(self) -> Vec<R> {
        unreachable!("{STUB}")
    }
    pub fn stream<R>(self) -> RowIter<R> {
        unreachable!("{STUB}")
    }
    pub fn count(self) -> i64 {
        unreachable!("{STUB}")
    }
    pub fn order_by<F, U>(self, f: F) -> Self
    where
        F: FnOnce(A, B) -> U,
    {
        unreachable!("{STUB}")
    }
    pub fn order_by_desc<F, U>(self, f: F) -> Self
    where
        F: FnOnce(A, B) -> U,
    {
        unreachable!("{STUB}")
    }
    pub fn limit(self, n: i64) -> Self {
        unreachable!("{STUB}")
    }
    pub fn offset(self, n: i64) -> Self {
        unreachable!("{STUB}")
    }
    pub fn distinct(self) -> Self {
        unreachable!("{STUB}")
    }
    pub fn update(self, f: impl FnOnce(&mut A, B)) -> UpdateBuilder<A> {
        unreachable!("{STUB}")
    }
    pub fn delete(self) -> DeleteBuilder<A> {
        unreachable!("{STUB}")
    }
    pub fn select<F, R>(self, _f: F) -> SelectBuilder<R>
    where
        F: FnOnce(A, B) -> R,
    {
        unreachable!("{STUB}")
    }
}

impl<A, B, C> Joined3Filter<A, B, C> {
    pub fn one<R>(self) -> R {
        unreachable!("{STUB}")
    }
    pub fn first<R>(self) -> Option<R> {
        unreachable!("{STUB}")
    }
    pub fn all<R>(self) -> Vec<R> {
        unreachable!("{STUB}")
    }
    pub fn stream<R>(self) -> RowIter<R> {
        unreachable!("{STUB}")
    }
    pub fn count(self) -> i64 {
        unreachable!("{STUB}")
    }
    pub fn order_by<F, U>(self, f: F) -> Self
    where
        F: FnOnce(A, B, C) -> U,
    {
        unreachable!("{STUB}")
    }
    pub fn order_by_desc<F, U>(self, f: F) -> Self
    where
        F: FnOnce(A, B, C) -> U,
    {
        unreachable!("{STUB}")
    }
    pub fn limit(self, n: i64) -> Self {
        unreachable!("{STUB}")
    }
    pub fn offset(self, n: i64) -> Self {
        unreachable!("{STUB}")
    }
    pub fn distinct(self) -> Self {
        unreachable!("{STUB}")
    }
    pub fn update(self, f: impl FnOnce(&mut A, B, C)) -> UpdateBuilder<A> {
        unreachable!("{STUB}")
    }
    pub fn delete(self) -> DeleteBuilder<A> {
        unreachable!("{STUB}")
    }
    pub fn select<F, R>(self, _f: F) -> SelectBuilder<R>
    where
        F: FnOnce(A, B, C) -> R,
    {
        unreachable!("{STUB}")
    }
}

impl<A, B, C, D> Joined4Filter<A, B, C, D> {
    pub fn one<R>(self) -> R {
        unreachable!("{STUB}")
    }
    pub fn first<R>(self) -> Option<R> {
        unreachable!("{STUB}")
    }
    pub fn all<R>(self) -> Vec<R> {
        unreachable!("{STUB}")
    }
    pub fn stream<R>(self) -> RowIter<R> {
        unreachable!("{STUB}")
    }
    pub fn count(self) -> i64 {
        unreachable!("{STUB}")
    }
    pub fn order_by<F, U>(self, f: F) -> Self
    where
        F: FnOnce(A, B, C, D) -> U,
    {
        unreachable!("{STUB}")
    }
    pub fn order_by_desc<F, U>(self, f: F) -> Self
    where
        F: FnOnce(A, B, C, D) -> U,
    {
        unreachable!("{STUB}")
    }
    pub fn limit(self, n: i64) -> Self {
        unreachable!("{STUB}")
    }
    pub fn offset(self, n: i64) -> Self {
        unreachable!("{STUB}")
    }
    pub fn distinct(self) -> Self {
        unreachable!("{STUB}")
    }
    pub fn update(self, f: impl FnOnce(&mut A, B, C, D)) -> UpdateBuilder<A> {
        unreachable!("{STUB}")
    }
    pub fn delete(self) -> DeleteBuilder<A> {
        unreachable!("{STUB}")
    }
    pub fn select<F, R>(self, _f: F) -> SelectBuilder<R>
    where
        F: FnOnce(A, B, C, D) -> R,
    {
        unreachable!("{STUB}")
    }
}

impl<T> InsertBuilder<T> {
    pub fn returning_one(self) -> T {
        unreachable!("{STUB}")
    }
    pub fn returning_first(self) -> Option<T> {
        unreachable!("{STUB}")
    }
    pub fn returning_all(self) -> Vec<T> {
        unreachable!("{STUB}")
    }
    pub fn on_conflict_do_nothing(self) -> Self {
        unreachable!("{STUB}")
    }
    pub fn on_conflict<F, U>(self, f: F) -> ConflictTarget<T>
    where
        F: FnOnce(T) -> U,
    {
        unreachable!("{STUB}")
    }
}

impl<T> ConflictTarget<T> {
    pub fn do_nothing(self) -> InsertBuilder<T> {
        unreachable!("{STUB}")
    }
    pub fn do_update<F>(self, f: F) -> InsertBuilder<T>
    where
        F: FnOnce(&mut T),
    {
        unreachable!("{STUB}")
    }
}

impl<T> UpdateBuilder<T> {
    pub fn returning_one(self) -> T {
        unreachable!("{STUB}")
    }
    pub fn returning_first(self) -> Option<T> {
        unreachable!("{STUB}")
    }
    pub fn returning_all(self) -> Vec<T> {
        unreachable!("{STUB}")
    }
}

impl<T> DeleteBuilder<T> {
    pub fn returning_one(self) -> T {
        unreachable!("{STUB}")
    }
    pub fn returning_first(self) -> Option<T> {
        unreachable!("{STUB}")
    }
    pub fn returning_all(self) -> Vec<T> {
        unreachable!("{STUB}")
    }
}

impl<T> Cte<T> {
    pub fn filter<F>(self, f: F) -> FilterBuilder<T>
    where
        F: FnOnce(T) -> bool,
    {
        unreachable!("{STUB}")
    }
    pub fn all(self) -> Vec<T> {
        unreachable!("{STUB}")
    }
    pub fn one(self) -> T {
        unreachable!("{STUB}")
    }
    pub fn first(self) -> Option<T> {
        unreachable!("{STUB}")
    }
    pub fn stream(self) -> RowIter<T> {
        unreachable!("{STUB}")
    }
    pub fn union(self, other: Self) -> Self {
        unreachable!("{STUB}")
    }
    pub fn union_all(self, other: Self) -> Self {
        unreachable!("{STUB}")
    }
    pub fn intersect(self, other: Self) -> Self {
        unreachable!("{STUB}")
    }
    pub fn except(self, other: Self) -> Self {
        unreachable!("{STUB}")
    }
}

impl<R> SelectBuilder<R> {
    pub fn one(self) -> R {
        unreachable!("{STUB}")
    }
    pub fn first(self) -> Option<R> {
        unreachable!("{STUB}")
    }
    pub fn all(self) -> Vec<R> {
        unreachable!("{STUB}")
    }
    pub fn stream(self) -> RowIter<R> {
        unreachable!("{STUB}")
    }
}

impl AggHandle {
    pub fn count(&self) -> i64 {
        unreachable!("{STUB}")
    }
    pub fn sum<U>(&self, col: U) -> Option<U> {
        unreachable!("{STUB}")
    }
    pub fn avg<U>(&self, col: U) -> Option<f64> {
        unreachable!("{STUB}")
    }
    pub fn min<U>(&self, col: U) -> Option<U> {
        unreachable!("{STUB}")
    }
    pub fn max<U>(&self, col: U) -> Option<U> {
        unreachable!("{STUB}")
    }
}

pub trait Text: Sized {
    fn like<S: AsRef<str>>(self, pattern: S) -> bool {
        unreachable!("{STUB}")
    }
    fn not_like<S: AsRef<str>>(self, pattern: S) -> bool {
        unreachable!("{STUB}")
    }
    fn glob<S: AsRef<str>>(self, pattern: S) -> bool {
        unreachable!("{STUB}")
    }
    fn concat<S: AsRef<str>>(self, other: S) -> String {
        unreachable!("{STUB}")
    }
}
impl Text for String {}
impl Text for &str {}
impl Text for Option<String> {}

pub fn exists<S>(subquery: S) -> bool {
    unreachable!("{STUB}")
}
pub fn not_exists<S>(subquery: S) -> bool {
    unreachable!("{STUB}")
}

pub struct WindowExpr<T>(PhantomData<T>);
pub struct WindowSpec(());

impl<T> WindowExpr<T> {
    pub fn over<F>(self, _f: F) -> T
    where
        F: FnOnce(WindowSpec) -> WindowSpec,
    {
        unreachable!("{STUB}")
    }
}

impl WindowSpec {
    pub fn partition_by<U>(self, _col: U) -> Self {
        unreachable!("{STUB}")
    }
    pub fn order_by<U>(self, _col: U) -> Self {
        unreachable!("{STUB}")
    }
    pub fn order_by_desc<U>(self, _col: U) -> Self {
        unreachable!("{STUB}")
    }
}

pub fn row_number() -> WindowExpr<i64> {
    unreachable!("{STUB}")
}
pub fn rank() -> WindowExpr<i64> {
    unreachable!("{STUB}")
}
pub fn dense_rank() -> WindowExpr<i64> {
    unreachable!("{STUB}")
}
pub fn count<T>(_x: T) -> WindowExpr<i64> {
    unreachable!("{STUB}")
}
pub fn sum<T, R>(_x: T) -> WindowExpr<R> {
    unreachable!("{STUB}")
}
pub fn avg<T, R>(_x: T) -> WindowExpr<R> {
    unreachable!("{STUB}")
}
pub fn min<T, R>(_x: T) -> WindowExpr<R> {
    unreachable!("{STUB}")
}
pub fn max<T, R>(_x: T) -> WindowExpr<R> {
    unreachable!("{STUB}")
}

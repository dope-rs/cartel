#![allow(unused_variables, clippy::unused_self, clippy::wrong_self_convention)]

use std::marker::PhantomData;

const STUB: &str = "cartel_pg: DSL stub only callable inside #[query] body";

pub struct FilterBuilder<T>(PhantomData<T>);

pub struct Stream<T>(PhantomData<T>);

pub struct JoinBuilder2<P, J>(PhantomData<(P, J)>);
pub type JoinBuilder<P, J> = JoinBuilder2<P, J>;
pub struct JoinBuilder3<A, B, C>(PhantomData<(A, B, C)>);
pub struct JoinBuilder4<A, B, C, D>(PhantomData<(A, B, C, D)>);

pub struct Joined2Filter<A, B>(PhantomData<(A, B)>);
pub struct Joined3Filter<A, B, C>(PhantomData<(A, B, C)>);
pub struct Joined4Filter<A, B, C, D>(PhantomData<(A, B, C, D)>);

pub struct AggBuilder<T, K>(PhantomData<(T, K)>);

pub struct InsertBuilder<T>(PhantomData<T>);

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

pub struct UpdateBuilder<T>(PhantomData<T>);

pub struct UpdateEachBuilder<T, D>(PhantomData<(T, D)>);

pub trait ColSlice {
    type Elem;
}
impl<S: ColSlice + ?Sized> ColSlice for &S {
    type Elem = S::Elem;
}
impl<A> ColSlice for [A] {
    type Elem = A;
}
impl<A> ColSlice for Vec<A> {
    type Elem = A;
}

pub trait EachCols {}
impl<C0: ColSlice> EachCols for (C0,) {}
impl<C0: ColSlice, C1: ColSlice> EachCols for (C0, C1) {}
impl<C0: ColSlice, C1: ColSlice, C2: ColSlice> EachCols for (C0, C1, C2) {}
impl<C0: ColSlice, C1: ColSlice, C2: ColSlice, C3: ColSlice> EachCols for (C0, C1, C2, C3) {}
impl<C0: ColSlice, C1: ColSlice, C2: ColSlice, C3: ColSlice, C4: ColSlice> EachCols
    for (C0, C1, C2, C3, C4)
{
}
impl<C0: ColSlice, C1: ColSlice, C2: ColSlice, C3: ColSlice, C4: ColSlice, C5: ColSlice> EachCols
    for (C0, C1, C2, C3, C4, C5)
{
}
impl<
    C0: ColSlice,
    C1: ColSlice,
    C2: ColSlice,
    C3: ColSlice,
    C4: ColSlice,
    C5: ColSlice,
    C6: ColSlice,
> EachCols for (C0, C1, C2, C3, C4, C5, C6)
{
}
impl<
    C0: ColSlice,
    C1: ColSlice,
    C2: ColSlice,
    C3: ColSlice,
    C4: ColSlice,
    C5: ColSlice,
    C6: ColSlice,
    C7: ColSlice,
> EachCols for (C0, C1, C2, C3, C4, C5, C6, C7)
{
}

pub trait EachClosure<D, Row> {}

impl<C0, Row, O, F> EachClosure<(C0,), Row> for F
where
    C0: ColSlice,
    F: FnOnce(Row, C0::Elem) -> O,
{
}
impl<C0, C1, Row, O, F> EachClosure<(C0, C1), Row> for F
where
    C0: ColSlice,
    C1: ColSlice,
    F: FnOnce(Row, C0::Elem, C1::Elem) -> O,
{
}
impl<C0, C1, C2, Row, O, F> EachClosure<(C0, C1, C2), Row> for F
where
    C0: ColSlice,
    C1: ColSlice,
    C2: ColSlice,
    F: FnOnce(Row, C0::Elem, C1::Elem, C2::Elem) -> O,
{
}
impl<C0, C1, C2, C3, Row, O, F> EachClosure<(C0, C1, C2, C3), Row> for F
where
    C0: ColSlice,
    C1: ColSlice,
    C2: ColSlice,
    C3: ColSlice,
    F: FnOnce(Row, C0::Elem, C1::Elem, C2::Elem, C3::Elem) -> O,
{
}
impl<C0, C1, C2, C3, C4, Row, O, F> EachClosure<(C0, C1, C2, C3, C4), Row> for F
where
    C0: ColSlice,
    C1: ColSlice,
    C2: ColSlice,
    C3: ColSlice,
    C4: ColSlice,
    F: FnOnce(Row, C0::Elem, C1::Elem, C2::Elem, C3::Elem, C4::Elem) -> O,
{
}
impl<C0, C1, C2, C3, C4, C5, Row, O, F> EachClosure<(C0, C1, C2, C3, C4, C5), Row> for F
where
    C0: ColSlice,
    C1: ColSlice,
    C2: ColSlice,
    C3: ColSlice,
    C4: ColSlice,
    C5: ColSlice,
    F: FnOnce(Row, C0::Elem, C1::Elem, C2::Elem, C3::Elem, C4::Elem, C5::Elem) -> O,
{
}
impl<C0, C1, C2, C3, C4, C5, C6, Row, O, F> EachClosure<(C0, C1, C2, C3, C4, C5, C6), Row> for F
where
    C0: ColSlice,
    C1: ColSlice,
    C2: ColSlice,
    C3: ColSlice,
    C4: ColSlice,
    C5: ColSlice,
    C6: ColSlice,
    F: FnOnce(Row, C0::Elem, C1::Elem, C2::Elem, C3::Elem, C4::Elem, C5::Elem, C6::Elem) -> O,
{
}
impl<C0, C1, C2, C3, C4, C5, C6, C7, Row, O, F> EachClosure<(C0, C1, C2, C3, C4, C5, C6, C7), Row>
    for F
where
    C0: ColSlice,
    C1: ColSlice,
    C2: ColSlice,
    C3: ColSlice,
    C4: ColSlice,
    C5: ColSlice,
    C6: ColSlice,
    C7: ColSlice,
    F: FnOnce(
        Row,
        C0::Elem,
        C1::Elem,
        C2::Elem,
        C3::Elem,
        C4::Elem,
        C5::Elem,
        C6::Elem,
        C7::Elem,
    ) -> O,
{
}

impl<T, D> UpdateEachBuilder<T, D> {
    pub fn update<F>(self, _f: F) -> UpdateBuilder<T>
    where
        D: EachCols,
        F: EachClosure<D, T>,
    {
        unreachable!("{STUB}")
    }
}

pub struct DeleteBuilder<T>(PhantomData<T>);

pub struct ConflictTarget<T>(PhantomData<T>);

pub struct Cte<T>(PhantomData<T>);

pub struct SelectBuilder<R>(PhantomData<R>);

pub struct WindowExpr<T>(PhantomData<T>);

pub struct WindowSpec(());

pub struct AggHandle(());

pub struct TsVector(());

pub struct TsQuery(());

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
    pub fn stream(self) -> crate::Stream<T> {
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
    pub fn distinct_on<F, U>(self, f: F) -> Self
    where
        F: FnOnce(T) -> U,
    {
        unreachable!("{STUB}")
    }
    pub fn for_update(self) -> Self {
        unreachable!("{STUB}")
    }
    pub fn for_share(self) -> Self {
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
    pub fn intersect_all(self, other: Self) -> Self {
        unreachable!("{STUB}")
    }
    pub fn except(self, other: Self) -> Self {
        unreachable!("{STUB}")
    }
    pub fn except_all(self, other: Self) -> Self {
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

    pub fn lateral_join<C>(
        self,
        f: impl FnOnce(A, B) -> FilterBuilder<C>,
    ) -> JoinBuilder3<A, B, C> {
        unreachable!("{STUB}")
    }
    pub fn lateral_left_join<C>(
        self,
        f: impl FnOnce(A, B) -> FilterBuilder<C>,
    ) -> JoinBuilder3<A, B, C> {
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

    pub fn lateral_join<D>(
        self,
        f: impl FnOnce(A, B, C) -> FilterBuilder<D>,
    ) -> JoinBuilder4<A, B, C, D> {
        unreachable!("{STUB}")
    }
    pub fn lateral_left_join<D>(
        self,
        f: impl FnOnce(A, B, C) -> FilterBuilder<D>,
    ) -> JoinBuilder4<A, B, C, D> {
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
    pub fn stream<R>(self) -> crate::Stream<R> {
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
    pub fn for_update(self) -> Self {
        unreachable!("{STUB}")
    }
    pub fn for_share(self) -> Self {
        unreachable!("{STUB}")
    }
    pub fn update(self, f: impl FnOnce(&mut A, B)) -> UpdateBuilder<A> {
        unreachable!("{STUB}")
    }
    pub fn delete(self) -> DeleteBuilder<A> {
        unreachable!("{STUB}")
    }
    pub fn select<F, R>(self, f: F) -> SelectBuilder<R>
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
    pub fn stream<R>(self) -> crate::Stream<R> {
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
    pub fn for_update(self) -> Self {
        unreachable!("{STUB}")
    }
    pub fn for_share(self) -> Self {
        unreachable!("{STUB}")
    }
    pub fn update(self, f: impl FnOnce(&mut A, B, C)) -> UpdateBuilder<A> {
        unreachable!("{STUB}")
    }
    pub fn delete(self) -> DeleteBuilder<A> {
        unreachable!("{STUB}")
    }
    pub fn select<F, R>(self, f: F) -> SelectBuilder<R>
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
    pub fn stream<R>(self) -> crate::Stream<R> {
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
    pub fn for_update(self) -> Self {
        unreachable!("{STUB}")
    }
    pub fn for_share(self) -> Self {
        unreachable!("{STUB}")
    }
    pub fn update(self, f: impl FnOnce(&mut A, B, C, D)) -> UpdateBuilder<A> {
        unreachable!("{STUB}")
    }
    pub fn delete(self) -> DeleteBuilder<A> {
        unreachable!("{STUB}")
    }
    pub fn select<F, R>(self, f: F) -> SelectBuilder<R>
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
    pub fn stream(self) -> crate::Stream<T> {
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
    pub fn stream(self) -> crate::Stream<R> {
        unreachable!("{STUB}")
    }
}

impl<T> WindowExpr<T> {
    pub fn over<F>(self, f: F) -> T
    where
        F: FnOnce(WindowSpec) -> WindowSpec,
    {
        unreachable!("{STUB}")
    }
}

impl WindowSpec {
    pub fn partition_by<U>(self, col: U) -> Self {
        unreachable!("{STUB}")
    }
    pub fn order_by<U>(self, col: U) -> Self {
        unreachable!("{STUB}")
    }
    pub fn order_by_desc<U>(self, col: U) -> Self {
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

pub fn lower<S: AsRef<str>>(s: S) -> String {
    unreachable!("{STUB}")
}
pub fn upper<S: AsRef<str>>(s: S) -> String {
    unreachable!("{STUB}")
}
pub fn length<S: AsRef<str>>(s: S) -> i32 {
    unreachable!("{STUB}")
}
pub fn char_length<S: AsRef<str>>(s: S) -> i32 {
    unreachable!("{STUB}")
}
pub fn trim<S: AsRef<str>>(s: S) -> String {
    unreachable!("{STUB}")
}

pub fn abs<T>(x: T) -> T {
    unreachable!("{STUB}")
}
pub fn floor<T, R>(x: T) -> R {
    unreachable!("{STUB}")
}
pub fn ceil<T, R>(x: T) -> R {
    unreachable!("{STUB}")
}
pub fn round<T, R>(x: T) -> R {
    unreachable!("{STUB}")
}
pub fn sqrt<T, R>(x: T) -> R {
    unreachable!("{STUB}")
}
pub fn power<T, U, R>(base: T, exp: U) -> R {
    unreachable!("{STUB}")
}

pub fn coalesce<T>(a: T, b: T) -> T {
    unreachable!("{STUB}")
}

pub fn now() -> crate::Timestamp {
    unreachable!("{STUB}")
}
pub fn current_timestamp() -> crate::Timestamp {
    unreachable!("{STUB}")
}
pub fn current_date() -> crate::Date {
    unreachable!("{STUB}")
}
pub fn current_time() -> crate::Timestamp {
    unreachable!("{STUB}")
}
pub fn date_part<S: AsRef<str>, T>(field: S, ts: T) -> f64 {
    unreachable!("{STUB}")
}
pub fn date_trunc<S: AsRef<str>, T>(field: S, ts: T) -> crate::Timestamp {
    unreachable!("{STUB}")
}
pub fn age<T1, T2>(t1: T1, t2: T2) -> i64 {
    unreachable!("{STUB}")
}

pub fn array_length<T>(arr: T, dim: i32) -> i32 {
    unreachable!("{STUB}")
}
pub fn cardinality<T>(arr: T) -> i32 {
    unreachable!("{STUB}")
}

pub fn to_tsvector<S: AsRef<str>>(text: S) -> TsVector {
    unreachable!("{STUB}")
}
pub fn to_tsquery<S: AsRef<str>>(query: S) -> TsQuery {
    unreachable!("{STUB}")
}
pub fn plainto_tsquery<S: AsRef<str>>(query: S) -> TsQuery {
    unreachable!("{STUB}")
}
pub fn phraseto_tsquery<S: AsRef<str>>(query: S) -> TsQuery {
    unreachable!("{STUB}")
}
pub fn websearch_to_tsquery<S: AsRef<str>>(query: S) -> TsQuery {
    unreachable!("{STUB}")
}
pub fn ts_rank(v: TsVector, q: TsQuery) -> f32 {
    unreachable!("{STUB}")
}

pub trait Fts: Sized {
    fn fts_match(self, q: TsQuery) -> bool {
        unreachable!("{STUB}")
    }
}
impl Fts for TsVector {}
impl Fts for String {}
impl Fts for &str {}

pub fn position<S1: AsRef<str>, S2: AsRef<str>>(needle: S1, haystack: S2) -> i32 {
    unreachable!("{STUB}")
}
pub fn substring<S: AsRef<str>>(s: S, from: i32, count: i32) -> String {
    unreachable!("{STUB}")
}
pub fn replace<S: AsRef<str>, P: AsRef<str>, R: AsRef<str>>(s: S, pat: P, repl: R) -> String {
    unreachable!("{STUB}")
}
pub fn regexp_replace<S: AsRef<str>, P: AsRef<str>, R: AsRef<str>>(
    s: S,
    pat: P,
    repl: R,
) -> String {
    unreachable!("{STUB}")
}
pub fn regexp_match<S: AsRef<str>, P: AsRef<str>>(s: S, pat: P) -> Option<Vec<String>> {
    unreachable!("{STUB}")
}

pub fn exists<S>(subquery: S) -> bool {
    unreachable!("{STUB}")
}
pub fn not_exists<S>(subquery: S) -> bool {
    unreachable!("{STUB}")
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

pub fn count<T>(x: T) -> WindowExpr<i64> {
    unreachable!("{STUB}")
}
pub fn sum<T, R>(x: T) -> WindowExpr<R> {
    unreachable!("{STUB}")
}
pub fn avg<T, R>(x: T) -> WindowExpr<R> {
    unreachable!("{STUB}")
}
pub fn min<T, R>(x: T) -> WindowExpr<R> {
    unreachable!("{STUB}")
}
pub fn max<T, R>(x: T) -> WindowExpr<R> {
    unreachable!("{STUB}")
}

pub fn lag<T, R>(col: T, offset: i64) -> WindowExpr<Option<R>> {
    unreachable!("{STUB}")
}
pub fn lead<T, R>(col: T, offset: i64) -> WindowExpr<Option<R>> {
    unreachable!("{STUB}")
}

pub trait Text: Sized {
    fn like<S: AsRef<str>>(self, pattern: S) -> bool {
        unreachable!("{STUB}")
    }
    fn ilike<S: AsRef<str>>(self, pattern: S) -> bool {
        unreachable!("{STUB}")
    }
    fn not_like<S: AsRef<str>>(self, pattern: S) -> bool {
        unreachable!("{STUB}")
    }
    fn not_ilike<S: AsRef<str>>(self, pattern: S) -> bool {
        unreachable!("{STUB}")
    }
    fn concat<S: AsRef<str>>(self, other: S) -> String {
        unreachable!("{STUB}")
    }
    fn regex_match<S: AsRef<str>>(self, pattern: S) -> bool {
        unreachable!("{STUB}")
    }
    fn regex_imatch<S: AsRef<str>>(self, pattern: S) -> bool {
        unreachable!("{STUB}")
    }
    fn not_regex_match<S: AsRef<str>>(self, pattern: S) -> bool {
        unreachable!("{STUB}")
    }
    fn not_regex_imatch<S: AsRef<str>>(self, pattern: S) -> bool {
        unreachable!("{STUB}")
    }
}
impl Text for String {}
impl Text for &str {}
impl Text for Option<String> {}

pub trait In<T>: Sized {
    fn in_(self, set: &[T]) -> bool {
        unreachable!("{STUB}")
    }
    fn not_in(self, set: &[T]) -> bool {
        unreachable!("{STUB}")
    }
}
impl In<Self> for i64 {}
impl In<Self> for i32 {}
impl In<Self> for i16 {}
impl In<Self> for String {}

pub trait Array<T>: Sized {
    fn pg_contains(self, other: T) -> bool {
        unreachable!("{STUB}")
    }
    fn pg_overlaps(self, other: T) -> bool {
        unreachable!("{STUB}")
    }
}
impl Array<&[u8]> for Vec<u8> {}
impl Array<&[u8]> for &Vec<u8> {}
impl Array<&[i32]> for Vec<i32> {}
impl Array<&[i32]> for &Vec<i32> {}
impl Array<&[i64]> for Vec<i64> {}
impl Array<&[i64]> for &Vec<i64> {}
impl Array<&[i16]> for Vec<i16> {}
impl Array<&[i16]> for &Vec<i16> {}
impl Array<&[f32]> for Vec<f32> {}
impl Array<&[f32]> for &Vec<f32> {}
impl Array<&[f64]> for Vec<f64> {}
impl Array<&[f64]> for &Vec<f64> {}
impl Array<&[bool]> for Vec<bool> {}
impl Array<&[bool]> for &Vec<bool> {}
impl Array<&[&str]> for Vec<String> {}
impl Array<&[&str]> for &Vec<String> {}

pub trait Json: Sized {
    fn json(self, key: impl Into<String>) -> Self {
        unreachable!("{STUB}")
    }
    fn text(self, key: impl Into<String>) -> String {
        unreachable!("{STUB}")
    }
    fn json_path(self, path: &[&str]) -> Self {
        unreachable!("{STUB}")
    }
    fn text_path(self, path: &[&str]) -> String {
        unreachable!("{STUB}")
    }
}
impl Json for Vec<u8> {}

pub trait Ltree: Sized {
    fn is_descendant_of(self, _ancestor: crate::Ltree) -> bool {
        unreachable!("{STUB}")
    }
    fn is_ancestor_of(self, _descendant: crate::Ltree) -> bool {
        unreachable!("{STUB}")
    }
}
impl Ltree for crate::Ltree {}
impl Ltree for &crate::Ltree {}

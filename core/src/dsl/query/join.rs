use crate::dsl::{never::Never, query};

pub struct Plan2<A, B>(Never<(A, B)>);
pub struct Plan3<A, B, C>(Never<(A, B, C)>);
pub struct Plan4<A, B, C, D>(Never<(A, B, C, D)>);

pub struct Rows2<A, B>(Never<(A, B)>);
pub struct Rows3<A, B, C>(Never<(A, B, C)>);
pub struct Rows4<A, B, C, D>(Never<(A, B, C, D)>);

impl<A, B> Plan2<A, B> {
    pub fn filter(self, pred: impl FnOnce(A, B) -> bool) -> Rows2<A, B> {
        self.0.infer(pred)
    }

    pub fn join<C>(self, pred: impl FnOnce(A, B, C) -> bool) -> Plan3<A, B, C> {
        self.0.infer(pred)
    }
}

impl<A, B, C> Plan3<A, B, C> {
    pub fn filter(self, pred: impl FnOnce(A, B, C) -> bool) -> Rows3<A, B, C> {
        self.0.infer(pred)
    }

    pub fn join<D>(self, pred: impl FnOnce(A, B, C, D) -> bool) -> Plan4<A, B, C, D> {
        self.0.infer(pred)
    }
}

impl<A, B, C, D> Plan4<A, B, C, D> {
    pub fn filter(self, pred: impl FnOnce(A, B, C, D) -> bool) -> Rows4<A, B, C, D> {
        self.0.infer(pred)
    }
}

impl<A, B> Rows2<A, B> {
    pub fn one(self) -> (A, B) {
        self.0.infer(())
    }

    pub fn first(self) -> Option<(A, B)> {
        self.0.infer(())
    }

    pub fn stream(self) -> query::Stream<(A, B)> {
        self.0.infer(())
    }

    pub fn all<R>(self) -> Vec<R> {
        self.0.infer(())
    }

    pub fn count(self) -> i64 {
        self.0.infer(())
    }

    pub fn order_by<F, U>(self, key: F) -> Self
    where
        F: FnOnce(A, B) -> U,
    {
        self.0.infer(key)
    }

    pub fn select<F, R>(self, map: F) -> query::Query<R, query::Projected>
    where
        F: FnOnce(A, B) -> R,
    {
        self.0.infer(map)
    }
}

impl<A, B, C> Rows3<A, B, C> {
    pub fn one(self) -> (A, B, C) {
        self.0.infer(())
    }

    pub fn first(self) -> Option<(A, B, C)> {
        self.0.infer(())
    }

    pub fn stream(self) -> query::Stream<(A, B, C)> {
        self.0.infer(())
    }

    pub fn all<R>(self) -> Vec<R> {
        self.0.infer(())
    }

    pub fn order_by<F, U>(self, key: F) -> Self
    where
        F: FnOnce(A, B, C) -> U,
    {
        self.0.infer(key)
    }
}

impl<A, B, C, D> Rows4<A, B, C, D> {
    pub fn one(self) -> (A, B, C, D) {
        self.0.infer(())
    }

    pub fn first(self) -> Option<(A, B, C, D)> {
        self.0.infer(())
    }

    pub fn stream(self) -> query::Stream<(A, B, C, D)> {
        self.0.infer(())
    }

    pub fn all<R>(self) -> Vec<R> {
        self.0.infer(())
    }

    pub fn order_by<F, U>(self, key: F) -> Self
    where
        F: FnOnce(A, B, C, D) -> U,
    {
        self.0.infer(key)
    }
}

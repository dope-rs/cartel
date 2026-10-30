use crate::dsl::scope::never::Never;

pub mod join;
pub mod ops;

pub struct Filter<T>(pub(super) Never<T>);
pub struct Cte<T>(pub(super) Never<T>);
pub struct Select<T>(pub(super) Never<T>);
pub struct Stream<T>(Never<T>);

impl<T> Cte<T> {
    pub fn filter<F>(self, pred: F) -> Filter<T>
    where
        F: FnOnce(T) -> bool,
    {
        self.0.infer(pred)
    }
}

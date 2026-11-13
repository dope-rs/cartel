use crate::dsl::never::Never;

pub struct Insert<T>(Never<T>);
pub struct Update<T>(Never<T>);
pub struct Delete<T>(Never<T>);
pub struct Conflict<T>(Never<T>);

impl<T> Insert<T> {
    pub fn return_one(self) -> T {
        self.0.infer(())
    }

    pub fn return_first(self) -> Option<T> {
        self.0.infer(())
    }

    pub fn return_all(self) -> Vec<T> {
        self.0.infer(())
    }

    pub fn ignore(self) -> Self {
        self.0.infer(())
    }

    pub fn on_conflict<F, U>(self, key: F) -> Conflict<T>
    where
        F: FnOnce(T) -> U,
    {
        self.0.infer(key)
    }
}

impl<T> Conflict<T> {
    pub fn ignore(self) -> Insert<T> {
        self.0.infer(())
    }

    pub fn update<F>(self, update: F) -> Insert<T>
    where
        F: FnOnce(&mut T),
    {
        self.0.infer(update)
    }
}

impl<T> Update<T> {
    pub fn required(self) -> Self {
        self
    }

    pub fn return_one(self) -> T {
        self.0.infer(())
    }
}

impl<T> Delete<T> {
    pub fn return_first(self) -> Option<T> {
        self.0.infer(())
    }

    pub fn return_all(self) -> Vec<T> {
        self.0.infer(())
    }
}

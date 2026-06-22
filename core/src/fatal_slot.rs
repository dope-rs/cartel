pub struct FatalSlot<E> {
    inner: Option<E>,
}

impl<E> Default for FatalSlot<E> {
    fn default() -> Self {
        Self { inner: None }
    }
}

impl<E> FatalSlot<E> {
    pub fn is_failed(&self) -> bool {
        self.inner.is_some()
    }

    pub fn record(&mut self, err: E) {
        if self.inner.is_none() {
            self.inner = Some(err);
        }
    }

    pub fn as_ref(&self) -> Option<&E> {
        self.inner.as_ref()
    }

    pub fn clear(&mut self) {
        self.inner = None;
    }
}

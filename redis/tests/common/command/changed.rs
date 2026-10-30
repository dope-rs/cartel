/// The measuring clone and the encoded iterator have equal wire byte lengths
/// but different pair counts, so the frame-length check alone is insufficient.
pub struct Changed<'a> {
    pub(crate) measured: &'a [(&'a [u8], &'a [u8])],
    pub(crate) emitted: &'a [(&'a [u8], &'a [u8])],
}

impl Clone for Changed<'_> {
    fn clone(&self) -> Self {
        Self {
            measured: self.measured,
            emitted: self.measured,
        }
    }
}

impl<'a> Iterator for Changed<'a> {
    type Item = (&'a [u8], &'a [u8]);

    fn next(&mut self) -> Option<Self::Item> {
        let (pair, rest) = self.emitted.split_first()?;
        self.emitted = rest;
        Some(*pair)
    }
}

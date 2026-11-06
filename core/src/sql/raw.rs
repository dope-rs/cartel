use super::Text;

/// SQL fragments whose exact combined length is proven by their caller.
#[derive(Clone, Copy)]
pub struct Parts<'a> {
    parts: &'a [&'a str],
}

impl<'a> Parts<'a> {
    pub const fn new(parts: &'a [&'a str]) -> Self {
        Self { parts }
    }

    /// Compares the complete fragments to text without assembling a buffer.
    pub const fn matches(self, text: &str) -> bool {
        let mut text = text.as_bytes();
        let mut pi = 0;
        while pi < self.parts.len() {
            let part = self.parts[pi].as_bytes();
            let Some((head, tail)) = text.split_at_checked(part.len()) else {
                return false;
            };
            if pi + 1 == self.parts.len() && !tail.is_empty() {
                return false;
            }
            let mut index = 0;
            while index < part.len() {
                if part[index] != head[index] {
                    return false;
                }
                index += 1;
            }
            text = tail;
            pi += 1;
        }
        text.is_empty()
    }

    /// Concatenates complete fragments into their exact const-sized text.
    ///
    /// # Safety
    /// `N` must equal the non-overflowing sum of every fragment length.
    pub const unsafe fn concat<const N: usize>(self) -> Text<N> {
        let mut bytes = [0u8; N];
        let mut remaining: &mut [u8] = &mut bytes;
        let mut pi = 0;
        while pi < self.parts.len() {
            let part = self.parts[pi].as_bytes();
            let (head, tail) = remaining.split_at_mut(part.len());
            head.copy_from_slice(part);
            remaining = tail;
            pi += 1;
        }
        Text::new(bytes)
    }
}

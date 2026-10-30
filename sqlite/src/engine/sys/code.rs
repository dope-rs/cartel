#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(transparent)]
pub struct Code(i32);

impl Code {
    pub const fn new(code: i32) -> Self {
        Self(code)
    }

    pub const fn get(self) -> i32 {
        self.0
    }

    pub const fn primary(self) -> i32 {
        self.0 & 0xff
    }
}

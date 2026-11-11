pub mod deadline;
pub mod dsl;
pub mod frame;
pub mod handoff;
pub mod memory;
pub mod queue;
pub mod sql;

pub use dope_fiber::reply;

/// Writes unsigned decimal digits into the buffer's suffix and returns its start.
#[inline]
pub fn write_digits(buffer: &mut [u8; 20], mut value: u64) -> usize {
    let mut index = buffer.len();
    loop {
        index -= 1;
        buffer[index] = b'0' + (value % 10) as u8;
        value /= 10;
        if value == 0 {
            break;
        }
    }
    index
}

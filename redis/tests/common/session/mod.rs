use cartel_redis::client::{settings, settings::Builder};
use o3::base::time::Positive;

pub mod deadline;
pub mod held;
pub mod hello;

pub fn config(window: Positive) -> Result<settings::Config, settings::Error> {
    Builder::new(settings::Capacities {
        connections: 1,
        admission_waiters: 2,
        inflight: 2,
        request_frame_bytes: 128,
        response_bytes: 128,
        response_values: 2,
        response_frame_bytes: 128,
        response_frame_values: 2,
    })
    .window(window)
    .build()
}

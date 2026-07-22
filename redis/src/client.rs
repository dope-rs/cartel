use std::fmt;
use std::io;
use std::task::Poll;
use std::time::Duration;

use cartel_core::{Extract, Reply, Slot};
use dope::DriverContext;
use dope::manifold::Manifold;
use dope::manifold::connector::Connector;
use dope::manifold::connector::source::Dialer;
use dope::manifold::env::Env;
use dope::runtime::StorageFactory;
use dope_fiber::Fiber;
use dope_net::Transport;
use dope_net::wire::Wire;
use o3::buffer::{PoolLayout, Shared};

use crate::port::Port;
use crate::protocol::{Outcome, Session};
use crate::value::FromValue;
use crate::{Error, encode};

pub type GeoCoord = (f64, f64);

pub const DEFAULT_BACKOFF: Duration = Duration::from_millis(500);

pub const MAX_FRAME_CAPACITY: usize = 16 * 1024 * 1024;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Config {
    capacities: Capacities,
    request_pool: PoolLayout,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Capacities {
    pub connection: usize,
    pub waiters: usize,
    pub inflight: usize,
    pub request_entries: usize,
    pub request_bytes: usize,
    pub response_bytes: usize,
    pub response_values: usize,
    pub max_frame_bytes: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConfigError {
    ZeroConnectionCapacity,
    ZeroWaiterCapacity,
    ZeroInflightCapacity,
    ZeroRequestCapacity,
    ZeroRequestByteCapacity,
    ZeroResponseByteCapacity,
    ZeroResponseValueCapacity,
    ZeroMaxFrameCapacity,
    CapacityOverflow,
    MaxFrameCapacityExceeded,
    InflightBelowConnectionCapacity,
    RequestBelowConnectionCapacity,
    RequestBelowInflightCapacity,
    ResponseValueBelowInflightCapacity,
    FrameExceedsResponseByteCapacity,
}

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ZeroConnectionCapacity => f.write_str("connection_capacity must be positive"),
            Self::ZeroWaiterCapacity => f.write_str("waiter_capacity must be positive"),
            Self::ZeroInflightCapacity => f.write_str("inflight_capacity must be positive"),
            Self::ZeroRequestCapacity => f.write_str("request_capacity must be positive"),
            Self::ZeroRequestByteCapacity => f.write_str("request_byte_capacity must be positive"),
            Self::ZeroResponseByteCapacity => {
                f.write_str("response_byte_capacity must be positive")
            }
            Self::ZeroResponseValueCapacity => {
                f.write_str("response_value_capacity must be positive")
            }
            Self::ZeroMaxFrameCapacity => f.write_str("max_frame_capacity must be positive"),
            Self::CapacityOverflow => f.write_str("configured capacity overflows platform limits"),
            Self::MaxFrameCapacityExceeded => write!(
                f,
                "max_frame_capacity exceeds supported maximum of {MAX_FRAME_CAPACITY} bytes"
            ),
            Self::InflightBelowConnectionCapacity => {
                f.write_str("inflight_capacity must cover every connection")
            }
            Self::RequestBelowConnectionCapacity => {
                f.write_str("request_capacity must cover every connection")
            }
            Self::RequestBelowInflightCapacity => {
                f.write_str("request_capacity must not be less than inflight_capacity")
            }
            Self::ResponseValueBelowInflightCapacity => {
                f.write_str("response_value_capacity must cover every inflight response")
            }
            Self::FrameExceedsResponseByteCapacity => {
                f.write_str("max_frame_capacity must not exceed response_byte_capacity")
            }
        }
    }
}

impl std::error::Error for ConfigError {}

impl Config {
    pub const fn new(capacities: Capacities) -> Result<Self, ConfigError> {
        let Capacities {
            connection: connection_capacity,
            waiters: waiter_capacity,
            inflight: inflight_capacity,
            request_entries: request_capacity,
            request_bytes: request_byte_capacity,
            response_bytes: response_byte_capacity,
            response_values: response_value_capacity,
            max_frame_bytes: max_frame_capacity,
        } = capacities;
        if connection_capacity == 0 {
            return Err(ConfigError::ZeroConnectionCapacity);
        }
        if waiter_capacity == 0 {
            return Err(ConfigError::ZeroWaiterCapacity);
        }
        if inflight_capacity == 0 {
            return Err(ConfigError::ZeroInflightCapacity);
        }
        if request_capacity == 0 {
            return Err(ConfigError::ZeroRequestCapacity);
        }
        if request_byte_capacity == 0 {
            return Err(ConfigError::ZeroRequestByteCapacity);
        }
        if response_byte_capacity == 0 {
            return Err(ConfigError::ZeroResponseByteCapacity);
        }
        if response_value_capacity == 0 {
            return Err(ConfigError::ZeroResponseValueCapacity);
        }
        if max_frame_capacity == 0 {
            return Err(ConfigError::ZeroMaxFrameCapacity);
        }
        if inflight_capacity < connection_capacity {
            return Err(ConfigError::InflightBelowConnectionCapacity);
        }
        if request_capacity < connection_capacity {
            return Err(ConfigError::RequestBelowConnectionCapacity);
        }
        if request_capacity < inflight_capacity {
            return Err(ConfigError::RequestBelowInflightCapacity);
        }
        if response_value_capacity < inflight_capacity {
            return Err(ConfigError::ResponseValueBelowInflightCapacity);
        }
        if max_frame_capacity > response_byte_capacity {
            return Err(ConfigError::FrameExceedsResponseByteCapacity);
        }
        if connection_capacity > u32::MAX as usize
            || waiter_capacity > u32::MAX as usize
            || inflight_capacity > u32::MAX as usize / 2
            || request_capacity > u32::MAX as usize
            || request_byte_capacity > u32::MAX as usize
            || response_value_capacity > u32::MAX as usize
            || match waiter_capacity.checked_mul(2) {
                Some(capacity) => capacity.checked_next_power_of_two().is_none(),
                None => true,
            }
        {
            return Err(ConfigError::CapacityOverflow);
        }
        let request_pool =
            match PoolLayout::new(request_capacity as u32, request_byte_capacity as u32) {
                Ok(layout) => layout,
                Err(_) => return Err(ConfigError::CapacityOverflow),
            };
        if max_frame_capacity > MAX_FRAME_CAPACITY {
            return Err(ConfigError::MaxFrameCapacityExceeded);
        }
        Ok(Self {
            capacities,
            request_pool,
        })
    }

    pub const fn factory(self) -> Factory {
        Factory { config: self }
    }

    pub const fn connection_capacity(self) -> usize {
        self.capacities.connection
    }

    pub const fn waiter_capacity(self) -> usize {
        self.capacities.waiters
    }

    pub const fn inflight_capacity(self) -> usize {
        self.capacities.inflight
    }

    pub const fn request_capacity(self) -> usize {
        self.capacities.request_entries
    }

    pub const fn request_byte_capacity(self) -> usize {
        self.capacities.request_bytes
    }

    pub const fn request_pool(self) -> PoolLayout {
        self.request_pool
    }

    pub const fn response_byte_capacity(self) -> usize {
        self.capacities.response_bytes
    }

    pub const fn response_value_capacity(self) -> usize {
        self.capacities.response_values
    }

    pub const fn max_frame_capacity(self) -> usize {
        self.capacities.max_frame_bytes
    }
}

#[derive(Clone, Copy)]
pub struct Factory {
    config: Config,
}

pub struct Store<'d> {
    port: Port<'d>,
}

impl<'d> Store<'d> {
    pub fn redis(&'d self) -> Redis<'d> {
        Redis { port: &self.port }
    }
}

impl StorageFactory for Factory {
    type Output<'d> = Store<'d>;

    fn build<'d>(self, driver: &mut DriverContext<'_, 'd>) -> Self::Output<'d> {
        Store {
            port: Port::new(self.config, driver.driver_ref()),
        }
    }
}

pub struct Connect<S> {
    pub topology: S,
}

pub struct Redis<'d> {
    port: &'d Port<'d>,
}

impl Copy for Redis<'_> {}

impl Clone for Redis<'_> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<'d> Redis<'d> {
    pub fn connect<const ID: u8, S, E>(
        self,
        config: Connect<S>,
        driver: &mut DriverContext<'_, 'd>,
    ) -> io::Result<impl Manifold<'d> + 'd>
    where
        S: Dialer<E::Transport> + 'd,
        E: Env + 'd,
        E::Transport: Transport<Addr: Clone>,
    {
        Connector::<'d, ID, Session<'d>, S, E>::new(
            Session::new(self.port),
            config.topology,
            self.port.capacity(),
            driver,
        )
    }

    pub fn connect_configured<const ID: u8, S, E>(
        self,
        config: Connect<S>,
        wire: <E::Wire as Wire>::InitConfig,
        driver: &mut DriverContext<'_, 'd>,
    ) -> io::Result<impl Manifold<'d> + 'd>
    where
        S: Dialer<E::Transport> + 'd,
        E: Env + 'd,
        E::Transport: Transport<Addr: Clone>,
    {
        Connector::<'d, ID, Session<'d>, S, E>::new(
            Session::new(self.port),
            config.topology,
            self.port.capacity(),
            driver,
        )
        .map(|connector| connector.config(wire))
    }
}

struct ExtractValue;

unsafe impl Extract<Outcome> for ExtractValue {
    type Output = Result<crate::value::Value, Error>;

    fn extract(slot: &mut Slot<'_, Outcome>) -> Option<Self::Output> {
        if !slot.completed() {
            return None;
        }
        if slot.take_overflow() {
            return Some(Err(Error::ResponseBufferCapacity));
        }
        Some(match slot.pop() {
            Some(Ok(frame)) => frame.into_value(),
            Some(Err(error)) => Err(error),
            None => Err(Error::Redis("redis reply slot empty".into())),
        })
    }
}

pub trait Ops<'d> {
    fn wait_active(self) -> impl Fiber<'d, Output = Result<(), Error>>;

    fn get(self, key: &[u8]) -> impl Fiber<'d, Output = Result<Option<Shared>, Error>>;
    fn set(self, key: &[u8], value: &[u8]) -> impl Fiber<'d, Output = Result<(), Error>>;
    fn set_ex(
        self,
        key: &[u8],
        value: &[u8],
        ttl: Duration,
    ) -> impl Fiber<'d, Output = Result<(), Error>>;
    fn set_px(
        self,
        key: &[u8],
        value: &[u8],
        ttl: Duration,
    ) -> impl Fiber<'d, Output = Result<(), Error>>;
    fn set_nx(self, key: &[u8], value: &[u8]) -> impl Fiber<'d, Output = Result<bool, Error>>;
    fn del(self, keys: &[&[u8]]) -> impl Fiber<'d, Output = Result<u64, Error>>;
    fn exists(self, keys: &[&[u8]]) -> impl Fiber<'d, Output = Result<u64, Error>>;
    fn incr(self, key: &[u8]) -> impl Fiber<'d, Output = Result<i64, Error>>;
    fn decr(self, key: &[u8]) -> impl Fiber<'d, Output = Result<i64, Error>>;
    fn incr_by(self, key: &[u8], by: i64) -> impl Fiber<'d, Output = Result<i64, Error>>;
    fn expire(self, key: &[u8], ttl: Duration) -> impl Fiber<'d, Output = Result<bool, Error>>;
    fn ttl(self, key: &[u8]) -> impl Fiber<'d, Output = Result<i64, Error>>;
    fn mget(self, keys: &[&[u8]]) -> impl Fiber<'d, Output = Result<Vec<Option<Shared>>, Error>>;
    fn mset(self, kv: &[(&[u8], &[u8])]) -> impl Fiber<'d, Output = Result<(), Error>>;
    fn ping(self) -> impl Fiber<'d, Output = Result<(), Error>>;
    fn cmd<R: FromValue>(self, args: &[&[u8]]) -> impl Fiber<'d, Output = Result<R, Error>>;
    fn publish(self, channel: &[u8], message: &[u8])
    -> impl Fiber<'d, Output = Result<i64, Error>>;
    fn hget(
        self,
        key: &[u8],
        field: &[u8],
    ) -> impl Fiber<'d, Output = Result<Option<Shared>, Error>>;
    fn hset(
        self,
        key: &[u8],
        field: &[u8],
        value: &[u8],
    ) -> impl Fiber<'d, Output = Result<bool, Error>>;
    fn hset_multi(
        self,
        key: &[u8],
        fv: &[(&[u8], &[u8])],
    ) -> impl Fiber<'d, Output = Result<u64, Error>>;
    fn hmget(
        self,
        key: &[u8],
        fields: &[&[u8]],
    ) -> impl Fiber<'d, Output = Result<Vec<Option<Shared>>, Error>>;
    fn hdel(self, key: &[u8], fields: &[&[u8]]) -> impl Fiber<'d, Output = Result<u64, Error>>;
    fn hget_all(self, key: &[u8]) -> impl Fiber<'d, Output = Result<Vec<(Shared, Shared)>, Error>>;
    fn hlen(self, key: &[u8]) -> impl Fiber<'d, Output = Result<u64, Error>>;
    fn hexists(self, key: &[u8], field: &[u8]) -> impl Fiber<'d, Output = Result<bool, Error>>;
    fn hincr_by(
        self,
        key: &[u8],
        field: &[u8],
        by: i64,
    ) -> impl Fiber<'d, Output = Result<i64, Error>>;
    fn sadd(self, key: &[u8], members: &[&[u8]]) -> impl Fiber<'d, Output = Result<u64, Error>>;
    fn srem(self, key: &[u8], members: &[&[u8]]) -> impl Fiber<'d, Output = Result<u64, Error>>;
    fn smembers(self, key: &[u8]) -> impl Fiber<'d, Output = Result<Vec<Shared>, Error>>;
    fn sismember(self, key: &[u8], member: &[u8]) -> impl Fiber<'d, Output = Result<bool, Error>>;
    fn scard(self, key: &[u8]) -> impl Fiber<'d, Output = Result<u64, Error>>;
    fn zadd<M: AsRef<[u8]>>(
        self,
        key: &[u8],
        score: f64,
        member: M,
    ) -> impl Fiber<'d, Output = Result<u64, Error>>;
    fn zrem(self, key: &[u8], members: &[&[u8]]) -> impl Fiber<'d, Output = Result<u64, Error>>;
    fn zrange(
        self,
        key: &[u8],
        start: i64,
        stop: i64,
    ) -> impl Fiber<'d, Output = Result<Vec<Shared>, Error>>;
    fn zrange_with_scores(
        self,
        key: &[u8],
        start: i64,
        stop: i64,
    ) -> impl Fiber<'d, Output = Result<Vec<(Shared, f64)>, Error>>;
    fn zrev_range_with_scores(
        self,
        key: &[u8],
        start: i64,
        stop: i64,
    ) -> impl Fiber<'d, Output = Result<Vec<(Shared, f64)>, Error>>;
    fn zrange_by_score(
        self,
        key: &[u8],
        min: f64,
        max: f64,
    ) -> impl Fiber<'d, Output = Result<Vec<Shared>, Error>>;
    fn zrank(
        self,
        key: &[u8],
        member: &[u8],
    ) -> impl Fiber<'d, Output = Result<Option<u64>, Error>>;
    fn zrev_rank<M: AsRef<[u8]>>(
        self,
        key: &[u8],
        member: M,
    ) -> impl Fiber<'d, Output = Result<Option<u64>, Error>>;
    fn zscore(
        self,
        key: &[u8],
        member: &[u8],
    ) -> impl Fiber<'d, Output = Result<Option<f64>, Error>>;
    fn zcard(self, key: &[u8]) -> impl Fiber<'d, Output = Result<u64, Error>>;
    fn zincr_by(
        self,
        key: &[u8],
        by: f64,
        member: &[u8],
    ) -> impl Fiber<'d, Output = Result<f64, Error>>;
    fn lpush(self, key: &[u8], values: &[&[u8]]) -> impl Fiber<'d, Output = Result<u64, Error>>;
    fn rpush(self, key: &[u8], values: &[&[u8]]) -> impl Fiber<'d, Output = Result<u64, Error>>;
    fn lpop(self, key: &[u8]) -> impl Fiber<'d, Output = Result<Option<Shared>, Error>>;
    fn rpop(self, key: &[u8]) -> impl Fiber<'d, Output = Result<Option<Shared>, Error>>;
    fn lrange(
        self,
        key: &[u8],
        start: i64,
        stop: i64,
    ) -> impl Fiber<'d, Output = Result<Vec<Shared>, Error>>;
    fn llen(self, key: &[u8]) -> impl Fiber<'d, Output = Result<u64, Error>>;
    fn get_set(
        self,
        key: &[u8],
        value: &[u8],
    ) -> impl Fiber<'d, Output = Result<Option<Shared>, Error>>;
    fn get_del(self, key: &[u8]) -> impl Fiber<'d, Output = Result<Option<Shared>, Error>>;
    fn append(self, key: &[u8], value: &[u8]) -> impl Fiber<'d, Output = Result<u64, Error>>;
    fn strlen(self, key: &[u8]) -> impl Fiber<'d, Output = Result<u64, Error>>;
    fn incr_by_float(self, key: &[u8], by: f64) -> impl Fiber<'d, Output = Result<f64, Error>>;
    fn key_type(self, key: &[u8]) -> impl Fiber<'d, Output = Result<Shared, Error>>;
    fn rename(self, src: &[u8], dst: &[u8]) -> impl Fiber<'d, Output = Result<(), Error>>;
    fn persist(self, key: &[u8]) -> impl Fiber<'d, Output = Result<bool, Error>>;
    fn unlink(self, keys: &[&[u8]]) -> impl Fiber<'d, Output = Result<u64, Error>>;
    fn db_size(self) -> impl Fiber<'d, Output = Result<u64, Error>>;
    fn scan(
        self,
        cursor: u64,
        match_pattern: Option<&[u8]>,
        count: Option<u64>,
    ) -> impl Fiber<'d, Output = Result<(u64, Vec<Shared>), Error>>;
    fn pf_add(self, key: &[u8], elements: &[&[u8]])
    -> impl Fiber<'d, Output = Result<bool, Error>>;
    fn pf_count(self, keys: &[&[u8]]) -> impl Fiber<'d, Output = Result<u64, Error>>;
    fn pf_merge(self, dest: &[u8], sources: &[&[u8]])
    -> impl Fiber<'d, Output = Result<(), Error>>;
    fn bit_count(self, key: &[u8]) -> impl Fiber<'d, Output = Result<u64, Error>>;
    fn bit_count_range(
        self,
        key: &[u8],
        start: i64,
        end: i64,
    ) -> impl Fiber<'d, Output = Result<u64, Error>>;
    fn set_bit(
        self,
        key: &[u8],
        offset: u64,
        value: bool,
    ) -> impl Fiber<'d, Output = Result<bool, Error>>;
    fn get_bit(self, key: &[u8], offset: u64) -> impl Fiber<'d, Output = Result<bool, Error>>;
    fn bit_op(
        self,
        op: &[u8],
        dest: &[u8],
        sources: &[&[u8]],
    ) -> impl Fiber<'d, Output = Result<u64, Error>>;
    fn bit_pos(self, key: &[u8], bit: bool) -> impl Fiber<'d, Output = Result<i64, Error>>;
    fn geo_add(
        self,
        key: &[u8],
        longitude: f64,
        latitude: f64,
        member: &[u8],
    ) -> impl Fiber<'d, Output = Result<u64, Error>>;
    fn geo_dist(
        self,
        key: &[u8],
        member1: &[u8],
        member2: &[u8],
        unit: Option<&[u8]>,
    ) -> impl Fiber<'d, Output = Result<Option<f64>, Error>>;
    fn geo_pos(
        self,
        key: &[u8],
        members: &[&[u8]],
    ) -> impl Fiber<'d, Output = Result<Vec<Option<GeoCoord>>, Error>>;
    fn geo_search_radius(
        self,
        key: &[u8],
        longitude: f64,
        latitude: f64,
        radius: f64,
        unit: &[u8],
    ) -> impl Fiber<'d, Output = Result<Vec<Shared>, Error>>;
    fn info(self, section: Option<&[u8]>) -> impl Fiber<'d, Output = Result<Shared, Error>>;
    fn client_get_name(self) -> impl Fiber<'d, Output = Result<Option<Shared>, Error>>;
    fn client_set_name(self, name: &[u8]) -> impl Fiber<'d, Output = Result<(), Error>>;
    fn client_id(self) -> impl Fiber<'d, Output = Result<i64, Error>>;
    fn client_list(self) -> impl Fiber<'d, Output = Result<Shared, Error>>;
    fn client_kill_addr(self, addr: &[u8]) -> impl Fiber<'d, Output = Result<u64, Error>>;
    fn config_get(
        self,
        parameter: &[u8],
    ) -> impl Fiber<'d, Output = Result<Vec<(Shared, Shared)>, Error>>;
    fn config_set(
        self,
        parameter: &[u8],
        value: &[u8],
    ) -> impl Fiber<'d, Output = Result<(), Error>>;
    fn config_reset_stat(self) -> impl Fiber<'d, Output = Result<(), Error>>;
    fn debug_object(self, key: &[u8]) -> impl Fiber<'d, Output = Result<Shared, Error>>;
    fn auth(self, password: &[u8]) -> impl Fiber<'d, Output = Result<(), Error>>;
    fn auth_user(
        self,
        username: &[u8],
        password: &[u8],
    ) -> impl Fiber<'d, Output = Result<(), Error>>;
    fn select_db(self, db: u32) -> impl Fiber<'d, Output = Result<(), Error>>;
    fn hello(
        self,
        protocol: Option<u8>,
    ) -> impl Fiber<'d, Output = Result<crate::value::Value, Error>>;
}

impl<'d> Ops<'d> for Redis<'d> {
    fn wait_active(self) -> impl Fiber<'d, Output = Result<(), Error>> {
        let redis = self;
        dope_fiber::wait_fn(move |cx, waiter| {
            if redis.port.active() {
                return Poll::Ready(Ok(()));
            }
            if let Some(message) = redis.port.fatal_message() {
                return Poll::Ready(Err(Error::Redis(message)));
            }
            if redis.port.try_register_active(waiter, cx.as_ref()) {
                Poll::Pending
            } else {
                Poll::Ready(Err(Error::WaiterCapacity))
            }
        })
    }

    fn get(self, key: &[u8]) -> impl Fiber<'d, Output = Result<Option<Shared>, Error>> {
        Redis::dispatch::<Option<Shared>>(self, move |out| encode::cmd_get(out, key))
    }

    fn set(self, key: &[u8], value: &[u8]) -> impl Fiber<'d, Output = Result<(), Error>> {
        Redis::dispatch::<()>(self, move |out| encode::cmd_set(out, key, value))
    }

    fn set_ex(
        self,
        key: &[u8],
        value: &[u8],
        ttl: Duration,
    ) -> impl Fiber<'d, Output = Result<(), Error>> {
        let secs = ttl.as_secs().max(1);
        Redis::dispatch::<()>(self, move |out| encode::cmd_set_ex(out, key, value, secs))
    }

    fn set_px(
        self,
        key: &[u8],
        value: &[u8],
        ttl: Duration,
    ) -> impl Fiber<'d, Output = Result<(), Error>> {
        let ms = u64::try_from(ttl.as_millis()).unwrap_or(u64::MAX).max(1);
        Redis::dispatch::<()>(self, move |out| encode::cmd_set_px(out, key, value, ms))
    }

    fn set_nx(self, key: &[u8], value: &[u8]) -> impl Fiber<'d, Output = Result<bool, Error>> {
        Redis::dispatch::<bool>(self, move |out| encode::cmd_set_nx(out, key, value))
    }

    fn del(self, keys: &[&[u8]]) -> impl Fiber<'d, Output = Result<u64, Error>> {
        let fut = (!keys.is_empty())
            .then(|| Redis::dispatch::<u64>(self, move |out| encode::cmd_del(out, keys)));
        dope_fiber::fiber!('d => async move {
            match fut {
                Some(f) => f.await,
                None => Ok(0),
            }
        })
    }

    fn exists(self, keys: &[&[u8]]) -> impl Fiber<'d, Output = Result<u64, Error>> {
        let fut = (!keys.is_empty())
            .then(|| Redis::dispatch::<u64>(self, move |out| encode::cmd_exists(out, keys)));
        dope_fiber::fiber!('d => async move {
            match fut {
                Some(f) => f.await,
                None => Ok(0),
            }
        })
    }

    fn incr(self, key: &[u8]) -> impl Fiber<'d, Output = Result<i64, Error>> {
        Redis::dispatch::<i64>(self, move |out| encode::cmd_incr(out, key))
    }

    fn decr(self, key: &[u8]) -> impl Fiber<'d, Output = Result<i64, Error>> {
        Redis::dispatch::<i64>(self, move |out| encode::cmd_decr(out, key))
    }

    fn incr_by(self, key: &[u8], by: i64) -> impl Fiber<'d, Output = Result<i64, Error>> {
        Redis::dispatch::<i64>(self, move |out| encode::cmd_incrby(out, key, by))
    }

    fn expire(self, key: &[u8], ttl: Duration) -> impl Fiber<'d, Output = Result<bool, Error>> {
        let secs = ttl.as_secs().max(1);
        Redis::dispatch::<bool>(self, move |out| encode::cmd_expire(out, key, secs))
    }

    fn ttl(self, key: &[u8]) -> impl Fiber<'d, Output = Result<i64, Error>> {
        Redis::dispatch::<i64>(self, move |out| encode::cmd_ttl(out, key))
    }

    fn mget(self, keys: &[&[u8]]) -> impl Fiber<'d, Output = Result<Vec<Option<Shared>>, Error>> {
        let fut = (!keys.is_empty()).then(|| {
            Redis::dispatch::<Vec<Option<Shared>>>(self, move |out| encode::cmd_mget(out, keys))
        });
        dope_fiber::fiber!('d => async move {
            match fut {
                Some(f) => f.await,
                None => Ok(Vec::new()),
            }
        })
    }

    fn mset(self, kv: &[(&[u8], &[u8])]) -> impl Fiber<'d, Output = Result<(), Error>> {
        let fut = (!kv.is_empty())
            .then(|| Redis::dispatch::<()>(self, move |out| encode::cmd_mset(out, kv)));
        dope_fiber::fiber!('d => async move {
            match fut {
                Some(f) => f.await,
                None => Ok(()),
            }
        })
    }

    fn ping(self) -> impl Fiber<'d, Output = Result<(), Error>> {
        Redis::dispatch::<()>(self, |out| encode::cmd_ping(out))
    }

    fn cmd<R: FromValue>(self, args: &[&[u8]]) -> impl Fiber<'d, Output = Result<R, Error>> {
        Redis::dispatch::<R>(self, move |out| encode::cmd_raw(out, args))
    }

    fn publish(
        self,
        channel: &[u8],
        message: &[u8],
    ) -> impl Fiber<'d, Output = Result<i64, Error>> {
        Redis::dispatch::<i64>(self, move |out| {
            encode::cmd_raw(out, &[b"PUBLISH", channel, message])
        })
    }

    fn hget(
        self,
        key: &[u8],
        field: &[u8],
    ) -> impl Fiber<'d, Output = Result<Option<Shared>, Error>> {
        Redis::dispatch::<Option<Shared>>(self, move |out| encode::cmd_hget(out, key, field))
    }

    fn hset(
        self,
        key: &[u8],
        field: &[u8],
        value: &[u8],
    ) -> impl Fiber<'d, Output = Result<bool, Error>> {
        let fut = Redis::dispatch::<i64>(self, move |out| {
            encode::cmd_hset_pairs(out, key, &[(field, value)])
        });
        dope_fiber::fiber!('d => async move { Ok(fut.await? != 0) })
    }

    fn hset_multi(
        self,
        key: &[u8],
        fv: &[(&[u8], &[u8])],
    ) -> impl Fiber<'d, Output = Result<u64, Error>> {
        let fut = (!fv.is_empty())
            .then(|| Redis::dispatch::<u64>(self, move |out| encode::cmd_hset_pairs(out, key, fv)));
        dope_fiber::fiber!('d => async move {
            match fut {
                Some(f) => f.await,
                None => Ok(0),
            }
        })
    }

    fn hmget(
        self,
        key: &[u8],
        fields: &[&[u8]],
    ) -> impl Fiber<'d, Output = Result<Vec<Option<Shared>>, Error>> {
        let fut = (!fields.is_empty()).then(|| {
            Redis::dispatch::<Vec<Option<Shared>>>(self, move |out| {
                encode::cmd_hmget(out, key, fields)
            })
        });
        dope_fiber::fiber!('d => async move {
            match fut {
                Some(f) => f.await,
                None => Ok(Vec::new()),
            }
        })
    }

    fn hdel(self, key: &[u8], fields: &[&[u8]]) -> impl Fiber<'d, Output = Result<u64, Error>> {
        let fut = (!fields.is_empty())
            .then(|| Redis::dispatch::<u64>(self, move |out| encode::cmd_hdel(out, key, fields)));
        dope_fiber::fiber!('d => async move {
            match fut {
                Some(f) => f.await,
                None => Ok(0),
            }
        })
    }

    fn hget_all(self, key: &[u8]) -> impl Fiber<'d, Output = Result<Vec<(Shared, Shared)>, Error>> {
        Redis::dispatch::<Vec<(Shared, Shared)>>(self, move |out| encode::cmd_hgetall(out, key))
    }

    fn hlen(self, key: &[u8]) -> impl Fiber<'d, Output = Result<u64, Error>> {
        Redis::dispatch::<u64>(self, move |out| encode::cmd_hlen(out, key))
    }

    fn hexists(self, key: &[u8], field: &[u8]) -> impl Fiber<'d, Output = Result<bool, Error>> {
        Redis::dispatch::<bool>(self, move |out| encode::cmd_hexists(out, key, field))
    }

    fn hincr_by(
        self,
        key: &[u8],
        field: &[u8],
        by: i64,
    ) -> impl Fiber<'d, Output = Result<i64, Error>> {
        Redis::dispatch::<i64>(self, move |out| encode::cmd_hincrby(out, key, field, by))
    }

    fn sadd(self, key: &[u8], members: &[&[u8]]) -> impl Fiber<'d, Output = Result<u64, Error>> {
        let fut = (!members.is_empty())
            .then(|| Redis::dispatch::<u64>(self, move |out| encode::cmd_sadd(out, key, members)));
        dope_fiber::fiber!('d => async move {
            match fut {
                Some(f) => f.await,
                None => Ok(0),
            }
        })
    }

    fn srem(self, key: &[u8], members: &[&[u8]]) -> impl Fiber<'d, Output = Result<u64, Error>> {
        let fut = (!members.is_empty())
            .then(|| Redis::dispatch::<u64>(self, move |out| encode::cmd_srem(out, key, members)));
        dope_fiber::fiber!('d => async move {
            match fut {
                Some(f) => f.await,
                None => Ok(0),
            }
        })
    }

    fn smembers(self, key: &[u8]) -> impl Fiber<'d, Output = Result<Vec<Shared>, Error>> {
        Redis::dispatch::<Vec<Shared>>(self, move |out| encode::cmd_smembers(out, key))
    }

    fn sismember(self, key: &[u8], member: &[u8]) -> impl Fiber<'d, Output = Result<bool, Error>> {
        Redis::dispatch::<bool>(self, move |out| encode::cmd_sismember(out, key, member))
    }

    fn scard(self, key: &[u8]) -> impl Fiber<'d, Output = Result<u64, Error>> {
        Redis::dispatch::<u64>(self, move |out| encode::cmd_scard(out, key))
    }

    fn zadd<M: AsRef<[u8]>>(
        self,
        key: &[u8],
        score: f64,
        member: M,
    ) -> impl Fiber<'d, Output = Result<u64, Error>> {
        Redis::dispatch::<u64>(self, move |out| {
            encode::cmd_zadd(out, key, score, member.as_ref())
        })
    }

    fn zrem(self, key: &[u8], members: &[&[u8]]) -> impl Fiber<'d, Output = Result<u64, Error>> {
        let fut = (!members.is_empty())
            .then(|| Redis::dispatch::<u64>(self, move |out| encode::cmd_zrem(out, key, members)));
        dope_fiber::fiber!('d => async move {
            match fut {
                Some(f) => f.await,
                None => Ok(0),
            }
        })
    }

    fn zrange(
        self,
        key: &[u8],
        start: i64,
        stop: i64,
    ) -> impl Fiber<'d, Output = Result<Vec<Shared>, Error>> {
        Redis::dispatch::<Vec<Shared>>(self, move |out| encode::cmd_zrange(out, key, start, stop))
    }

    fn zrange_with_scores(
        self,
        key: &[u8],
        start: i64,
        stop: i64,
    ) -> impl Fiber<'d, Output = Result<Vec<(Shared, f64)>, Error>> {
        Redis::dispatch::<Vec<(Shared, f64)>>(self, move |out| {
            encode::cmd_zrange_with_scores(out, key, start, stop)
        })
    }

    fn zrev_range_with_scores(
        self,
        key: &[u8],
        start: i64,
        stop: i64,
    ) -> impl Fiber<'d, Output = Result<Vec<(Shared, f64)>, Error>> {
        Redis::dispatch::<Vec<(Shared, f64)>>(self, move |out| {
            encode::cmd_zrevrange_with_scores(out, key, start, stop)
        })
    }

    fn zrange_by_score(
        self,
        key: &[u8],
        min: f64,
        max: f64,
    ) -> impl Fiber<'d, Output = Result<Vec<Shared>, Error>> {
        Redis::dispatch::<Vec<Shared>>(self, move |out| {
            encode::cmd_zrangebyscore(out, key, min, max)
        })
    }

    fn zrank(
        self,
        key: &[u8],
        member: &[u8],
    ) -> impl Fiber<'d, Output = Result<Option<u64>, Error>> {
        Redis::dispatch::<Option<u64>>(self, move |out| encode::cmd_zrank(out, key, member))
    }

    fn zrev_rank<M: AsRef<[u8]>>(
        self,
        key: &[u8],
        member: M,
    ) -> impl Fiber<'d, Output = Result<Option<u64>, Error>> {
        Redis::dispatch::<Option<u64>>(self, move |out| {
            encode::cmd_zrevrank(out, key, member.as_ref())
        })
    }

    fn zscore(
        self,
        key: &[u8],
        member: &[u8],
    ) -> impl Fiber<'d, Output = Result<Option<f64>, Error>> {
        Redis::dispatch::<Option<f64>>(self, move |out| encode::cmd_zscore(out, key, member))
    }

    fn zcard(self, key: &[u8]) -> impl Fiber<'d, Output = Result<u64, Error>> {
        Redis::dispatch::<u64>(self, move |out| encode::cmd_zcard(out, key))
    }

    fn zincr_by(
        self,
        key: &[u8],
        by: f64,
        member: &[u8],
    ) -> impl Fiber<'d, Output = Result<f64, Error>> {
        Redis::dispatch::<f64>(self, move |out| encode::cmd_zincrby(out, key, by, member))
    }

    fn lpush(self, key: &[u8], values: &[&[u8]]) -> impl Fiber<'d, Output = Result<u64, Error>> {
        let fut = (!values.is_empty())
            .then(|| Redis::dispatch::<u64>(self, move |out| encode::cmd_lpush(out, key, values)));
        dope_fiber::fiber!('d => async move {
            match fut {
                Some(f) => f.await,
                None => Ok(0),
            }
        })
    }

    fn rpush(self, key: &[u8], values: &[&[u8]]) -> impl Fiber<'d, Output = Result<u64, Error>> {
        let fut = (!values.is_empty())
            .then(|| Redis::dispatch::<u64>(self, move |out| encode::cmd_rpush(out, key, values)));
        dope_fiber::fiber!('d => async move {
            match fut {
                Some(f) => f.await,
                None => Ok(0),
            }
        })
    }

    fn lpop(self, key: &[u8]) -> impl Fiber<'d, Output = Result<Option<Shared>, Error>> {
        Redis::dispatch::<Option<Shared>>(self, move |out| encode::cmd_lpop(out, key))
    }

    fn rpop(self, key: &[u8]) -> impl Fiber<'d, Output = Result<Option<Shared>, Error>> {
        Redis::dispatch::<Option<Shared>>(self, move |out| encode::cmd_rpop(out, key))
    }

    fn lrange(
        self,
        key: &[u8],
        start: i64,
        stop: i64,
    ) -> impl Fiber<'d, Output = Result<Vec<Shared>, Error>> {
        Redis::dispatch::<Vec<Shared>>(self, move |out| encode::cmd_lrange(out, key, start, stop))
    }

    fn llen(self, key: &[u8]) -> impl Fiber<'d, Output = Result<u64, Error>> {
        Redis::dispatch::<u64>(self, move |out| encode::cmd_llen(out, key))
    }

    fn get_set(
        self,
        key: &[u8],
        value: &[u8],
    ) -> impl Fiber<'d, Output = Result<Option<Shared>, Error>> {
        Redis::dispatch::<Option<Shared>>(self, move |out| encode::cmd_getset(out, key, value))
    }

    fn get_del(self, key: &[u8]) -> impl Fiber<'d, Output = Result<Option<Shared>, Error>> {
        Redis::dispatch::<Option<Shared>>(self, move |out| encode::cmd_getdel(out, key))
    }

    fn append(self, key: &[u8], value: &[u8]) -> impl Fiber<'d, Output = Result<u64, Error>> {
        Redis::dispatch::<u64>(self, move |out| encode::cmd_append(out, key, value))
    }

    fn strlen(self, key: &[u8]) -> impl Fiber<'d, Output = Result<u64, Error>> {
        Redis::dispatch::<u64>(self, move |out| encode::cmd_strlen(out, key))
    }

    fn incr_by_float(self, key: &[u8], by: f64) -> impl Fiber<'d, Output = Result<f64, Error>> {
        Redis::dispatch::<f64>(self, move |out| encode::cmd_incrbyfloat(out, key, by))
    }

    fn key_type(self, key: &[u8]) -> impl Fiber<'d, Output = Result<Shared, Error>> {
        let fut = Redis::dispatch::<crate::Value>(self, move |out| encode::cmd_type(out, key));
        dope_fiber::fiber!('d => async move {
            match fut.await? {
                crate::Value::Status(b) => Ok(b),
                crate::Value::Bulk(b) => Ok(b),
                _ => Err(Error::Redis("unexpected TYPE response".into())),
            }
        })
    }

    fn rename(self, src: &[u8], dst: &[u8]) -> impl Fiber<'d, Output = Result<(), Error>> {
        Redis::dispatch::<()>(self, move |out| encode::cmd_rename(out, src, dst))
    }

    fn persist(self, key: &[u8]) -> impl Fiber<'d, Output = Result<bool, Error>> {
        Redis::dispatch::<bool>(self, move |out| encode::cmd_persist(out, key))
    }

    fn unlink(self, keys: &[&[u8]]) -> impl Fiber<'d, Output = Result<u64, Error>> {
        let fut = (!keys.is_empty())
            .then(|| Redis::dispatch::<u64>(self, move |out| encode::cmd_unlink(out, keys)));
        dope_fiber::fiber!('d => async move {
            match fut {
                Some(f) => f.await,
                None => Ok(0),
            }
        })
    }

    fn db_size(self) -> impl Fiber<'d, Output = Result<u64, Error>> {
        Redis::dispatch::<u64>(self, |out| encode::cmd_dbsize(out))
    }

    fn scan(
        self,
        cursor: u64,
        match_pattern: Option<&[u8]>,
        count: Option<u64>,
    ) -> impl Fiber<'d, Output = Result<(u64, Vec<Shared>), Error>> {
        Redis::dispatch::<(u64, Vec<Shared>)>(self, move |out| {
            encode::cmd_scan(out, cursor, match_pattern, count)
        })
    }

    fn pf_add(
        self,
        key: &[u8],
        elements: &[&[u8]],
    ) -> impl Fiber<'d, Output = Result<bool, Error>> {
        let fut = Redis::submit::<i64>(
            self,
            Redis::frame(self, |out| encode::cmd_pfadd(out, key, elements)),
        );
        dope_fiber::fiber!('d => async move { Ok(fut.await? != 0) })
    }

    fn pf_count(self, keys: &[&[u8]]) -> impl Fiber<'d, Output = Result<u64, Error>> {
        let fut = (!keys.is_empty())
            .then(|| Redis::dispatch::<u64>(self, move |out| encode::cmd_pfcount(out, keys)));
        dope_fiber::fiber!('d => async move {
            match fut {
                Some(f) => f.await,
                None => Ok(0),
            }
        })
    }

    fn pf_merge(
        self,
        dest: &[u8],
        sources: &[&[u8]],
    ) -> impl Fiber<'d, Output = Result<(), Error>> {
        Redis::submit::<()>(
            self,
            Redis::frame(self, |out| encode::cmd_pfmerge(out, dest, sources)),
        )
    }

    fn bit_count(self, key: &[u8]) -> impl Fiber<'d, Output = Result<u64, Error>> {
        Redis::dispatch::<u64>(self, move |out| encode::cmd_raw(out, &[b"BITCOUNT", key]))
    }

    fn bit_count_range(
        self,
        key: &[u8],
        start: i64,
        end: i64,
    ) -> impl Fiber<'d, Output = Result<u64, Error>> {
        Redis::dispatch::<u64>(self, move |out| {
            encode::cmd_bit_count_range(out, key, start, end)
        })
    }

    fn set_bit(
        self,
        key: &[u8],
        offset: u64,
        value: bool,
    ) -> impl Fiber<'d, Output = Result<bool, Error>> {
        let fut = Redis::dispatch::<i64>(self, move |out| {
            encode::cmd_set_bit(out, key, offset, value)
        });
        dope_fiber::fiber!('d => async move { Ok(fut.await? != 0) })
    }

    fn get_bit(self, key: &[u8], offset: u64) -> impl Fiber<'d, Output = Result<bool, Error>> {
        let fut = Redis::dispatch::<i64>(self, move |out| encode::cmd_get_bit(out, key, offset));
        dope_fiber::fiber!('d => async move { Ok(fut.await? != 0) })
    }

    fn bit_op(
        self,
        op: &[u8],
        dest: &[u8],
        sources: &[&[u8]],
    ) -> impl Fiber<'d, Output = Result<u64, Error>> {
        Redis::submit::<u64>(
            self,
            Redis::frame(self, |out| encode::cmd_bitop(out, op, dest, sources)),
        )
    }

    fn bit_pos(self, key: &[u8], bit: bool) -> impl Fiber<'d, Output = Result<i64, Error>> {
        let b: &[u8] = if bit { b"1" } else { b"0" };
        Redis::dispatch::<i64>(self, move |out| encode::cmd_raw(out, &[b"BITPOS", key, b]))
    }

    fn geo_add(
        self,
        key: &[u8],
        longitude: f64,
        latitude: f64,
        member: &[u8],
    ) -> impl Fiber<'d, Output = Result<u64, Error>> {
        Redis::dispatch::<u64>(self, move |out| {
            encode::cmd_geo_add(out, key, longitude, latitude, member)
        })
    }

    fn geo_dist(
        self,
        key: &[u8],
        member1: &[u8],
        member2: &[u8],
        unit: Option<&[u8]>,
    ) -> impl Fiber<'d, Output = Result<Option<f64>, Error>> {
        let fut = Redis::submit::<Option<Shared>>(
            self,
            Redis::frame(self, |out| {
                encode::cmd_geodist(out, key, member1, member2, unit)
            }),
        );
        dope_fiber::fiber!('d => async move {
            match fut.await? {
                None => Ok(None),
                Some(b) => std::str::from_utf8(b.as_slice())
                    .ok()
                    .and_then(|s| s.parse::<f64>().ok())
                    .map(Some)
                    .ok_or_else(|| Error::Redis("GEODIST: invalid float".into())),
            }
        })
    }

    fn geo_pos(
        self,
        key: &[u8],
        members: &[&[u8]],
    ) -> impl Fiber<'d, Output = Result<Vec<Option<(f64, f64)>>, Error>> {
        let fut = Redis::submit::<crate::value::Value>(
            self,
            Redis::frame(self, |out| encode::cmd_geopos(out, key, members)),
        );
        dope_fiber::fiber!('d => async move {
            let arr = fut.await?.into_array()?;
            let mut out = Vec::with_capacity(arr.len());
            for entry in arr {
                match entry {
                    crate::value::Value::Nil => out.push(None),
                    crate::value::Value::Array(items) if items.len() == 2 => {
                        let mut it = items.into_iter();
                        let parse_f = |v: crate::value::Value| -> Result<f64, Error> {
                            let b = v.into_bulk()?;
                            std::str::from_utf8(b.as_slice())
                                .ok()
                                .and_then(|s| s.parse::<f64>().ok())
                                .ok_or_else(|| Error::Redis("GEOPOS: invalid float".into()))
                        };
                        let lng = parse_f(it.next().unwrap())?;
                        let lat = parse_f(it.next().unwrap())?;
                        out.push(Some((lng, lat)));
                    }
                    _ => return Err(Error::Redis("GEOPOS: unexpected entry shape".into())),
                }
            }
            Ok(out)
        })
    }

    fn geo_search_radius(
        self,
        key: &[u8],
        longitude: f64,
        latitude: f64,
        radius: f64,
        unit: &[u8],
    ) -> impl Fiber<'d, Output = Result<Vec<Shared>, Error>> {
        Redis::dispatch::<Vec<Shared>>(self, move |out| {
            encode::cmd_geo_search_radius(out, key, longitude, latitude, radius, unit)
        })
    }

    fn info(self, section: Option<&[u8]>) -> impl Fiber<'d, Output = Result<Shared, Error>> {
        Redis::dispatch::<Shared>(self, move |out| match section {
            None => encode::cmd_raw(out, &[b"INFO"]),
            Some(s) => encode::cmd_raw(out, &[b"INFO", s]),
        })
    }

    fn client_get_name(self) -> impl Fiber<'d, Output = Result<Option<Shared>, Error>> {
        Redis::dispatch::<Option<Shared>>(self, |out| {
            encode::cmd_raw(out, &[b"CLIENT", b"GETNAME"])
        })
    }

    fn client_set_name(self, name: &[u8]) -> impl Fiber<'d, Output = Result<(), Error>> {
        Redis::dispatch::<()>(self, move |out| {
            encode::cmd_raw(out, &[b"CLIENT", b"SETNAME", name])
        })
    }

    fn client_id(self) -> impl Fiber<'d, Output = Result<i64, Error>> {
        Redis::dispatch::<i64>(self, |out| encode::cmd_raw(out, &[b"CLIENT", b"ID"]))
    }

    fn client_list(self) -> impl Fiber<'d, Output = Result<Shared, Error>> {
        Redis::dispatch::<Shared>(self, |out| encode::cmd_raw(out, &[b"CLIENT", b"LIST"]))
    }

    fn client_kill_addr(self, addr: &[u8]) -> impl Fiber<'d, Output = Result<u64, Error>> {
        Redis::dispatch::<u64>(self, move |out| {
            encode::cmd_raw(out, &[b"CLIENT", b"KILL", b"ADDR", addr])
        })
    }

    fn config_get(
        self,
        parameter: &[u8],
    ) -> impl Fiber<'d, Output = Result<Vec<(Shared, Shared)>, Error>> {
        Redis::dispatch::<Vec<(Shared, Shared)>>(self, move |out| {
            encode::cmd_raw(out, &[b"CONFIG", b"GET", parameter])
        })
    }

    fn config_set(
        self,
        parameter: &[u8],
        value: &[u8],
    ) -> impl Fiber<'d, Output = Result<(), Error>> {
        Redis::dispatch::<()>(self, move |out| {
            encode::cmd_raw(out, &[b"CONFIG", b"SET", parameter, value])
        })
    }

    fn config_reset_stat(self) -> impl Fiber<'d, Output = Result<(), Error>> {
        Redis::dispatch::<()>(self, |out| encode::cmd_raw(out, &[b"CONFIG", b"RESETSTAT"]))
    }

    fn debug_object(self, key: &[u8]) -> impl Fiber<'d, Output = Result<Shared, Error>> {
        let fut = Redis::dispatch::<crate::value::Value>(self, move |out| {
            encode::cmd_raw(out, &[b"DEBUG", b"OBJECT", key])
        });
        dope_fiber::fiber!('d => async move {
            match fut.await? {
                crate::value::Value::Status(b) => Ok(b),
                crate::value::Value::Bulk(b) => Ok(b),
                _ => Err(Error::Redis("DEBUG OBJECT: unexpected value".into())),
            }
        })
    }

    fn auth(self, password: &[u8]) -> impl Fiber<'d, Output = Result<(), Error>> {
        Redis::dispatch::<()>(self, move |out| encode::cmd_raw(out, &[b"AUTH", password]))
    }

    fn auth_user(
        self,
        username: &[u8],
        password: &[u8],
    ) -> impl Fiber<'d, Output = Result<(), Error>> {
        Redis::dispatch::<()>(self, move |out| {
            encode::cmd_raw(out, &[b"AUTH", username, password])
        })
    }

    fn select_db(self, db: u32) -> impl Fiber<'d, Output = Result<(), Error>> {
        Redis::dispatch::<()>(self, move |out| encode::cmd_select(out, db))
    }

    fn hello(
        self,
        protocol: Option<u8>,
    ) -> impl Fiber<'d, Output = Result<crate::value::Value, Error>> {
        Redis::dispatch::<crate::value::Value>(self, move |out| encode::cmd_hello(out, protocol))
    }
}

impl Redis<'_> {
    fn frame<'d>(
        redis: Redis<'d>,
        encode: impl FnOnce(&mut crate::port::Frame<'_>),
    ) -> Result<crate::port::Frame<'d>, Error> {
        redis.port.encode(encode).map(crate::port::Frame::cast)
    }

    fn dispatch<'d, R>(
        redis: Redis<'d>,
        encode: impl FnOnce(&mut crate::port::Frame<'_>),
    ) -> impl Fiber<'d, Output = Result<R, Error>>
    where
        R: FromValue,
    {
        Self::submit::<R>(redis, Self::frame(redis, encode))
    }

    fn submit<'d, R>(
        redis: Redis<'d>,
        frame: Result<crate::port::Frame<'d>, Error>,
    ) -> impl Fiber<'d, Output = Result<R, Error>>
    where
        R: FromValue,
    {
        dope_fiber::fiber!('d => async move {
            let reply = Self::enqueue(redis, frame?)?;
            let value = reply.await;
            value.and_then(R::from_value)
        })
    }

    fn enqueue<'d>(
        redis: Redis<'d>,
        frame: crate::port::Frame<'d>,
    ) -> Result<Reply<'d, Outcome, ExtractValue>, Error> {
        let mut reply = Reply::new();
        redis
            .port
            .try_enqueue_reply(frame, &mut reply)
            .map_err(|(error, _)| error)?;
        Ok(reply)
    }
}

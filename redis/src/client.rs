use std::future::{Future, poll_fn};
use std::pin::Pin;
use std::task::Poll;
use std::time::Duration;

use cartel_core::{Extract, Registrable, Reply, Slot};
use dope::WakeRef;
use dope::fiber::{Fiber, Holding};
use dope::manifold::connector::Connector;
use dope::manifold::connector::source::Dialer;
use dope::manifold::env::Env;
use dope::transport::Transport;
use o3::buffer::Shared;

use crate::protocol::{Outcome, Session};
use crate::value::FromValue;
use crate::{Error, encode};

pub type GeoCoord = (f64, f64);

pub const DEFAULT_BACKOFF: Duration = Duration::from_millis(500);

struct ExtractValue;

impl Extract<Outcome> for ExtractValue {
    type Output = Outcome;

    fn extract(slot: &mut Slot<Outcome>) -> Option<Self::Output> {
        if !slot.completed() {
            return None;
        }
        Some(
            slot.pop()
                .unwrap_or_else(|| Err(Error::Redis("redis reply slot empty".into()))),
        )
    }
}

pub trait Ops<'d, S, E>
where
    S: Dialer<E::Transport> + 'd,
    E: Env + 'd,
    E::Transport: Transport<Addr: Clone>,
{
    fn wait_active<'a>(&'a self) -> Fiber<'d, impl Future<Output = Result<(), Error>> + 'a>;

    fn get(&self, key: &[u8]) -> Fiber<'d, impl Future<Output = Result<Option<Shared>, Error>>>;
    fn set(&self, key: &[u8], value: &[u8]) -> Fiber<'d, impl Future<Output = Result<(), Error>>>;
    fn set_ex(
        &self,
        key: &[u8],
        value: &[u8],
        ttl: Duration,
    ) -> Fiber<'d, impl Future<Output = Result<(), Error>>>;
    fn set_px(
        &self,
        key: &[u8],
        value: &[u8],
        ttl: Duration,
    ) -> Fiber<'d, impl Future<Output = Result<(), Error>>>;
    fn set_nx(
        &self,
        key: &[u8],
        value: &[u8],
    ) -> Fiber<'d, impl Future<Output = Result<bool, Error>>>;
    fn del(&self, keys: &[&[u8]]) -> Fiber<'d, impl Future<Output = Result<u64, Error>>>;
    fn exists(&self, keys: &[&[u8]]) -> Fiber<'d, impl Future<Output = Result<u64, Error>>>;
    fn incr(&self, key: &[u8]) -> Fiber<'d, impl Future<Output = Result<i64, Error>>>;
    fn decr(&self, key: &[u8]) -> Fiber<'d, impl Future<Output = Result<i64, Error>>>;
    fn incrby(&self, key: &[u8], by: i64) -> Fiber<'d, impl Future<Output = Result<i64, Error>>>;
    fn expire(
        &self,
        key: &[u8],
        ttl: Duration,
    ) -> Fiber<'d, impl Future<Output = Result<bool, Error>>>;
    fn ttl(&self, key: &[u8]) -> Fiber<'d, impl Future<Output = Result<i64, Error>>>;
    fn mget(
        &self,
        keys: &[&[u8]],
    ) -> Fiber<'d, impl Future<Output = Result<Vec<Option<Shared>>, Error>>>;
    fn mset(&self, kv: &[(&[u8], &[u8])]) -> Fiber<'d, impl Future<Output = Result<(), Error>>>;
    fn ping(&self) -> Fiber<'d, impl Future<Output = Result<(), Error>>>;
    fn cmd<R: FromValue>(
        &self,
        args: &[&[u8]],
    ) -> Fiber<'d, impl Future<Output = Result<R, Error>>>;
    fn publish(
        &self,
        channel: &[u8],
        message: &[u8],
    ) -> Fiber<'d, impl Future<Output = Result<i64, Error>>>;
    fn hget(
        &self,
        key: &[u8],
        field: &[u8],
    ) -> Fiber<'d, impl Future<Output = Result<Option<Shared>, Error>>>;
    fn hset(
        &self,
        key: &[u8],
        field: &[u8],
        value: &[u8],
    ) -> Fiber<'d, impl Future<Output = Result<bool, Error>>>;
    fn hset_multi(
        &self,
        key: &[u8],
        fv: &[(&[u8], &[u8])],
    ) -> Fiber<'d, impl Future<Output = Result<u64, Error>>>;
    fn hmget(
        &self,
        key: &[u8],
        fields: &[&[u8]],
    ) -> Fiber<'d, impl Future<Output = Result<Vec<Option<Shared>>, Error>>>;
    fn hdel(
        &self,
        key: &[u8],
        fields: &[&[u8]],
    ) -> Fiber<'d, impl Future<Output = Result<u64, Error>>>;
    fn hgetall(
        &self,
        key: &[u8],
    ) -> Fiber<'d, impl Future<Output = Result<Vec<(Shared, Shared)>, Error>>>;
    fn hlen(&self, key: &[u8]) -> Fiber<'d, impl Future<Output = Result<u64, Error>>>;
    fn hexists(
        &self,
        key: &[u8],
        field: &[u8],
    ) -> Fiber<'d, impl Future<Output = Result<bool, Error>>>;
    fn hincrby(
        &self,
        key: &[u8],
        field: &[u8],
        by: i64,
    ) -> Fiber<'d, impl Future<Output = Result<i64, Error>>>;
    fn sadd(
        &self,
        key: &[u8],
        members: &[&[u8]],
    ) -> Fiber<'d, impl Future<Output = Result<u64, Error>>>;
    fn srem(
        &self,
        key: &[u8],
        members: &[&[u8]],
    ) -> Fiber<'d, impl Future<Output = Result<u64, Error>>>;
    fn smembers(&self, key: &[u8]) -> Fiber<'d, impl Future<Output = Result<Vec<Shared>, Error>>>;
    fn sismember(
        &self,
        key: &[u8],
        member: &[u8],
    ) -> Fiber<'d, impl Future<Output = Result<bool, Error>>>;
    fn scard(&self, key: &[u8]) -> Fiber<'d, impl Future<Output = Result<u64, Error>>>;
    fn zadd(
        &self,
        key: &[u8],
        score: f64,
        member: &[u8],
    ) -> Fiber<'d, impl Future<Output = Result<u64, Error>>>;
    fn zrem(
        &self,
        key: &[u8],
        members: &[&[u8]],
    ) -> Fiber<'d, impl Future<Output = Result<u64, Error>>>;
    fn zrange(
        &self,
        key: &[u8],
        start: i64,
        stop: i64,
    ) -> Fiber<'d, impl Future<Output = Result<Vec<Shared>, Error>>>;
    fn zrange_with_scores(
        &self,
        key: &[u8],
        start: i64,
        stop: i64,
    ) -> Fiber<'d, impl Future<Output = Result<Vec<(Shared, f64)>, Error>>>;
    fn zrangebyscore(
        &self,
        key: &[u8],
        min: f64,
        max: f64,
    ) -> Fiber<'d, impl Future<Output = Result<Vec<Shared>, Error>>>;
    fn zrank(
        &self,
        key: &[u8],
        member: &[u8],
    ) -> Fiber<'d, impl Future<Output = Result<Option<u64>, Error>>>;
    fn zscore(
        &self,
        key: &[u8],
        member: &[u8],
    ) -> Fiber<'d, impl Future<Output = Result<Option<f64>, Error>>>;
    fn zcard(&self, key: &[u8]) -> Fiber<'d, impl Future<Output = Result<u64, Error>>>;
    fn zincrby(
        &self,
        key: &[u8],
        by: f64,
        member: &[u8],
    ) -> Fiber<'d, impl Future<Output = Result<f64, Error>>>;
    fn lpush(
        &self,
        key: &[u8],
        values: &[&[u8]],
    ) -> Fiber<'d, impl Future<Output = Result<u64, Error>>>;
    fn rpush(
        &self,
        key: &[u8],
        values: &[&[u8]],
    ) -> Fiber<'d, impl Future<Output = Result<u64, Error>>>;
    fn lpop(&self, key: &[u8]) -> Fiber<'d, impl Future<Output = Result<Option<Shared>, Error>>>;
    fn rpop(&self, key: &[u8]) -> Fiber<'d, impl Future<Output = Result<Option<Shared>, Error>>>;
    fn lrange(
        &self,
        key: &[u8],
        start: i64,
        stop: i64,
    ) -> Fiber<'d, impl Future<Output = Result<Vec<Shared>, Error>>>;
    fn llen(&self, key: &[u8]) -> Fiber<'d, impl Future<Output = Result<u64, Error>>>;
    fn getset(
        &self,
        key: &[u8],
        value: &[u8],
    ) -> Fiber<'d, impl Future<Output = Result<Option<Shared>, Error>>>;
    fn getdel(&self, key: &[u8]) -> Fiber<'d, impl Future<Output = Result<Option<Shared>, Error>>>;
    fn append(
        &self,
        key: &[u8],
        value: &[u8],
    ) -> Fiber<'d, impl Future<Output = Result<u64, Error>>>;
    fn strlen(&self, key: &[u8]) -> Fiber<'d, impl Future<Output = Result<u64, Error>>>;
    fn incrbyfloat(
        &self,
        key: &[u8],
        by: f64,
    ) -> Fiber<'d, impl Future<Output = Result<f64, Error>>>;
    fn key_type(&self, key: &[u8]) -> Fiber<'d, impl Future<Output = Result<Shared, Error>>>;
    fn rename(&self, src: &[u8], dst: &[u8]) -> Fiber<'d, impl Future<Output = Result<(), Error>>>;
    fn persist(&self, key: &[u8]) -> Fiber<'d, impl Future<Output = Result<bool, Error>>>;
    fn unlink(&self, keys: &[&[u8]]) -> Fiber<'d, impl Future<Output = Result<u64, Error>>>;
    fn dbsize(&self) -> Fiber<'d, impl Future<Output = Result<u64, Error>>>;
    fn scan(
        &self,
        cursor: u64,
        match_pattern: Option<&[u8]>,
        count: Option<u64>,
    ) -> Fiber<'d, impl Future<Output = Result<(u64, Vec<Shared>), Error>>>;
    fn pfadd(
        &self,
        key: &[u8],
        elements: &[&[u8]],
    ) -> Fiber<'d, impl Future<Output = Result<bool, Error>>>;
    fn pfcount(&self, keys: &[&[u8]]) -> Fiber<'d, impl Future<Output = Result<u64, Error>>>;
    fn pfmerge(
        &self,
        dest: &[u8],
        sources: &[&[u8]],
    ) -> Fiber<'d, impl Future<Output = Result<(), Error>>>;
    fn bitcount(&self, key: &[u8]) -> Fiber<'d, impl Future<Output = Result<u64, Error>>>;
    fn bitcount_range(
        &self,
        key: &[u8],
        start: i64,
        end: i64,
    ) -> Fiber<'d, impl Future<Output = Result<u64, Error>>>;
    fn setbit(
        &self,
        key: &[u8],
        offset: u64,
        value: bool,
    ) -> Fiber<'d, impl Future<Output = Result<bool, Error>>>;
    fn getbit(
        &self,
        key: &[u8],
        offset: u64,
    ) -> Fiber<'d, impl Future<Output = Result<bool, Error>>>;
    fn bitop(
        &self,
        op: &[u8],
        dest: &[u8],
        sources: &[&[u8]],
    ) -> Fiber<'d, impl Future<Output = Result<u64, Error>>>;
    fn bitpos(&self, key: &[u8], bit: bool) -> Fiber<'d, impl Future<Output = Result<i64, Error>>>;
    fn geoadd(
        &self,
        key: &[u8],
        longitude: f64,
        latitude: f64,
        member: &[u8],
    ) -> Fiber<'d, impl Future<Output = Result<u64, Error>>>;
    fn geodist(
        &self,
        key: &[u8],
        member1: &[u8],
        member2: &[u8],
        unit: Option<&[u8]>,
    ) -> Fiber<'d, impl Future<Output = Result<Option<f64>, Error>>>;
    fn geopos(
        &self,
        key: &[u8],
        members: &[&[u8]],
    ) -> Fiber<'d, impl Future<Output = Result<Vec<Option<GeoCoord>>, Error>>>;
    fn geosearch_radius(
        &self,
        key: &[u8],
        longitude: f64,
        latitude: f64,
        radius: f64,
        unit: &[u8],
    ) -> Fiber<'d, impl Future<Output = Result<Vec<Shared>, Error>>>;
    fn info(
        &self,
        section: Option<&[u8]>,
    ) -> Fiber<'d, impl Future<Output = Result<Shared, Error>>>;
    fn client_getname(&self) -> Fiber<'d, impl Future<Output = Result<Option<Shared>, Error>>>;
    fn client_setname(&self, name: &[u8]) -> Fiber<'d, impl Future<Output = Result<(), Error>>>;
    fn client_id(&self) -> Fiber<'d, impl Future<Output = Result<i64, Error>>>;
    fn client_list(&self) -> Fiber<'d, impl Future<Output = Result<Shared, Error>>>;
    fn client_kill_addr(&self, addr: &[u8]) -> Fiber<'d, impl Future<Output = Result<u64, Error>>>;
    fn config_get(
        &self,
        parameter: &[u8],
    ) -> Fiber<'d, impl Future<Output = Result<Vec<(Shared, Shared)>, Error>>>;
    fn config_set(
        &self,
        parameter: &[u8],
        value: &[u8],
    ) -> Fiber<'d, impl Future<Output = Result<(), Error>>>;
    fn config_resetstat(&self) -> Fiber<'d, impl Future<Output = Result<(), Error>>>;
    fn debug_object(&self, key: &[u8]) -> Fiber<'d, impl Future<Output = Result<Shared, Error>>>;
    fn auth(&self, password: &[u8]) -> Fiber<'d, impl Future<Output = Result<(), Error>>>;
    fn auth_user(
        &self,
        username: &[u8],
        password: &[u8],
    ) -> Fiber<'d, impl Future<Output = Result<(), Error>>>;
    fn select_db(&self, db: u32) -> Fiber<'d, impl Future<Output = Result<(), Error>>>;
    fn hello(
        &self,
        protocol: Option<u8>,
    ) -> Fiber<'d, impl Future<Output = Result<crate::value::Value, Error>>>;
}

impl<'d, const ID: u8, S, E> Ops<'d, S, E> for Holding<'d, Connector<ID, Session, S, E>>
where
    S: Dialer<E::Transport> + 'd,
    E: Env + 'd,
    E::Transport: Transport<Addr: Clone>,
{
    fn wait_active<'a>(&'a self) -> Fiber<'d, impl Future<Output = Result<(), Error>> + 'a> {
        let inner = *self;
        Fiber::new(poll_fn(move |cx| {
            let mut h = inner.hold();
            let shared = &mut h.as_mut().session_mut().shared;
            if shared.conn_id.is_some() {
                return Poll::Ready(Ok(()));
            }
            if let Some(e) = shared.fatal.as_ref() {
                return Poll::Ready(Err(Error::Redis(e.to_string())));
            }
            // SAFETY: cx.waker() was minted by the dope dispatcher's Slot::make_waker.
            shared.active_wakers.register(WakeRef::verified(cx.waker()));
            Poll::Pending
        }))
    }

    fn get(&self, key: &[u8]) -> Fiber<'d, impl Future<Output = Result<Option<Shared>, Error>>> {
        Session::dispatch::<ID, S, E, Option<Shared>>(*self, move |out| encode::cmd_get(out, key))
    }

    fn set(&self, key: &[u8], value: &[u8]) -> Fiber<'d, impl Future<Output = Result<(), Error>>> {
        Session::dispatch::<ID, S, E, ()>(*self, move |out| encode::cmd_set(out, key, value))
    }

    fn set_ex(
        &self,
        key: &[u8],
        value: &[u8],
        ttl: Duration,
    ) -> Fiber<'d, impl Future<Output = Result<(), Error>>> {
        let secs = ttl.as_secs().max(1);
        Session::dispatch::<ID, S, E, ()>(*self, move |out| {
            encode::cmd_set_ex(out, key, value, secs)
        })
    }

    fn set_px(
        &self,
        key: &[u8],
        value: &[u8],
        ttl: Duration,
    ) -> Fiber<'d, impl Future<Output = Result<(), Error>>> {
        let ms = u64::try_from(ttl.as_millis()).unwrap_or(u64::MAX).max(1);
        Session::dispatch::<ID, S, E, ()>(*self, move |out| encode::cmd_set_px(out, key, value, ms))
    }

    fn set_nx(
        &self,
        key: &[u8],
        value: &[u8],
    ) -> Fiber<'d, impl Future<Output = Result<bool, Error>>> {
        Session::dispatch::<ID, S, E, bool>(*self, move |out| encode::cmd_set_nx(out, key, value))
    }

    fn del(&self, keys: &[&[u8]]) -> Fiber<'d, impl Future<Output = Result<u64, Error>>> {
        let fut = (!keys.is_empty()).then(|| {
            Session::dispatch::<ID, S, E, u64>(*self, move |out| encode::cmd_del(out, keys))
        });
        Fiber::new(async move {
            match fut {
                Some(f) => f.await,
                None => Ok(0),
            }
        })
    }

    fn exists(&self, keys: &[&[u8]]) -> Fiber<'d, impl Future<Output = Result<u64, Error>>> {
        let fut = (!keys.is_empty()).then(|| {
            Session::dispatch::<ID, S, E, u64>(*self, move |out| encode::cmd_exists(out, keys))
        });
        Fiber::new(async move {
            match fut {
                Some(f) => f.await,
                None => Ok(0),
            }
        })
    }

    fn incr(&self, key: &[u8]) -> Fiber<'d, impl Future<Output = Result<i64, Error>>> {
        Session::dispatch::<ID, S, E, i64>(*self, move |out| encode::cmd_incr(out, key))
    }

    fn decr(&self, key: &[u8]) -> Fiber<'d, impl Future<Output = Result<i64, Error>>> {
        Session::dispatch::<ID, S, E, i64>(*self, move |out| encode::cmd_decr(out, key))
    }

    fn incrby(&self, key: &[u8], by: i64) -> Fiber<'d, impl Future<Output = Result<i64, Error>>> {
        Session::dispatch::<ID, S, E, i64>(*self, move |out| encode::cmd_incrby(out, key, by))
    }

    fn expire(
        &self,
        key: &[u8],
        ttl: Duration,
    ) -> Fiber<'d, impl Future<Output = Result<bool, Error>>> {
        let secs = ttl.as_secs().max(1);
        Session::dispatch::<ID, S, E, bool>(*self, move |out| encode::cmd_expire(out, key, secs))
    }

    fn ttl(&self, key: &[u8]) -> Fiber<'d, impl Future<Output = Result<i64, Error>>> {
        Session::dispatch::<ID, S, E, i64>(*self, move |out| encode::cmd_ttl(out, key))
    }

    fn mget(
        &self,
        keys: &[&[u8]],
    ) -> Fiber<'d, impl Future<Output = Result<Vec<Option<Shared>>, Error>>> {
        let fut = (!keys.is_empty()).then(|| {
            Session::dispatch::<ID, S, E, Vec<Option<Shared>>>(*self, move |out| {
                encode::cmd_mget(out, keys)
            })
        });
        Fiber::new(async move {
            match fut {
                Some(f) => f.await,
                None => Ok(Vec::new()),
            }
        })
    }

    fn mset(&self, kv: &[(&[u8], &[u8])]) -> Fiber<'d, impl Future<Output = Result<(), Error>>> {
        let fut = (!kv.is_empty()).then(|| {
            Session::dispatch::<ID, S, E, ()>(*self, move |out| encode::cmd_mset(out, kv))
        });
        Fiber::new(async move {
            match fut {
                Some(f) => f.await,
                None => Ok(()),
            }
        })
    }

    fn ping(&self) -> Fiber<'d, impl Future<Output = Result<(), Error>>> {
        Session::dispatch::<ID, S, E, ()>(*self, encode::cmd_ping)
    }

    fn cmd<R: FromValue>(
        &self,
        args: &[&[u8]],
    ) -> Fiber<'d, impl Future<Output = Result<R, Error>>> {
        Session::dispatch::<ID, S, E, R>(*self, move |out| encode::cmd_raw(out, args))
    }

    fn publish(
        &self,
        channel: &[u8],
        message: &[u8],
    ) -> Fiber<'d, impl Future<Output = Result<i64, Error>>> {
        Session::dispatch::<ID, S, E, i64>(*self, move |out| {
            encode::cmd_raw(out, &[b"PUBLISH", channel, message])
        })
    }

    fn hget(
        &self,
        key: &[u8],
        field: &[u8],
    ) -> Fiber<'d, impl Future<Output = Result<Option<Shared>, Error>>> {
        Session::dispatch::<ID, S, E, Option<Shared>>(*self, move |out| {
            encode::cmd_hget(out, key, field)
        })
    }

    fn hset(
        &self,
        key: &[u8],
        field: &[u8],
        value: &[u8],
    ) -> Fiber<'d, impl Future<Output = Result<bool, Error>>> {
        let fut = Session::dispatch::<ID, S, E, i64>(*self, move |out| {
            encode::cmd_hset_pairs(out, key, &[(field, value)])
        });
        Fiber::new(async move { Ok(fut.await? != 0) })
    }

    fn hset_multi(
        &self,
        key: &[u8],
        fv: &[(&[u8], &[u8])],
    ) -> Fiber<'d, impl Future<Output = Result<u64, Error>>> {
        let fut = (!fv.is_empty()).then(|| {
            Session::dispatch::<ID, S, E, u64>(*self, move |out| {
                encode::cmd_hset_pairs(out, key, fv)
            })
        });
        Fiber::new(async move {
            match fut {
                Some(f) => f.await,
                None => Ok(0),
            }
        })
    }

    fn hmget(
        &self,
        key: &[u8],
        fields: &[&[u8]],
    ) -> Fiber<'d, impl Future<Output = Result<Vec<Option<Shared>>, Error>>> {
        let fut = (!fields.is_empty()).then(|| {
            Session::dispatch::<ID, S, E, Vec<Option<Shared>>>(*self, move |out| {
                encode::cmd_hmget(out, key, fields)
            })
        });
        Fiber::new(async move {
            match fut {
                Some(f) => f.await,
                None => Ok(Vec::new()),
            }
        })
    }

    fn hdel(
        &self,
        key: &[u8],
        fields: &[&[u8]],
    ) -> Fiber<'d, impl Future<Output = Result<u64, Error>>> {
        let fut = (!fields.is_empty()).then(|| {
            Session::dispatch::<ID, S, E, u64>(*self, move |out| encode::cmd_hdel(out, key, fields))
        });
        Fiber::new(async move {
            match fut {
                Some(f) => f.await,
                None => Ok(0),
            }
        })
    }

    fn hgetall(
        &self,
        key: &[u8],
    ) -> Fiber<'d, impl Future<Output = Result<Vec<(Shared, Shared)>, Error>>> {
        Session::dispatch::<ID, S, E, Vec<(Shared, Shared)>>(*self, move |out| {
            encode::cmd_hgetall(out, key)
        })
    }

    fn hlen(&self, key: &[u8]) -> Fiber<'d, impl Future<Output = Result<u64, Error>>> {
        Session::dispatch::<ID, S, E, u64>(*self, move |out| encode::cmd_hlen(out, key))
    }

    fn hexists(
        &self,
        key: &[u8],
        field: &[u8],
    ) -> Fiber<'d, impl Future<Output = Result<bool, Error>>> {
        Session::dispatch::<ID, S, E, bool>(*self, move |out| encode::cmd_hexists(out, key, field))
    }

    fn hincrby(
        &self,
        key: &[u8],
        field: &[u8],
        by: i64,
    ) -> Fiber<'d, impl Future<Output = Result<i64, Error>>> {
        Session::dispatch::<ID, S, E, i64>(*self, move |out| {
            encode::cmd_hincrby(out, key, field, by)
        })
    }

    fn sadd(
        &self,
        key: &[u8],
        members: &[&[u8]],
    ) -> Fiber<'d, impl Future<Output = Result<u64, Error>>> {
        let fut = (!members.is_empty()).then(|| {
            Session::dispatch::<ID, S, E, u64>(*self, move |out| {
                encode::cmd_sadd(out, key, members)
            })
        });
        Fiber::new(async move {
            match fut {
                Some(f) => f.await,
                None => Ok(0),
            }
        })
    }

    fn srem(
        &self,
        key: &[u8],
        members: &[&[u8]],
    ) -> Fiber<'d, impl Future<Output = Result<u64, Error>>> {
        let fut = (!members.is_empty()).then(|| {
            Session::dispatch::<ID, S, E, u64>(*self, move |out| {
                encode::cmd_srem(out, key, members)
            })
        });
        Fiber::new(async move {
            match fut {
                Some(f) => f.await,
                None => Ok(0),
            }
        })
    }

    fn smembers(&self, key: &[u8]) -> Fiber<'d, impl Future<Output = Result<Vec<Shared>, Error>>> {
        Session::dispatch::<ID, S, E, Vec<Shared>>(*self, move |out| encode::cmd_smembers(out, key))
    }

    fn sismember(
        &self,
        key: &[u8],
        member: &[u8],
    ) -> Fiber<'d, impl Future<Output = Result<bool, Error>>> {
        Session::dispatch::<ID, S, E, bool>(*self, move |out| {
            encode::cmd_sismember(out, key, member)
        })
    }

    fn scard(&self, key: &[u8]) -> Fiber<'d, impl Future<Output = Result<u64, Error>>> {
        Session::dispatch::<ID, S, E, u64>(*self, move |out| encode::cmd_scard(out, key))
    }

    fn zadd(
        &self,
        key: &[u8],
        score: f64,
        member: &[u8],
    ) -> Fiber<'d, impl Future<Output = Result<u64, Error>>> {
        Session::dispatch::<ID, S, E, u64>(*self, move |out| {
            encode::cmd_zadd(out, key, score, member)
        })
    }

    fn zrem(
        &self,
        key: &[u8],
        members: &[&[u8]],
    ) -> Fiber<'d, impl Future<Output = Result<u64, Error>>> {
        let fut = (!members.is_empty()).then(|| {
            Session::dispatch::<ID, S, E, u64>(*self, move |out| {
                encode::cmd_zrem(out, key, members)
            })
        });
        Fiber::new(async move {
            match fut {
                Some(f) => f.await,
                None => Ok(0),
            }
        })
    }

    fn zrange(
        &self,
        key: &[u8],
        start: i64,
        stop: i64,
    ) -> Fiber<'d, impl Future<Output = Result<Vec<Shared>, Error>>> {
        Session::dispatch::<ID, S, E, Vec<Shared>>(*self, move |out| {
            encode::cmd_zrange(out, key, start, stop)
        })
    }

    fn zrange_with_scores(
        &self,
        key: &[u8],
        start: i64,
        stop: i64,
    ) -> Fiber<'d, impl Future<Output = Result<Vec<(Shared, f64)>, Error>>> {
        Session::dispatch::<ID, S, E, Vec<(Shared, f64)>>(*self, move |out| {
            encode::cmd_zrange_with_scores(out, key, start, stop)
        })
    }

    fn zrangebyscore(
        &self,
        key: &[u8],
        min: f64,
        max: f64,
    ) -> Fiber<'d, impl Future<Output = Result<Vec<Shared>, Error>>> {
        Session::dispatch::<ID, S, E, Vec<Shared>>(*self, move |out| {
            encode::cmd_zrangebyscore(out, key, min, max)
        })
    }

    fn zrank(
        &self,
        key: &[u8],
        member: &[u8],
    ) -> Fiber<'d, impl Future<Output = Result<Option<u64>, Error>>> {
        Session::dispatch::<ID, S, E, Option<u64>>(*self, move |out| {
            encode::cmd_zrank(out, key, member)
        })
    }

    fn zscore(
        &self,
        key: &[u8],
        member: &[u8],
    ) -> Fiber<'d, impl Future<Output = Result<Option<f64>, Error>>> {
        Session::dispatch::<ID, S, E, Option<f64>>(*self, move |out| {
            encode::cmd_zscore(out, key, member)
        })
    }

    fn zcard(&self, key: &[u8]) -> Fiber<'d, impl Future<Output = Result<u64, Error>>> {
        Session::dispatch::<ID, S, E, u64>(*self, move |out| encode::cmd_zcard(out, key))
    }

    fn zincrby(
        &self,
        key: &[u8],
        by: f64,
        member: &[u8],
    ) -> Fiber<'d, impl Future<Output = Result<f64, Error>>> {
        Session::dispatch::<ID, S, E, f64>(*self, move |out| {
            encode::cmd_zincrby(out, key, by, member)
        })
    }

    fn lpush(
        &self,
        key: &[u8],
        values: &[&[u8]],
    ) -> Fiber<'d, impl Future<Output = Result<u64, Error>>> {
        let fut = (!values.is_empty()).then(|| {
            Session::dispatch::<ID, S, E, u64>(*self, move |out| {
                encode::cmd_lpush(out, key, values)
            })
        });
        Fiber::new(async move {
            match fut {
                Some(f) => f.await,
                None => Ok(0),
            }
        })
    }

    fn rpush(
        &self,
        key: &[u8],
        values: &[&[u8]],
    ) -> Fiber<'d, impl Future<Output = Result<u64, Error>>> {
        let fut = (!values.is_empty()).then(|| {
            Session::dispatch::<ID, S, E, u64>(*self, move |out| {
                encode::cmd_rpush(out, key, values)
            })
        });
        Fiber::new(async move {
            match fut {
                Some(f) => f.await,
                None => Ok(0),
            }
        })
    }

    fn lpop(&self, key: &[u8]) -> Fiber<'d, impl Future<Output = Result<Option<Shared>, Error>>> {
        Session::dispatch::<ID, S, E, Option<Shared>>(*self, move |out| encode::cmd_lpop(out, key))
    }

    fn rpop(&self, key: &[u8]) -> Fiber<'d, impl Future<Output = Result<Option<Shared>, Error>>> {
        Session::dispatch::<ID, S, E, Option<Shared>>(*self, move |out| encode::cmd_rpop(out, key))
    }

    fn lrange(
        &self,
        key: &[u8],
        start: i64,
        stop: i64,
    ) -> Fiber<'d, impl Future<Output = Result<Vec<Shared>, Error>>> {
        Session::dispatch::<ID, S, E, Vec<Shared>>(*self, move |out| {
            encode::cmd_lrange(out, key, start, stop)
        })
    }

    fn llen(&self, key: &[u8]) -> Fiber<'d, impl Future<Output = Result<u64, Error>>> {
        Session::dispatch::<ID, S, E, u64>(*self, move |out| encode::cmd_llen(out, key))
    }

    fn getset(
        &self,
        key: &[u8],
        value: &[u8],
    ) -> Fiber<'d, impl Future<Output = Result<Option<Shared>, Error>>> {
        Session::dispatch::<ID, S, E, Option<Shared>>(*self, move |out| {
            encode::cmd_getset(out, key, value)
        })
    }

    fn getdel(&self, key: &[u8]) -> Fiber<'d, impl Future<Output = Result<Option<Shared>, Error>>> {
        Session::dispatch::<ID, S, E, Option<Shared>>(*self, move |out| {
            encode::cmd_getdel(out, key)
        })
    }

    fn append(
        &self,
        key: &[u8],
        value: &[u8],
    ) -> Fiber<'d, impl Future<Output = Result<u64, Error>>> {
        Session::dispatch::<ID, S, E, u64>(*self, move |out| encode::cmd_append(out, key, value))
    }

    fn strlen(&self, key: &[u8]) -> Fiber<'d, impl Future<Output = Result<u64, Error>>> {
        Session::dispatch::<ID, S, E, u64>(*self, move |out| encode::cmd_strlen(out, key))
    }

    fn incrbyfloat(
        &self,
        key: &[u8],
        by: f64,
    ) -> Fiber<'d, impl Future<Output = Result<f64, Error>>> {
        Session::dispatch::<ID, S, E, f64>(*self, move |out| encode::cmd_incrbyfloat(out, key, by))
    }

    fn key_type(&self, key: &[u8]) -> Fiber<'d, impl Future<Output = Result<Shared, Error>>> {
        let fut = Session::dispatch::<ID, S, E, crate::Value>(*self, move |out| {
            encode::cmd_type(out, key)
        });
        Fiber::new(async move {
            match fut.await? {
                crate::Value::Status(b) => Ok(b),
                crate::Value::Bulk(b) => Ok(b),
                _ => Err(Error::Redis("unexpected TYPE response".into())),
            }
        })
    }

    fn rename(&self, src: &[u8], dst: &[u8]) -> Fiber<'d, impl Future<Output = Result<(), Error>>> {
        Session::dispatch::<ID, S, E, ()>(*self, move |out| encode::cmd_rename(out, src, dst))
    }

    fn persist(&self, key: &[u8]) -> Fiber<'d, impl Future<Output = Result<bool, Error>>> {
        Session::dispatch::<ID, S, E, bool>(*self, move |out| encode::cmd_persist(out, key))
    }

    fn unlink(&self, keys: &[&[u8]]) -> Fiber<'d, impl Future<Output = Result<u64, Error>>> {
        let fut = (!keys.is_empty()).then(|| {
            Session::dispatch::<ID, S, E, u64>(*self, move |out| encode::cmd_unlink(out, keys))
        });
        Fiber::new(async move {
            match fut {
                Some(f) => f.await,
                None => Ok(0),
            }
        })
    }

    fn dbsize(&self) -> Fiber<'d, impl Future<Output = Result<u64, Error>>> {
        Session::dispatch::<ID, S, E, u64>(*self, encode::cmd_dbsize)
    }

    fn scan(
        &self,
        cursor: u64,
        match_pattern: Option<&[u8]>,
        count: Option<u64>,
    ) -> Fiber<'d, impl Future<Output = Result<(u64, Vec<Shared>), Error>>> {
        Session::dispatch::<ID, S, E, (u64, Vec<Shared>)>(*self, move |out| {
            encode::cmd_scan(out, cursor, match_pattern, count)
        })
    }

    fn pfadd(
        &self,
        key: &[u8],
        elements: &[&[u8]],
    ) -> Fiber<'d, impl Future<Output = Result<bool, Error>>> {
        let fut = Session::submit::<ID, S, E, i64>(
            *self,
            Session::frame(|out| encode::cmd_pfadd(out, key, elements)),
        );
        Fiber::new(async move { Ok(fut.await? != 0) })
    }

    fn pfcount(&self, keys: &[&[u8]]) -> Fiber<'d, impl Future<Output = Result<u64, Error>>> {
        let fut = (!keys.is_empty()).then(|| {
            Session::dispatch::<ID, S, E, u64>(*self, move |out| encode::cmd_pfcount(out, keys))
        });
        Fiber::new(async move {
            match fut {
                Some(f) => f.await,
                None => Ok(0),
            }
        })
    }

    fn pfmerge(
        &self,
        dest: &[u8],
        sources: &[&[u8]],
    ) -> Fiber<'d, impl Future<Output = Result<(), Error>>> {
        Session::submit::<ID, S, E, ()>(
            *self,
            Session::frame(|out| encode::cmd_pfmerge(out, dest, sources)),
        )
    }

    fn bitcount(&self, key: &[u8]) -> Fiber<'d, impl Future<Output = Result<u64, Error>>> {
        Session::dispatch::<ID, S, E, u64>(*self, move |out| {
            encode::cmd_raw(out, &[b"BITCOUNT", key])
        })
    }

    fn bitcount_range(
        &self,
        key: &[u8],
        start: i64,
        end: i64,
    ) -> Fiber<'d, impl Future<Output = Result<u64, Error>>> {
        let s = start.to_string();
        let e = end.to_string();
        Session::dispatch::<ID, S, E, u64>(*self, move |out| {
            encode::cmd_raw(out, &[b"BITCOUNT", key, s.as_bytes(), e.as_bytes()])
        })
    }

    fn setbit(
        &self,
        key: &[u8],
        offset: u64,
        value: bool,
    ) -> Fiber<'d, impl Future<Output = Result<bool, Error>>> {
        let off = offset.to_string();
        let v: &[u8] = if value { b"1" } else { b"0" };
        let fut = Session::dispatch::<ID, S, E, i64>(*self, move |out| {
            encode::cmd_raw(out, &[b"SETBIT", key, off.as_bytes(), v])
        });
        Fiber::new(async move { Ok(fut.await? != 0) })
    }

    fn getbit(
        &self,
        key: &[u8],
        offset: u64,
    ) -> Fiber<'d, impl Future<Output = Result<bool, Error>>> {
        let off = offset.to_string();
        let fut = Session::dispatch::<ID, S, E, i64>(*self, move |out| {
            encode::cmd_raw(out, &[b"GETBIT", key, off.as_bytes()])
        });
        Fiber::new(async move { Ok(fut.await? != 0) })
    }

    fn bitop(
        &self,
        op: &[u8],
        dest: &[u8],
        sources: &[&[u8]],
    ) -> Fiber<'d, impl Future<Output = Result<u64, Error>>> {
        Session::submit::<ID, S, E, u64>(
            *self,
            Session::frame(|out| encode::cmd_bitop(out, op, dest, sources)),
        )
    }

    fn bitpos(&self, key: &[u8], bit: bool) -> Fiber<'d, impl Future<Output = Result<i64, Error>>> {
        let b: &[u8] = if bit { b"1" } else { b"0" };
        Session::dispatch::<ID, S, E, i64>(*self, move |out| {
            encode::cmd_raw(out, &[b"BITPOS", key, b])
        })
    }

    fn geoadd(
        &self,
        key: &[u8],
        longitude: f64,
        latitude: f64,
        member: &[u8],
    ) -> Fiber<'d, impl Future<Output = Result<u64, Error>>> {
        let lng = format!("{longitude}");
        let lat = format!("{latitude}");
        Session::dispatch::<ID, S, E, u64>(*self, move |out| {
            encode::cmd_raw(
                out,
                &[b"GEOADD", key, lng.as_bytes(), lat.as_bytes(), member],
            )
        })
    }

    fn geodist(
        &self,
        key: &[u8],
        member1: &[u8],
        member2: &[u8],
        unit: Option<&[u8]>,
    ) -> Fiber<'d, impl Future<Output = Result<Option<f64>, Error>>> {
        let fut = Session::submit::<ID, S, E, Option<Shared>>(
            *self,
            Session::frame(|out| encode::cmd_geodist(out, key, member1, member2, unit)),
        );
        Fiber::new(async move {
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

    fn geopos(
        &self,
        key: &[u8],
        members: &[&[u8]],
    ) -> Fiber<'d, impl Future<Output = Result<Vec<Option<(f64, f64)>>, Error>>> {
        let fut = Session::submit::<ID, S, E, crate::value::Value>(
            *self,
            Session::frame(|out| encode::cmd_geopos(out, key, members)),
        );
        Fiber::new(async move {
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

    fn geosearch_radius(
        &self,
        key: &[u8],
        longitude: f64,
        latitude: f64,
        radius: f64,
        unit: &[u8],
    ) -> Fiber<'d, impl Future<Output = Result<Vec<Shared>, Error>>> {
        let lng = format!("{longitude}");
        let lat = format!("{latitude}");
        let r = format!("{radius}");
        Session::dispatch::<ID, S, E, Vec<Shared>>(*self, move |out| {
            encode::cmd_raw(
                out,
                &[
                    b"GEOSEARCH",
                    key,
                    b"FROMLONLAT",
                    lng.as_bytes(),
                    lat.as_bytes(),
                    b"BYRADIUS",
                    r.as_bytes(),
                    unit,
                ],
            )
        })
    }

    fn info(
        &self,
        section: Option<&[u8]>,
    ) -> Fiber<'d, impl Future<Output = Result<Shared, Error>>> {
        Session::dispatch::<ID, S, E, Shared>(*self, move |out| match section {
            None => encode::cmd_raw(out, &[b"INFO"]),
            Some(s) => encode::cmd_raw(out, &[b"INFO", s]),
        })
    }

    fn client_getname(&self) -> Fiber<'d, impl Future<Output = Result<Option<Shared>, Error>>> {
        Session::dispatch::<ID, S, E, Option<Shared>>(*self, move |out| {
            encode::cmd_raw(out, &[b"CLIENT", b"GETNAME"])
        })
    }

    fn client_setname(&self, name: &[u8]) -> Fiber<'d, impl Future<Output = Result<(), Error>>> {
        Session::dispatch::<ID, S, E, ()>(*self, move |out| {
            encode::cmd_raw(out, &[b"CLIENT", b"SETNAME", name])
        })
    }

    fn client_id(&self) -> Fiber<'d, impl Future<Output = Result<i64, Error>>> {
        Session::dispatch::<ID, S, E, i64>(*self, move |out| {
            encode::cmd_raw(out, &[b"CLIENT", b"ID"])
        })
    }

    fn client_list(&self) -> Fiber<'d, impl Future<Output = Result<Shared, Error>>> {
        Session::dispatch::<ID, S, E, Shared>(*self, move |out| {
            encode::cmd_raw(out, &[b"CLIENT", b"LIST"])
        })
    }

    fn client_kill_addr(&self, addr: &[u8]) -> Fiber<'d, impl Future<Output = Result<u64, Error>>> {
        Session::dispatch::<ID, S, E, u64>(*self, move |out| {
            encode::cmd_raw(out, &[b"CLIENT", b"KILL", b"ADDR", addr])
        })
    }

    fn config_get(
        &self,
        parameter: &[u8],
    ) -> Fiber<'d, impl Future<Output = Result<Vec<(Shared, Shared)>, Error>>> {
        Session::dispatch::<ID, S, E, Vec<(Shared, Shared)>>(*self, move |out| {
            encode::cmd_raw(out, &[b"CONFIG", b"GET", parameter])
        })
    }

    fn config_set(
        &self,
        parameter: &[u8],
        value: &[u8],
    ) -> Fiber<'d, impl Future<Output = Result<(), Error>>> {
        Session::dispatch::<ID, S, E, ()>(*self, move |out| {
            encode::cmd_raw(out, &[b"CONFIG", b"SET", parameter, value])
        })
    }

    fn config_resetstat(&self) -> Fiber<'d, impl Future<Output = Result<(), Error>>> {
        Session::dispatch::<ID, S, E, ()>(*self, move |out| {
            encode::cmd_raw(out, &[b"CONFIG", b"RESETSTAT"])
        })
    }

    fn debug_object(&self, key: &[u8]) -> Fiber<'d, impl Future<Output = Result<Shared, Error>>> {
        let fut = Session::dispatch::<ID, S, E, crate::value::Value>(*self, move |out| {
            encode::cmd_raw(out, &[b"DEBUG", b"OBJECT", key])
        });
        Fiber::new(async move {
            match fut.await? {
                crate::value::Value::Status(b) => Ok(b),
                crate::value::Value::Bulk(b) => Ok(b),
                _ => Err(Error::Redis("DEBUG OBJECT: unexpected value".into())),
            }
        })
    }

    fn auth(&self, password: &[u8]) -> Fiber<'d, impl Future<Output = Result<(), Error>>> {
        Session::dispatch::<ID, S, E, ()>(*self, move |out| {
            encode::cmd_raw(out, &[b"AUTH", password])
        })
    }

    fn auth_user(
        &self,
        username: &[u8],
        password: &[u8],
    ) -> Fiber<'d, impl Future<Output = Result<(), Error>>> {
        Session::dispatch::<ID, S, E, ()>(*self, move |out| {
            encode::cmd_raw(out, &[b"AUTH", username, password])
        })
    }

    fn select_db(&self, db: u32) -> Fiber<'d, impl Future<Output = Result<(), Error>>> {
        let s = db.to_string();
        Session::dispatch::<ID, S, E, ()>(*self, move |out| {
            encode::cmd_raw(out, &[b"SELECT", s.as_bytes()])
        })
    }

    fn hello(
        &self,
        protocol: Option<u8>,
    ) -> Fiber<'d, impl Future<Output = Result<crate::value::Value, Error>>> {
        let p_s = protocol.map(|p| p.to_string());
        Session::dispatch::<ID, S, E, crate::value::Value>(*self, move |out| match &p_s {
            None => encode::cmd_raw(out, &[b"HELLO"]),
            Some(s) => encode::cmd_raw(out, &[b"HELLO", s.as_bytes()]),
        })
    }
}

impl Session {
    fn frame(encode: impl FnOnce(&mut o3::buffer::Owned)) -> Shared {
        let mut buf = o3::buffer::Owned::with_capacity(64);
        encode(&mut buf);
        buf.freeze()
    }

    fn dispatch<'d, const ID: u8, S, E, R>(
        inner: Holding<'d, Connector<ID, Session, S, E>>,
        encode: impl FnOnce(&mut o3::buffer::Owned),
    ) -> Fiber<'d, impl Future<Output = Result<R, Error>>>
    where
        S: Dialer<E::Transport> + 'd,
        E: Env + 'd,
        E::Transport: Transport<Addr: Clone>,
        R: FromValue,
    {
        Self::submit::<ID, S, E, R>(inner, Self::frame(encode))
    }

    fn submit<'d, const ID: u8, S, E, R>(
        holding: Holding<'d, Connector<ID, Session, S, E>>,
        bytes: Shared,
    ) -> Fiber<'d, impl Future<Output = Result<R, Error>>>
    where
        S: Dialer<E::Transport> + 'd,
        E: Env + 'd,
        E::Transport: Transport<Addr: Clone>,
        R: FromValue,
    {
        let mut bytes = Some(bytes);
        Fiber::new(async move {
            let reply = poll_fn(move |cx| {
                let _ = cx;
                let bytes = bytes.take().expect("redis dispatch enqueue polled twice");
                Poll::Ready(Self::enqueue::<ID, S, E>(holding, bytes))
            })
            .await?;
            let value = reply.await;
            value.and_then(R::from_value)
        })
    }

    fn enqueue<'d, const ID: u8, S, E>(
        holding: Holding<'d, Connector<ID, Session, S, E>>,
        bytes: Shared,
    ) -> Result<Pin<Box<Reply<'d, Outcome, ExtractValue>>>, Error>
    where
        S: Dialer<E::Transport>,
        E: Env,
        E::Transport: Transport<Addr: Clone>,
    {
        let mut reply: Pin<Box<Reply<'d, Outcome, ExtractValue>>> = Box::pin(Reply::new());
        let mut h = holding.hold();
        let mut pool = h.as_mut();
        let conn_id;
        {
            let shared = &mut pool.as_mut().session_mut().shared;
            if let Some(e) = shared.fatal.as_ref() {
                return Err(Error::Redis(e.to_string()));
            }
            conn_id = shared
                .conn_id
                .ok_or_else(|| Error::Redis("redis conn not yet ready".into()))?;
            let depth = shared.slab.depth();
            if depth >= shared.max_inflight {
                return Err(Error::Backpressure {
                    message: format!("pipeline full ({}/{})", depth, shared.max_inflight),
                });
            }
        }
        match pool.as_mut().state_for(conn_id) {
            None => return Err(Error::Redis("redis conn vanished mid-dispatch".into())),
            Some(session) => {
                if !session.enqueue(bytes) {
                    return Err(Error::Backpressure {
                        message: "egress over cap".into(),
                    });
                }
            }
        }
        let reply_mut = reply.as_mut().get_mut();
        reply_mut.attach(&mut pool.as_mut().session_mut().shared.slab);
        pool.request_flush(conn_id);
        Ok(reply)
    }
}

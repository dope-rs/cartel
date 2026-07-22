pub(super) trait Sink {
    fn push(&mut self, byte: u8);
    fn extend_from_slice(&mut self, src: &[u8]);
}

impl Sink for Vec<u8> {
    fn push(&mut self, byte: u8) {
        Vec::push(self, byte);
    }

    fn extend_from_slice(&mut self, src: &[u8]) {
        Vec::extend_from_slice(self, src);
    }
}

fn write_uint(out: &mut impl Sink, mut n: u64) {
    let mut buf = [0u8; 20];
    let mut i = buf.len();
    if n == 0 {
        i -= 1;
        buf[i] = b'0';
    } else {
        while n > 0 {
            i -= 1;
            buf[i] = b'0' + (n % 10) as u8;
            n /= 10;
        }
    }
    out.extend_from_slice(&buf[i..]);
}

fn write_bulk(out: &mut impl Sink, payload: &[u8]) {
    out.push(b'$');
    write_uint(out, payload.len() as u64);
    out.extend_from_slice(b"\r\n");
    out.extend_from_slice(payload);
    out.extend_from_slice(b"\r\n");
}

fn write_array_header(out: &mut impl Sink, n: usize) {
    out.push(b'*');
    write_uint(out, n as u64);
    out.extend_from_slice(b"\r\n");
}

fn write_int_bulk(out: &mut impl Sink, n: i64) {
    let mut buf = [0u8; 21];
    let s = format_int_into(&mut buf, n);
    write_bulk(out, s);
}

fn write_uint_bulk(out: &mut impl Sink, n: u64) {
    let mut buf = [0u8; 20];
    let mut index = buf.len();
    let mut value = n;
    if value == 0 {
        index -= 1;
        buf[index] = b'0';
    } else {
        while value > 0 {
            index -= 1;
            buf[index] = b'0' + (value % 10) as u8;
            value /= 10;
        }
    }
    write_bulk(out, &buf[index..]);
}

fn format_int_into(buf: &mut [u8; 21], n: i64) -> &[u8] {
    let mut i = buf.len();
    let (sign, mut u) = if n < 0 {
        (true, (n as i128).unsigned_abs() as u64)
    } else {
        (false, n as u64)
    };
    if u == 0 {
        i -= 1;
        buf[i] = b'0';
    } else {
        while u > 0 {
            i -= 1;
            buf[i] = b'0' + (u % 10) as u8;
            u /= 10;
        }
    }
    if sign {
        i -= 1;
        buf[i] = b'-';
    }
    &buf[i..]
}

pub(super) fn cmd1(out: &mut impl Sink, verb: &[u8], arg: &[u8]) {
    write_array_header(out, 2);
    write_bulk(out, verb);
    write_bulk(out, arg);
}

pub(super) fn cmd2(out: &mut impl Sink, verb: &[u8], a: &[u8], b: &[u8]) {
    write_array_header(out, 3);
    write_bulk(out, verb);
    write_bulk(out, a);
    write_bulk(out, b);
}

pub(super) fn cmd_get(out: &mut impl Sink, key: &[u8]) {
    cmd1(out, b"GET", key);
}

pub(super) fn cmd_set(out: &mut impl Sink, key: &[u8], value: &[u8]) {
    cmd2(out, b"SET", key, value);
}

pub(super) fn cmd_set_ex(out: &mut impl Sink, key: &[u8], value: &[u8], seconds: u64) {
    write_array_header(out, 5);
    write_bulk(out, b"SET");
    write_bulk(out, key);
    write_bulk(out, value);
    write_bulk(out, b"EX");
    write_uint_bulk(out, seconds);
}

pub(super) fn cmd_set_px(out: &mut impl Sink, key: &[u8], value: &[u8], millis: u64) {
    write_array_header(out, 5);
    write_bulk(out, b"SET");
    write_bulk(out, key);
    write_bulk(out, value);
    write_bulk(out, b"PX");
    write_uint_bulk(out, millis);
}

pub(super) fn cmd_set_nx(out: &mut impl Sink, key: &[u8], value: &[u8]) {
    write_array_header(out, 4);
    write_bulk(out, b"SET");
    write_bulk(out, key);
    write_bulk(out, value);
    write_bulk(out, b"NX");
}

pub(super) fn cmd_del(out: &mut impl Sink, keys: &[&[u8]]) {
    write_array_header(out, 1 + keys.len());
    write_bulk(out, b"DEL");
    for k in keys {
        write_bulk(out, k);
    }
}

pub(super) fn cmd_exists(out: &mut impl Sink, keys: &[&[u8]]) {
    write_array_header(out, 1 + keys.len());
    write_bulk(out, b"EXISTS");
    for k in keys {
        write_bulk(out, k);
    }
}

pub(super) fn cmd_incr(out: &mut impl Sink, key: &[u8]) {
    cmd1(out, b"INCR", key);
}

pub(super) fn cmd_decr(out: &mut impl Sink, key: &[u8]) {
    cmd1(out, b"DECR", key);
}

pub(super) fn cmd_incrby(out: &mut impl Sink, key: &[u8], by: i64) {
    write_array_header(out, 3);
    write_bulk(out, b"INCRBY");
    write_bulk(out, key);
    write_int_bulk(out, by);
}

pub(super) fn cmd_expire(out: &mut impl Sink, key: &[u8], seconds: u64) {
    write_array_header(out, 3);
    write_bulk(out, b"EXPIRE");
    write_bulk(out, key);
    write_uint_bulk(out, seconds);
}

pub(super) fn cmd_ttl(out: &mut impl Sink, key: &[u8]) {
    cmd1(out, b"TTL", key);
}

pub(super) fn cmd_mget(out: &mut impl Sink, keys: &[&[u8]]) {
    write_array_header(out, 1 + keys.len());
    write_bulk(out, b"MGET");
    for k in keys {
        write_bulk(out, k);
    }
}

pub(super) fn cmd_mset(out: &mut impl Sink, kv: &[(&[u8], &[u8])]) {
    write_array_header(out, 1 + kv.len() * 2);
    write_bulk(out, b"MSET");
    for (k, v) in kv {
        write_bulk(out, k);
        write_bulk(out, v);
    }
}

pub(super) fn cmd_ping(out: &mut impl Sink) {
    out.extend_from_slice(b"*1\r\n$4\r\nPING\r\n");
}

pub(super) fn cmd_raw(out: &mut impl Sink, args: &[&[u8]]) {
    write_array_header(out, args.len());
    for a in args {
        write_bulk(out, a);
    }
}

fn write_float_bulk(out: &mut impl Sink, v: f64) {
    if v == 0.0 {
        write_bulk(out, b"0");
        return;
    }
    if v.is_nan() {
        write_bulk(out, b"nan");
        return;
    }
    if v.is_infinite() {
        write_bulk(out, if v > 0.0 { b"inf" } else { b"-inf" });
        return;
    }
    let mut formatted = ryu::Buffer::new();
    write_bulk(out, formatted.format_finite(v).as_bytes());
}

pub(super) fn cmd_hget(out: &mut impl Sink, key: &[u8], field: &[u8]) {
    cmd2(out, b"HGET", key, field);
}

pub(super) fn cmd_hset_pairs(out: &mut impl Sink, key: &[u8], fv: &[(&[u8], &[u8])]) {
    write_array_header(out, 2 + fv.len() * 2);
    write_bulk(out, b"HSET");
    write_bulk(out, key);
    for (f, v) in fv {
        write_bulk(out, f);
        write_bulk(out, v);
    }
}

pub(super) fn cmd_hmget(out: &mut impl Sink, key: &[u8], fields: &[&[u8]]) {
    write_array_header(out, 2 + fields.len());
    write_bulk(out, b"HMGET");
    write_bulk(out, key);
    for f in fields {
        write_bulk(out, f);
    }
}

pub(super) fn cmd_hdel(out: &mut impl Sink, key: &[u8], fields: &[&[u8]]) {
    write_array_header(out, 2 + fields.len());
    write_bulk(out, b"HDEL");
    write_bulk(out, key);
    for f in fields {
        write_bulk(out, f);
    }
}

pub(super) fn cmd_hgetall(out: &mut impl Sink, key: &[u8]) {
    cmd1(out, b"HGETALL", key);
}

pub(super) fn cmd_hlen(out: &mut impl Sink, key: &[u8]) {
    cmd1(out, b"HLEN", key);
}

pub(super) fn cmd_hexists(out: &mut impl Sink, key: &[u8], field: &[u8]) {
    cmd2(out, b"HEXISTS", key, field);
}

pub(super) fn cmd_hincrby(out: &mut impl Sink, key: &[u8], field: &[u8], by: i64) {
    write_array_header(out, 4);
    write_bulk(out, b"HINCRBY");
    write_bulk(out, key);
    write_bulk(out, field);
    write_int_bulk(out, by);
}

pub(super) fn cmd_sadd(out: &mut impl Sink, key: &[u8], members: &[&[u8]]) {
    write_array_header(out, 2 + members.len());
    write_bulk(out, b"SADD");
    write_bulk(out, key);
    for m in members {
        write_bulk(out, m);
    }
}

pub(super) fn cmd_srem(out: &mut impl Sink, key: &[u8], members: &[&[u8]]) {
    write_array_header(out, 2 + members.len());
    write_bulk(out, b"SREM");
    write_bulk(out, key);
    for m in members {
        write_bulk(out, m);
    }
}

pub(super) fn cmd_smembers(out: &mut impl Sink, key: &[u8]) {
    cmd1(out, b"SMEMBERS", key);
}

pub(super) fn cmd_sismember(out: &mut impl Sink, key: &[u8], member: &[u8]) {
    cmd2(out, b"SISMEMBER", key, member);
}

pub(super) fn cmd_scard(out: &mut impl Sink, key: &[u8]) {
    cmd1(out, b"SCARD", key);
}

pub(super) fn cmd_zadd(out: &mut impl Sink, key: &[u8], score: f64, member: &[u8]) {
    write_array_header(out, 4);
    write_bulk(out, b"ZADD");
    write_bulk(out, key);
    write_float_bulk(out, score);
    write_bulk(out, member);
}

pub(super) fn cmd_zrem(out: &mut impl Sink, key: &[u8], members: &[&[u8]]) {
    write_array_header(out, 2 + members.len());
    write_bulk(out, b"ZREM");
    write_bulk(out, key);
    for m in members {
        write_bulk(out, m);
    }
}

pub(super) fn cmd_zrange(out: &mut impl Sink, key: &[u8], start: i64, stop: i64) {
    write_array_header(out, 4);
    write_bulk(out, b"ZRANGE");
    write_bulk(out, key);
    write_int_bulk(out, start);
    write_int_bulk(out, stop);
}

pub(super) fn cmd_zrange_with_scores(out: &mut impl Sink, key: &[u8], start: i64, stop: i64) {
    write_array_header(out, 5);
    write_bulk(out, b"ZRANGE");
    write_bulk(out, key);
    write_int_bulk(out, start);
    write_int_bulk(out, stop);
    write_bulk(out, b"WITHSCORES");
}

pub(super) fn cmd_zrevrange_with_scores(out: &mut impl Sink, key: &[u8], start: i64, stop: i64) {
    write_array_header(out, 5);
    write_bulk(out, b"ZREVRANGE");
    write_bulk(out, key);
    write_int_bulk(out, start);
    write_int_bulk(out, stop);
    write_bulk(out, b"WITHSCORES");
}

pub(super) fn cmd_zrangebyscore(out: &mut impl Sink, key: &[u8], min: f64, max: f64) {
    write_array_header(out, 4);
    write_bulk(out, b"ZRANGEBYSCORE");
    write_bulk(out, key);
    write_float_bulk(out, min);
    write_float_bulk(out, max);
}

pub(super) fn cmd_zrank(out: &mut impl Sink, key: &[u8], member: &[u8]) {
    cmd2(out, b"ZRANK", key, member);
}

pub(super) fn cmd_zrevrank(out: &mut impl Sink, key: &[u8], member: &[u8]) {
    cmd2(out, b"ZREVRANK", key, member);
}

pub(super) fn cmd_zscore(out: &mut impl Sink, key: &[u8], member: &[u8]) {
    cmd2(out, b"ZSCORE", key, member);
}

pub(super) fn cmd_zcard(out: &mut impl Sink, key: &[u8]) {
    cmd1(out, b"ZCARD", key);
}

pub(super) fn cmd_zincrby(out: &mut impl Sink, key: &[u8], by: f64, member: &[u8]) {
    write_array_header(out, 4);
    write_bulk(out, b"ZINCRBY");
    write_bulk(out, key);
    write_float_bulk(out, by);
    write_bulk(out, member);
}

pub(super) fn cmd_lpush(out: &mut impl Sink, key: &[u8], values: &[&[u8]]) {
    write_array_header(out, 2 + values.len());
    write_bulk(out, b"LPUSH");
    write_bulk(out, key);
    for v in values {
        write_bulk(out, v);
    }
}

pub(super) fn cmd_rpush(out: &mut impl Sink, key: &[u8], values: &[&[u8]]) {
    write_array_header(out, 2 + values.len());
    write_bulk(out, b"RPUSH");
    write_bulk(out, key);
    for v in values {
        write_bulk(out, v);
    }
}

pub(super) fn cmd_lpop(out: &mut impl Sink, key: &[u8]) {
    cmd1(out, b"LPOP", key);
}

pub(super) fn cmd_rpop(out: &mut impl Sink, key: &[u8]) {
    cmd1(out, b"RPOP", key);
}

pub(super) fn cmd_lrange(out: &mut impl Sink, key: &[u8], start: i64, stop: i64) {
    write_array_header(out, 4);
    write_bulk(out, b"LRANGE");
    write_bulk(out, key);
    write_int_bulk(out, start);
    write_int_bulk(out, stop);
}

pub(super) fn cmd_llen(out: &mut impl Sink, key: &[u8]) {
    cmd1(out, b"LLEN", key);
}

pub(super) fn cmd_getset(out: &mut impl Sink, key: &[u8], value: &[u8]) {
    cmd2(out, b"GETSET", key, value);
}

pub(super) fn cmd_getdel(out: &mut impl Sink, key: &[u8]) {
    cmd1(out, b"GETDEL", key);
}

pub(super) fn cmd_append(out: &mut impl Sink, key: &[u8], value: &[u8]) {
    cmd2(out, b"APPEND", key, value);
}

pub(super) fn cmd_strlen(out: &mut impl Sink, key: &[u8]) {
    cmd1(out, b"STRLEN", key);
}

pub(super) fn cmd_incrbyfloat(out: &mut impl Sink, key: &[u8], by: f64) {
    write_array_header(out, 3);
    write_bulk(out, b"INCRBYFLOAT");
    write_bulk(out, key);
    write_float_bulk(out, by);
}

pub(super) fn cmd_type(out: &mut impl Sink, key: &[u8]) {
    cmd1(out, b"TYPE", key);
}

pub(super) fn cmd_rename(out: &mut impl Sink, src: &[u8], dst: &[u8]) {
    cmd2(out, b"RENAME", src, dst);
}

pub(super) fn cmd_persist(out: &mut impl Sink, key: &[u8]) {
    cmd1(out, b"PERSIST", key);
}

pub(super) fn cmd_unlink(out: &mut impl Sink, keys: &[&[u8]]) {
    write_array_header(out, 1 + keys.len());
    write_bulk(out, b"UNLINK");
    for k in keys {
        write_bulk(out, k);
    }
}

pub(super) fn cmd_dbsize(out: &mut impl Sink) {
    out.extend_from_slice(b"*1\r\n$6\r\nDBSIZE\r\n");
}

pub(super) fn cmd_scan(
    out: &mut impl Sink,
    cursor: u64,
    match_pattern: Option<&[u8]>,
    count: Option<u64>,
) {
    let mut argc = 2;
    if match_pattern.is_some() {
        argc += 2;
    }
    if count.is_some() {
        argc += 2;
    }
    write_array_header(out, argc);
    write_bulk(out, b"SCAN");
    write_uint_bulk(out, cursor);
    if let Some(p) = match_pattern {
        write_bulk(out, b"MATCH");
        write_bulk(out, p);
    }
    if let Some(c) = count {
        write_bulk(out, b"COUNT");
        write_uint_bulk(out, c);
    }
}

pub(super) fn cmd_pfadd(out: &mut impl Sink, key: &[u8], elements: &[&[u8]]) {
    write_array_header(out, 2 + elements.len());
    write_bulk(out, b"PFADD");
    write_bulk(out, key);
    for e in elements {
        write_bulk(out, e);
    }
}

pub(super) fn cmd_pfcount(out: &mut impl Sink, keys: &[&[u8]]) {
    write_array_header(out, 1 + keys.len());
    write_bulk(out, b"PFCOUNT");
    for k in keys {
        write_bulk(out, k);
    }
}

pub(super) fn cmd_pfmerge(out: &mut impl Sink, dest: &[u8], sources: &[&[u8]]) {
    write_array_header(out, 2 + sources.len());
    write_bulk(out, b"PFMERGE");
    write_bulk(out, dest);
    for s in sources {
        write_bulk(out, s);
    }
}

pub(super) fn cmd_bitop(out: &mut impl Sink, op: &[u8], dest: &[u8], sources: &[&[u8]]) {
    write_array_header(out, 3 + sources.len());
    write_bulk(out, b"BITOP");
    write_bulk(out, op);
    write_bulk(out, dest);
    for s in sources {
        write_bulk(out, s);
    }
}

pub(super) fn cmd_bit_count_range(out: &mut impl Sink, key: &[u8], start: i64, end: i64) {
    write_array_header(out, 4);
    write_bulk(out, b"BITCOUNT");
    write_bulk(out, key);
    write_int_bulk(out, start);
    write_int_bulk(out, end);
}

pub(super) fn cmd_set_bit(out: &mut impl Sink, key: &[u8], offset: u64, value: bool) {
    write_array_header(out, 4);
    write_bulk(out, b"SETBIT");
    write_bulk(out, key);
    write_uint_bulk(out, offset);
    write_bulk(out, if value { b"1" } else { b"0" });
}

pub(super) fn cmd_get_bit(out: &mut impl Sink, key: &[u8], offset: u64) {
    write_array_header(out, 3);
    write_bulk(out, b"GETBIT");
    write_bulk(out, key);
    write_uint_bulk(out, offset);
}

pub(super) fn cmd_geo_add(
    out: &mut impl Sink,
    key: &[u8],
    longitude: f64,
    latitude: f64,
    member: &[u8],
) {
    write_array_header(out, 5);
    write_bulk(out, b"GEOADD");
    write_bulk(out, key);
    write_float_bulk(out, longitude);
    write_float_bulk(out, latitude);
    write_bulk(out, member);
}

pub(super) fn cmd_geo_search_radius(
    out: &mut impl Sink,
    key: &[u8],
    longitude: f64,
    latitude: f64,
    radius: f64,
    unit: &[u8],
) {
    write_array_header(out, 8);
    write_bulk(out, b"GEOSEARCH");
    write_bulk(out, key);
    write_bulk(out, b"FROMLONLAT");
    write_float_bulk(out, longitude);
    write_float_bulk(out, latitude);
    write_bulk(out, b"BYRADIUS");
    write_float_bulk(out, radius);
    write_bulk(out, unit);
}

pub(super) fn cmd_select(out: &mut impl Sink, db: u32) {
    write_array_header(out, 2);
    write_bulk(out, b"SELECT");
    write_int_bulk(out, db as i64);
}

pub(super) fn cmd_hello(out: &mut impl Sink, protocol: Option<u8>) {
    write_array_header(out, 1 + protocol.is_some() as usize);
    write_bulk(out, b"HELLO");
    if let Some(protocol) = protocol {
        write_int_bulk(out, protocol as i64);
    }
}

pub(super) fn cmd_geodist(
    out: &mut impl Sink,
    key: &[u8],
    m1: &[u8],
    m2: &[u8],
    unit: Option<&[u8]>,
) {
    write_array_header(out, 4 + unit.is_some() as usize);
    write_bulk(out, b"GEODIST");
    write_bulk(out, key);
    write_bulk(out, m1);
    write_bulk(out, m2);
    if let Some(u) = unit {
        write_bulk(out, u);
    }
}

pub(super) fn cmd_geopos(out: &mut impl Sink, key: &[u8], members: &[&[u8]]) {
    write_array_header(out, 2 + members.len());
    write_bulk(out, b"GEOPOS");
    write_bulk(out, key);
    for m in members {
        write_bulk(out, m);
    }
}

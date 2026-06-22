use o3::buffer::Owned;

fn write_uint(out: &mut Owned, mut n: u64) {
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

fn write_bulk(out: &mut Owned, payload: &[u8]) {
    out.push(b'$');
    write_uint(out, payload.len() as u64);
    out.extend_from_slice(b"\r\n");
    out.extend_from_slice(payload);
    out.extend_from_slice(b"\r\n");
}

fn write_array_header(out: &mut Owned, n: usize) {
    out.push(b'*');
    write_uint(out, n as u64);
    out.extend_from_slice(b"\r\n");
}

fn write_int_bulk(out: &mut Owned, n: i64) {
    let mut buf = [0u8; 21];
    let s = format_int_into(&mut buf, n);
    write_bulk(out, s);
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

pub(super) fn cmd1(out: &mut Owned, verb: &[u8], arg: &[u8]) {
    write_array_header(out, 2);
    write_bulk(out, verb);
    write_bulk(out, arg);
}

pub(super) fn cmd2(out: &mut Owned, verb: &[u8], a: &[u8], b: &[u8]) {
    write_array_header(out, 3);
    write_bulk(out, verb);
    write_bulk(out, a);
    write_bulk(out, b);
}

pub(super) fn cmd_get(out: &mut Owned, key: &[u8]) {
    cmd1(out, b"GET", key);
}

pub(super) fn cmd_set(out: &mut Owned, key: &[u8], value: &[u8]) {
    cmd2(out, b"SET", key, value);
}

pub(super) fn cmd_set_ex(out: &mut Owned, key: &[u8], value: &[u8], seconds: u64) {
    write_array_header(out, 5);
    write_bulk(out, b"SET");
    write_bulk(out, key);
    write_bulk(out, value);
    write_bulk(out, b"EX");
    write_int_bulk(out, seconds as i64);
}

pub(super) fn cmd_set_px(out: &mut Owned, key: &[u8], value: &[u8], millis: u64) {
    write_array_header(out, 5);
    write_bulk(out, b"SET");
    write_bulk(out, key);
    write_bulk(out, value);
    write_bulk(out, b"PX");
    write_int_bulk(out, millis as i64);
}

pub(super) fn cmd_set_nx(out: &mut Owned, key: &[u8], value: &[u8]) {
    write_array_header(out, 4);
    write_bulk(out, b"SET");
    write_bulk(out, key);
    write_bulk(out, value);
    write_bulk(out, b"NX");
}

pub(super) fn cmd_del(out: &mut Owned, keys: &[&[u8]]) {
    write_array_header(out, 1 + keys.len());
    write_bulk(out, b"DEL");
    for k in keys {
        write_bulk(out, k);
    }
}

pub(super) fn cmd_exists(out: &mut Owned, keys: &[&[u8]]) {
    write_array_header(out, 1 + keys.len());
    write_bulk(out, b"EXISTS");
    for k in keys {
        write_bulk(out, k);
    }
}

pub(super) fn cmd_incr(out: &mut Owned, key: &[u8]) {
    cmd1(out, b"INCR", key);
}

pub(super) fn cmd_decr(out: &mut Owned, key: &[u8]) {
    cmd1(out, b"DECR", key);
}

pub(super) fn cmd_incrby(out: &mut Owned, key: &[u8], by: i64) {
    write_array_header(out, 3);
    write_bulk(out, b"INCRBY");
    write_bulk(out, key);
    write_int_bulk(out, by);
}

pub(super) fn cmd_expire(out: &mut Owned, key: &[u8], seconds: u64) {
    write_array_header(out, 3);
    write_bulk(out, b"EXPIRE");
    write_bulk(out, key);
    write_int_bulk(out, seconds as i64);
}

pub(super) fn cmd_ttl(out: &mut Owned, key: &[u8]) {
    cmd1(out, b"TTL", key);
}

pub(super) fn cmd_mget(out: &mut Owned, keys: &[&[u8]]) {
    write_array_header(out, 1 + keys.len());
    write_bulk(out, b"MGET");
    for k in keys {
        write_bulk(out, k);
    }
}

pub(super) fn cmd_mset(out: &mut Owned, kv: &[(&[u8], &[u8])]) {
    write_array_header(out, 1 + kv.len() * 2);
    write_bulk(out, b"MSET");
    for (k, v) in kv {
        write_bulk(out, k);
        write_bulk(out, v);
    }
}

pub(super) fn cmd_ping(out: &mut Owned) {
    out.extend_from_slice(b"*1\r\n$4\r\nPING\r\n");
}

pub(super) fn cmd_raw(out: &mut Owned, args: &[&[u8]]) {
    write_array_header(out, args.len());
    for a in args {
        write_bulk(out, a);
    }
}

fn write_float_bulk(out: &mut Owned, v: f64) {
    let s = ryu_like_format(v);
    write_bulk(out, s.as_bytes());
}

fn ryu_like_format(v: f64) -> String {
    if v == 0.0 {
        return "0".to_string();
    }
    if v.is_nan() {
        return "nan".to_string();
    }
    if v.is_infinite() {
        return if v > 0.0 {
            "inf".to_string()
        } else {
            "-inf".to_string()
        };
    }
    format!("{v}")
}

pub(super) fn cmd_hget(out: &mut Owned, key: &[u8], field: &[u8]) {
    cmd2(out, b"HGET", key, field);
}

pub(super) fn cmd_hset_pairs(out: &mut Owned, key: &[u8], fv: &[(&[u8], &[u8])]) {
    write_array_header(out, 2 + fv.len() * 2);
    write_bulk(out, b"HSET");
    write_bulk(out, key);
    for (f, v) in fv {
        write_bulk(out, f);
        write_bulk(out, v);
    }
}

pub(super) fn cmd_hmget(out: &mut Owned, key: &[u8], fields: &[&[u8]]) {
    write_array_header(out, 2 + fields.len());
    write_bulk(out, b"HMGET");
    write_bulk(out, key);
    for f in fields {
        write_bulk(out, f);
    }
}

pub(super) fn cmd_hdel(out: &mut Owned, key: &[u8], fields: &[&[u8]]) {
    write_array_header(out, 2 + fields.len());
    write_bulk(out, b"HDEL");
    write_bulk(out, key);
    for f in fields {
        write_bulk(out, f);
    }
}

pub(super) fn cmd_hgetall(out: &mut Owned, key: &[u8]) {
    cmd1(out, b"HGETALL", key);
}

pub(super) fn cmd_hlen(out: &mut Owned, key: &[u8]) {
    cmd1(out, b"HLEN", key);
}

pub(super) fn cmd_hexists(out: &mut Owned, key: &[u8], field: &[u8]) {
    cmd2(out, b"HEXISTS", key, field);
}

pub(super) fn cmd_hincrby(out: &mut Owned, key: &[u8], field: &[u8], by: i64) {
    write_array_header(out, 4);
    write_bulk(out, b"HINCRBY");
    write_bulk(out, key);
    write_bulk(out, field);
    write_int_bulk(out, by);
}

pub(super) fn cmd_sadd(out: &mut Owned, key: &[u8], members: &[&[u8]]) {
    write_array_header(out, 2 + members.len());
    write_bulk(out, b"SADD");
    write_bulk(out, key);
    for m in members {
        write_bulk(out, m);
    }
}

pub(super) fn cmd_srem(out: &mut Owned, key: &[u8], members: &[&[u8]]) {
    write_array_header(out, 2 + members.len());
    write_bulk(out, b"SREM");
    write_bulk(out, key);
    for m in members {
        write_bulk(out, m);
    }
}

pub(super) fn cmd_smembers(out: &mut Owned, key: &[u8]) {
    cmd1(out, b"SMEMBERS", key);
}

pub(super) fn cmd_sismember(out: &mut Owned, key: &[u8], member: &[u8]) {
    cmd2(out, b"SISMEMBER", key, member);
}

pub(super) fn cmd_scard(out: &mut Owned, key: &[u8]) {
    cmd1(out, b"SCARD", key);
}

pub(super) fn cmd_zadd(out: &mut Owned, key: &[u8], score: f64, member: &[u8]) {
    write_array_header(out, 4);
    write_bulk(out, b"ZADD");
    write_bulk(out, key);
    write_float_bulk(out, score);
    write_bulk(out, member);
}

pub(super) fn cmd_zrem(out: &mut Owned, key: &[u8], members: &[&[u8]]) {
    write_array_header(out, 2 + members.len());
    write_bulk(out, b"ZREM");
    write_bulk(out, key);
    for m in members {
        write_bulk(out, m);
    }
}

pub(super) fn cmd_zrange(out: &mut Owned, key: &[u8], start: i64, stop: i64) {
    write_array_header(out, 4);
    write_bulk(out, b"ZRANGE");
    write_bulk(out, key);
    write_int_bulk(out, start);
    write_int_bulk(out, stop);
}

pub(super) fn cmd_zrange_with_scores(out: &mut Owned, key: &[u8], start: i64, stop: i64) {
    write_array_header(out, 5);
    write_bulk(out, b"ZRANGE");
    write_bulk(out, key);
    write_int_bulk(out, start);
    write_int_bulk(out, stop);
    write_bulk(out, b"WITHSCORES");
}

pub(super) fn cmd_zrangebyscore(out: &mut Owned, key: &[u8], min: f64, max: f64) {
    write_array_header(out, 4);
    write_bulk(out, b"ZRANGEBYSCORE");
    write_bulk(out, key);
    write_float_bulk(out, min);
    write_float_bulk(out, max);
}

pub(super) fn cmd_zrank(out: &mut Owned, key: &[u8], member: &[u8]) {
    cmd2(out, b"ZRANK", key, member);
}

pub(super) fn cmd_zscore(out: &mut Owned, key: &[u8], member: &[u8]) {
    cmd2(out, b"ZSCORE", key, member);
}

pub(super) fn cmd_zcard(out: &mut Owned, key: &[u8]) {
    cmd1(out, b"ZCARD", key);
}

pub(super) fn cmd_zincrby(out: &mut Owned, key: &[u8], by: f64, member: &[u8]) {
    write_array_header(out, 4);
    write_bulk(out, b"ZINCRBY");
    write_bulk(out, key);
    write_float_bulk(out, by);
    write_bulk(out, member);
}

pub(super) fn cmd_lpush(out: &mut Owned, key: &[u8], values: &[&[u8]]) {
    write_array_header(out, 2 + values.len());
    write_bulk(out, b"LPUSH");
    write_bulk(out, key);
    for v in values {
        write_bulk(out, v);
    }
}

pub(super) fn cmd_rpush(out: &mut Owned, key: &[u8], values: &[&[u8]]) {
    write_array_header(out, 2 + values.len());
    write_bulk(out, b"RPUSH");
    write_bulk(out, key);
    for v in values {
        write_bulk(out, v);
    }
}

pub(super) fn cmd_lpop(out: &mut Owned, key: &[u8]) {
    cmd1(out, b"LPOP", key);
}

pub(super) fn cmd_rpop(out: &mut Owned, key: &[u8]) {
    cmd1(out, b"RPOP", key);
}

pub(super) fn cmd_lrange(out: &mut Owned, key: &[u8], start: i64, stop: i64) {
    write_array_header(out, 4);
    write_bulk(out, b"LRANGE");
    write_bulk(out, key);
    write_int_bulk(out, start);
    write_int_bulk(out, stop);
}

pub(super) fn cmd_llen(out: &mut Owned, key: &[u8]) {
    cmd1(out, b"LLEN", key);
}

pub(super) fn cmd_getset(out: &mut Owned, key: &[u8], value: &[u8]) {
    cmd2(out, b"GETSET", key, value);
}

pub(super) fn cmd_getdel(out: &mut Owned, key: &[u8]) {
    cmd1(out, b"GETDEL", key);
}

pub(super) fn cmd_append(out: &mut Owned, key: &[u8], value: &[u8]) {
    cmd2(out, b"APPEND", key, value);
}

pub(super) fn cmd_strlen(out: &mut Owned, key: &[u8]) {
    cmd1(out, b"STRLEN", key);
}

pub(super) fn cmd_incrbyfloat(out: &mut Owned, key: &[u8], by: f64) {
    write_array_header(out, 3);
    write_bulk(out, b"INCRBYFLOAT");
    write_bulk(out, key);
    write_float_bulk(out, by);
}

pub(super) fn cmd_type(out: &mut Owned, key: &[u8]) {
    cmd1(out, b"TYPE", key);
}

pub(super) fn cmd_rename(out: &mut Owned, src: &[u8], dst: &[u8]) {
    cmd2(out, b"RENAME", src, dst);
}

pub(super) fn cmd_persist(out: &mut Owned, key: &[u8]) {
    cmd1(out, b"PERSIST", key);
}

pub(super) fn cmd_unlink(out: &mut Owned, keys: &[&[u8]]) {
    write_array_header(out, 1 + keys.len());
    write_bulk(out, b"UNLINK");
    for k in keys {
        write_bulk(out, k);
    }
}

pub(super) fn cmd_dbsize(out: &mut Owned) {
    out.extend_from_slice(b"*1\r\n$6\r\nDBSIZE\r\n");
}

pub(super) fn cmd_scan(
    out: &mut Owned,
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
    write_int_bulk(out, cursor as i64);
    if let Some(p) = match_pattern {
        write_bulk(out, b"MATCH");
        write_bulk(out, p);
    }
    if let Some(c) = count {
        write_bulk(out, b"COUNT");
        write_int_bulk(out, c as i64);
    }
}

pub(super) fn cmd_pfadd(out: &mut Owned, key: &[u8], elements: &[&[u8]]) {
    write_array_header(out, 2 + elements.len());
    write_bulk(out, b"PFADD");
    write_bulk(out, key);
    for e in elements {
        write_bulk(out, e);
    }
}

pub(super) fn cmd_pfcount(out: &mut Owned, keys: &[&[u8]]) {
    write_array_header(out, 1 + keys.len());
    write_bulk(out, b"PFCOUNT");
    for k in keys {
        write_bulk(out, k);
    }
}

pub(super) fn cmd_pfmerge(out: &mut Owned, dest: &[u8], sources: &[&[u8]]) {
    write_array_header(out, 2 + sources.len());
    write_bulk(out, b"PFMERGE");
    write_bulk(out, dest);
    for s in sources {
        write_bulk(out, s);
    }
}

pub(super) fn cmd_bitop(out: &mut Owned, op: &[u8], dest: &[u8], sources: &[&[u8]]) {
    write_array_header(out, 3 + sources.len());
    write_bulk(out, b"BITOP");
    write_bulk(out, op);
    write_bulk(out, dest);
    for s in sources {
        write_bulk(out, s);
    }
}

pub(super) fn cmd_geodist(out: &mut Owned, key: &[u8], m1: &[u8], m2: &[u8], unit: Option<&[u8]>) {
    write_array_header(out, 4 + unit.is_some() as usize);
    write_bulk(out, b"GEODIST");
    write_bulk(out, key);
    write_bulk(out, m1);
    write_bulk(out, m2);
    if let Some(u) = unit {
        write_bulk(out, u);
    }
}

pub(super) fn cmd_geopos(out: &mut Owned, key: &[u8], members: &[&[u8]]) {
    write_array_header(out, 2 + members.len());
    write_bulk(out, b"GEOPOS");
    write_bulk(out, key);
    for m in members {
        write_bulk(out, m);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn enc(f: impl FnOnce(&mut Owned)) -> Vec<u8> {
        let mut b = Owned::with_capacity(64);
        f(&mut b);
        b.as_slice().to_vec()
    }

    #[test]
    fn get_basic() {
        assert_eq!(
            enc(|b| cmd_get(b, b"foo")),
            b"*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n",
        );
    }

    #[test]
    fn set_ex() {
        assert_eq!(
            enc(|b| cmd_set_ex(b, b"k", b"v", 60)),
            b"*5\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n$2\r\nEX\r\n$2\r\n60\r\n",
        );
    }

    #[test]
    fn incrby_negative() {
        assert_eq!(
            enc(|b| cmd_incrby(b, b"counter", -42)),
            b"*3\r\n$6\r\nINCRBY\r\n$7\r\ncounter\r\n$3\r\n-42\r\n",
        );
    }

    #[test]
    fn del_multi() {
        assert_eq!(
            enc(|b| cmd_del(b, &[b"a", b"bb", b"ccc"])),
            b"*4\r\n$3\r\nDEL\r\n$1\r\na\r\n$2\r\nbb\r\n$3\r\nccc\r\n",
        );
    }

    #[test]
    fn raw_passes_args_through() {
        assert_eq!(
            enc(|b| cmd_raw(b, &[b"CLUSTER", b"SLOTS"])),
            b"*2\r\n$7\r\nCLUSTER\r\n$5\r\nSLOTS\r\n",
        );
    }

    #[test]
    fn hset_pairs_two_fields() {
        assert_eq!(
            enc(|b| cmd_hset_pairs(b, b"u:1", &[(b"name", b"alice"), (b"age", b"30")])),
            b"*6\r\n$4\r\nHSET\r\n$3\r\nu:1\r\n$4\r\nname\r\n$5\r\nalice\r\n$3\r\nage\r\n$2\r\n30\r\n",
        );
    }

    #[test]
    fn hmget_multi_fields() {
        assert_eq!(
            enc(|b| cmd_hmget(b, b"u:1", &[b"name", b"age"])),
            b"*4\r\n$5\r\nHMGET\r\n$3\r\nu:1\r\n$4\r\nname\r\n$3\r\nage\r\n",
        );
    }

    #[test]
    fn hincrby_negative() {
        assert_eq!(
            enc(|b| cmd_hincrby(b, b"counters", b"a", -5)),
            b"*4\r\n$7\r\nHINCRBY\r\n$8\r\ncounters\r\n$1\r\na\r\n$2\r\n-5\r\n",
        );
    }

    #[test]
    fn sadd_multi_members() {
        assert_eq!(
            enc(|b| cmd_sadd(b, b"tags", &[b"a", b"b"])),
            b"*4\r\n$4\r\nSADD\r\n$4\r\ntags\r\n$1\r\na\r\n$1\r\nb\r\n",
        );
    }

    #[test]
    fn zadd_with_float_score() {
        assert_eq!(
            enc(|b| cmd_zadd(b, b"lb", 3.5, b"alice")),
            b"*4\r\n$4\r\nZADD\r\n$2\r\nlb\r\n$3\r\n3.5\r\n$5\r\nalice\r\n",
        );
    }

    #[test]
    fn zrange_with_scores_appends_flag() {
        assert_eq!(
            enc(|b| cmd_zrange_with_scores(b, b"lb", 0, -1)),
            b"*5\r\n$6\r\nZRANGE\r\n$2\r\nlb\r\n$1\r\n0\r\n$2\r\n-1\r\n$10\r\nWITHSCORES\r\n",
        );
    }

    #[test]
    fn lpush_multi_values() {
        assert_eq!(
            enc(|b| cmd_lpush(b, b"q", &[b"x", b"y"])),
            b"*4\r\n$5\r\nLPUSH\r\n$1\r\nq\r\n$1\r\nx\r\n$1\r\ny\r\n",
        );
    }

    #[test]
    fn lrange_full() {
        assert_eq!(
            enc(|b| cmd_lrange(b, b"q", 0, -1)),
            b"*4\r\n$6\r\nLRANGE\r\n$1\r\nq\r\n$1\r\n0\r\n$2\r\n-1\r\n",
        );
    }

    #[test]
    fn scan_with_match_and_count() {
        assert_eq!(
            enc(|b| cmd_scan(b, 0, Some(b"user:*"), Some(100))),
            b"*6\r\n$4\r\nSCAN\r\n$1\r\n0\r\n$5\r\nMATCH\r\n$6\r\nuser:*\r\n$5\r\nCOUNT\r\n$3\r\n100\r\n",
        );
    }

    #[test]
    fn scan_minimal() {
        assert_eq!(
            enc(|b| cmd_scan(b, 42, None, None)),
            b"*2\r\n$4\r\nSCAN\r\n$2\r\n42\r\n",
        );
    }

    #[test]
    fn type_and_rename() {
        assert_eq!(
            enc(|b| cmd_type(b, b"k")),
            b"*2\r\n$4\r\nTYPE\r\n$1\r\nk\r\n",
        );
        assert_eq!(
            enc(|b| cmd_rename(b, b"a", b"b")),
            b"*3\r\n$6\r\nRENAME\r\n$1\r\na\r\n$1\r\nb\r\n",
        );
    }
}

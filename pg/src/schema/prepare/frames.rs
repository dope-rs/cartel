use crate::{protocol::encode, schema};

pub(super) struct Lengths {
    pub(super) parse: i32,
    pub(super) describe: i32,
    pub(super) wire: usize,
}

pub(super) const fn add(
    scope: encode::Scope,
    current: usize,
    added: usize,
) -> Result<usize, encode::Error> {
    match current.checked_add(added) {
        Some(total) => Ok(total),
        None => Err(encode::Error::LengthOverflow {
            scope,
            current,
            additional: added,
        }),
    }
}

const fn parse_message_len(name: &str, sql: &str, oids: &[u32]) -> Result<usize, encode::Error> {
    // QueryMeta construction admits both strings and the i16 OID count;
    // the sole non-metadata caller uses the fixed, parameterless ltree probe.
    let bytes = oids.len() * 4;
    let mut len = 4usize;
    let name = match add(encode::Scope::Frame, name.len(), 1) {
        Ok(name) => name,
        Err(error) => return Err(error),
    };
    let sql = match add(encode::Scope::Frame, sql.len(), 1) {
        Ok(sql) => sql,
        Err(error) => return Err(error),
    };
    len = match add(encode::Scope::Frame, len, name) {
        Ok(len) => len,
        Err(error) => return Err(error),
    };
    len = match add(encode::Scope::Frame, len, sql) {
        Ok(len) => len,
        Err(error) => return Err(error),
    };
    len = match add(encode::Scope::Frame, len, 2) {
        Ok(len) => len,
        Err(error) => return Err(error),
    };
    len = match add(encode::Scope::Frame, len, bytes) {
        Ok(len) => len,
        Err(error) => return Err(error),
    };
    if len > i32::MAX as usize {
        return Err(encode::Error::FrameLength { actual: len });
    }
    Ok(len)
}

pub(super) const fn statement_lengths(
    name: &str,
    sql: &str,
    oids: &[u32],
) -> Result<Lengths, encode::Error> {
    let parse = match parse_message_len(name, sql, oids) {
        Ok(parse) => parse,
        Err(error) => return Err(error),
    };
    // Parse already admits the name and n + sql.len() + 4 * oids.len() + 8 <= i32::MAX.
    // Describe is n + 6 <= parse - 2, so both frames plus tags fit 2 * i32::MAX.
    let describe = name.len() + 6;
    Ok(Lengths {
        parse: parse as i32,
        describe: describe as i32,
        wire: parse + describe + 2,
    })
}

pub(super) const fn write_slice<const N: usize>(out: &mut [u8; N], at: &mut usize, bytes: &[u8]) {
    let mut index = 0;
    while index < bytes.len() {
        out[*at] = bytes[index];
        *at += 1;
        index += 1;
    }
}

pub(super) const fn write_statement<const N: usize>(
    out: &mut [u8; N],
    at: &mut usize,
    name: &str,
    sql: &str,
    oids: &[u32],
    lengths: Lengths,
) {
    out[*at] = b'P';
    *at += 1;
    write_slice(out, at, &lengths.parse.to_be_bytes());
    write_slice(out, at, name.as_bytes());
    out[*at] = 0;
    *at += 1;
    write_slice(out, at, sql.as_bytes());
    out[*at] = 0;
    *at += 1;
    write_slice(out, at, &(oids.len() as i16).to_be_bytes());
    let mut oid_i = 0;
    while oid_i < oids.len() {
        write_slice(out, at, &oids[oid_i].to_be_bytes());
        oid_i += 1;
    }
    out[*at] = b'D';
    *at += 1;
    write_slice(out, at, &lengths.describe.to_be_bytes());
    out[*at] = b'S';
    *at += 1;
    write_slice(out, at, name.as_bytes());
    out[*at] = 0;
    *at += 1;
}

pub(super) const fn write_query<const N: usize>(
    out: &mut [u8; N],
    at: &mut usize,
    query: &schema::QueryMeta<'_>,
    lengths: Lengths,
) {
    write_statement(out, at, query.name, query.sql, query.param_oids, lengths)
}

use crate::Error;

fn take_cstr(buf: &[u8]) -> Result<(&str, &[u8]), Error> {
    let pos = buf
        .iter()
        .position(|&b| b == 0)
        .ok_or(Error::Protocol("missing cstring terminator"))?;
    let s = std::str::from_utf8(&buf[..pos])
        .map_err(|_| Error::Protocol("invalid utf-8 in cstring"))?;
    Ok((s, &buf[pos + 1..]))
}

pub(super) enum AuthRequest<'a> {
    Ok,
    Sasl { mechanisms: Vec<&'a str> },
    SaslContinue { data: &'a [u8] },
    SaslFinal { data: &'a [u8] },
    Other(u32),
}

pub(super) fn parse_auth(payload: &[u8]) -> Result<AuthRequest<'_>, Error> {
    if payload.len() < 4 {
        return Err(Error::Protocol("auth frame truncated"));
    }
    let kind = u32::from_be_bytes(payload[0..4].try_into().unwrap());
    let rest = &payload[4..];
    match kind {
        crate::wire::Auth::OK => Ok(AuthRequest::Ok),
        crate::wire::Auth::SASL => {
            let mut mechs = Vec::new();
            let mut cur = rest;
            loop {
                if cur.is_empty() || cur[0] == 0 {
                    break;
                }
                let (m, next) = take_cstr(cur)?;
                mechs.push(m);
                cur = next;
            }
            Ok(AuthRequest::Sasl { mechanisms: mechs })
        }
        crate::wire::Auth::SASL_CONTINUE => Ok(AuthRequest::SaslContinue { data: rest }),
        crate::wire::Auth::SASL_FINAL => Ok(AuthRequest::SaslFinal { data: rest }),
        n => Ok(AuthRequest::Other(n)),
    }
}

pub(super) fn parse_db_error(payload: &[u8]) -> crate::DbError {
    let mut err = crate::DbError::default();
    let mut cur = payload;
    while !cur.is_empty() {
        let field_type = cur[0];
        if field_type == 0 {
            break;
        }
        cur = &cur[1..];
        let Ok((value, rest)) = take_cstr(cur) else {
            break;
        };
        match field_type {
            b'V' | b'S' if err.severity.is_empty() => err.severity = value.to_string(),
            b'C' => err.code = value.to_string(),
            b'M' => err.message = value.to_string(),
            b'D' => err.detail = Some(value.to_string()),
            b'H' => err.hint = Some(value.to_string()),
            b'P' => err.position = value.parse().ok(),
            b's' => err.schema = Some(value.to_string()),
            b't' => err.table = Some(value.to_string()),
            b'c' => err.column = Some(value.to_string()),
            b'n' => err.constraint = Some(value.to_string()),
            _ => {}
        }
        cur = rest;
    }
    err
}

pub(super) fn parse_notification(payload: &[u8]) -> Option<crate::Notification> {
    if payload.len() < 4 {
        return None;
    }
    let pid = u32::from_be_bytes(payload[0..4].try_into().unwrap());
    let (channel, rest) = take_cstr(&payload[4..]).ok()?;
    let (msg, _) = take_cstr(rest).ok()?;
    Some(crate::Notification {
        pid,
        channel: channel.to_string(),
        payload: msg.to_string(),
    })
}

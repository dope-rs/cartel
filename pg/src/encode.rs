use crate::wire::{Fe, Sink};

fn reserve_len<S: Sink>(out: &mut S) -> usize {
    let pos = out.len();
    out.extend_from_slice(&[0u8; 4]);
    pos
}

fn finish_len<S: Sink>(out: &mut S, pos: usize) {
    let len = out.len();
    if pos + 4 > len {
        return;
    }
    let total = (len - pos) as u32;
    let bytes = total.to_be_bytes();
    out.as_mut_slice()[pos..pos + 4].copy_from_slice(&bytes);
}

fn write_cstr<S: Sink>(out: &mut S, s: &str) {
    out.extend_from_slice(s.as_bytes());
    out.push(0);
}

pub(super) fn startup<S: Sink>(
    out: &mut S,
    user: &str,
    database: &str,
    application_name: &str,
    options: &str,
    statement_timeout_ms: u32,
) {
    let pos = reserve_len(out);
    out.extend_from_slice(&Fe::STARTUP_PROTOCOL.to_be_bytes());

    write_cstr(out, "user");
    write_cstr(out, user);
    write_cstr(out, "database");
    write_cstr(out, database);
    write_cstr(out, "application_name");
    write_cstr(out, application_name);
    write_cstr(out, "client_encoding");
    write_cstr(out, "UTF8");
    if statement_timeout_ms > 0 && !options.contains("statement_timeout") {
        write_cstr(out, "statement_timeout");
        write_cstr(out, &statement_timeout_ms.to_string());
    }
    if !options.is_empty() {
        write_cstr(out, "options");
        write_cstr(out, options);
    }
    out.push(0);

    finish_len(out, pos);
}

pub(super) fn cancel_request<S: Sink>(out: &mut S, pid: i32, secret: i32) {
    out.extend_from_slice(&16u32.to_be_bytes());
    out.extend_from_slice(&Fe::CANCEL_REQUEST.to_be_bytes());
    out.extend_from_slice(&pid.to_be_bytes());
    out.extend_from_slice(&secret.to_be_bytes());
}

pub(super) fn sync<S: Sink>(out: &mut S) {
    out.push(Fe::SYNC);
    out.extend_from_slice(&4u32.to_be_bytes());
}

pub(super) fn copy_data<S: Sink>(out: &mut S, data: &[u8]) {
    out.push(Fe::COPY_DATA);
    let total_len = (4 + data.len()) as u32;
    out.extend_from_slice(&total_len.to_be_bytes());
    out.extend_from_slice(data);
}

pub(super) fn copy_done<S: Sink>(out: &mut S) {
    out.push(Fe::COPY_DONE);
    out.extend_from_slice(&4u32.to_be_bytes());
}

pub(super) fn sasl_initial_response<S: Sink>(out: &mut S, mechanism: &str, initial: &[u8]) {
    out.push(Fe::PASSWORD);
    let pos = reserve_len(out);
    write_cstr(out, mechanism);
    out.extend_from_slice(&(initial.len() as i32).to_be_bytes());
    out.extend_from_slice(initial);
    finish_len(out, pos);
}

pub(super) fn sasl_response<S: Sink>(out: &mut S, msg: &[u8]) {
    out.push(Fe::PASSWORD);
    let pos = reserve_len(out);
    out.extend_from_slice(msg);
    finish_len(out, pos);
}

pub(super) fn parse<S: Sink>(out: &mut S, stmt_name: &str, sql: &str, param_oids: &[u32]) {
    out.push(Fe::PARSE);
    let pos = reserve_len(out);
    write_cstr(out, stmt_name);
    write_cstr(out, sql);
    out.extend_from_slice(&(param_oids.len() as u16).to_be_bytes());
    for oid in param_oids {
        out.extend_from_slice(&oid.to_be_bytes());
    }
    finish_len(out, pos);
}

pub(super) fn bind_header<S: Sink>(
    out: &mut S,
    portal: &str,
    stmt_name: &str,
    param_formats: &[u16],
    n_params: u16,
) -> usize {
    out.push(Fe::BIND);
    let pos = reserve_len(out);
    write_cstr(out, portal);
    write_cstr(out, stmt_name);

    out.extend_from_slice(&(param_formats.len() as u16).to_be_bytes());
    for f in param_formats {
        out.extend_from_slice(&f.to_be_bytes());
    }

    out.extend_from_slice(&n_params.to_be_bytes());
    pos
}

pub(super) fn bind_trailer<S: Sink>(out: &mut S, pos: usize, result_formats: &[u16]) {
    out.extend_from_slice(&(result_formats.len() as u16).to_be_bytes());
    for f in result_formats {
        out.extend_from_slice(&f.to_be_bytes());
    }
    finish_len(out, pos);
}

pub(super) fn execute<S: Sink>(out: &mut S) {
    const MSG: [u8; 10] = [Fe::EXECUTE, 0, 0, 0, 9, 0, 0, 0, 0, 0];
    out.extend_from_slice(&MSG);
}

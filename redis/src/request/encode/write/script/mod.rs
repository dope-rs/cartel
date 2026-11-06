use crate::request::encode;

/// Measures an EVAL request from payload lengths, checking both declared counts.
pub fn wire_len(
    script: &[u8],
    key_count: usize,
    key_lengths: impl Iterator<Item = usize>,
    argument_count: usize,
    argument_lengths: impl Iterator<Item = usize>,
) -> Result<usize, crate::request::Error> {
    let length = start(script, key_count, argument_count)?;
    let length = lengths(length, key_count, key_lengths)?;
    lengths(length, argument_count, argument_lengths).map(encode::Length::get)
}

pub(crate) fn length<K, A>(
    script: &[u8],
    keys: &[K],
    args: &[A],
) -> Result<encode::Length, crate::request::Error>
where
    K: AsRef<[u8]>,
    A: crate::request::Bulk,
{
    let mut length = start(script, keys.len(), args.len())?;
    for key in keys {
        length = length.bulk(key.as_ref().len())?;
    }
    for argument in args {
        length = length.bulk(argument.len())?;
    }
    Ok(length)
}

pub(crate) fn eval<K, A>(
    mut out: encode::Writer,
    script: &[u8],
    keys: &[K],
    args: &[A],
) -> Result<encode::Complete, crate::request::Error>
where
    K: AsRef<[u8]>,
    A: crate::request::Bulk,
{
    let count = count(keys.len(), args.len())?;
    let mut wire = core::fmt::NumBuffer::new();
    let arity = (keys.len() as u64).format_into(&mut wire).as_bytes();
    out.array(count)?;
    out.bulk(b"EVAL")?;
    out.bulk(script)?;
    out.bulk(arity)?;
    for key in keys {
        out.bulk(key.as_ref())?;
    }
    for argument in args {
        out.bulk(argument)?;
    }
    out.complete()
}

#[inline(always)]
fn start(script: &[u8], keys: usize, args: usize) -> Result<encode::Length, crate::request::Error> {
    encode::Length::array(count(keys, args)?)?
        .bulk(b"EVAL".len())?
        .bulk(script.len())?
        .bulk(encode::Number(keys as u64).digits())
}

fn lengths(
    mut length: encode::Length,
    declared: usize,
    sizes: impl Iterator<Item = usize>,
) -> Result<encode::Length, crate::request::Error> {
    let mut actual = 0;
    for size in sizes {
        actual = encode::next_item(declared, actual)?;
        length = length.bulk(size)?;
    }
    encode::check_items(declared, actual)?;
    Ok(length)
}

fn count(keys: usize, args: usize) -> Result<usize, crate::request::Error> {
    let variable = encode::add_count(keys, args)?;
    encode::add_count(3, variable)
}

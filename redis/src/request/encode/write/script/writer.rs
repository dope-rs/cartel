use crate::request::encode;

#[repr(transparent)]
pub(crate) struct Writer(encode::Writer);

impl Writer {
    pub(crate) const fn new(out: encode::Writer) -> Self {
        Self(out)
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
        let count = count(keys.len(), args.len())?;
        let mut wire = [0; 20];
        let arity = encode::format_uint_into(&mut wire, keys.len() as u64);
        let mut length = encode::Length::array(count)?
            .bulk(b"EVAL")?
            .bulk(script)?
            .bulk(arity)?;
        for key in keys {
            length = length.bulk(key.as_ref())?;
        }
        for argument in args {
            length = length.bulk(argument)?;
        }
        Ok(length)
    }

    pub(crate) fn eval<K, A>(
        self,
        script: &[u8],
        keys: &[K],
        args: &[A],
    ) -> Result<encode::Complete, crate::request::Error>
    where
        K: AsRef<[u8]>,
        A: crate::request::Bulk,
    {
        let count = count(keys.len(), args.len())?;
        let mut wire = [0; 20];
        let arity = encode::format_uint_into(&mut wire, keys.len() as u64);
        let mut out = self.0;
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
}

fn count(keys: usize, args: usize) -> Result<usize, crate::request::Error> {
    let variable = keys.checked_add(args).ok_or(crate::request::Error::Add {
        left: keys,
        right: args,
    })?;
    encode::add_count(3, variable)
}

const _: () = assert!(std::mem::size_of::<Writer>() == std::mem::size_of::<encode::Writer>());

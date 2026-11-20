mod bind;
mod error;

pub use error::Error;

#[doc(hidden)]
pub const fn format_codes<const PARAMETERS: bool>(values: &'static [u16]) -> &'static [u16] {
    let [first, ..] = values else {
        return if PARAMETERS { &[] } else { &[1] };
    };
    let mut index = 1;
    while index < values.len() {
        if values[index] != *first {
            return values;
        }
        index += 1;
    }
    match *first {
        1 => &[1],
        0 if PARAMETERS => &[],
        _ => values,
    }
}

#[doc(hidden)]
pub const fn result_formats<const N: usize>(shape: &crate::schema::desc::Formats) -> [u16; N] {
    use crate::schema::desc::Formats;

    const fn write(shape: &Formats, out: &mut [u16], at: &mut usize) {
        match shape {
            Formats::Value(code) => {
                if *at < out.len() {
                    out[*at] = *code;
                    *at += 1;
                }
            }
            Formats::Row(fields) => {
                let mut index = 0;
                while index < fields.len() {
                    write(&fields[index], out, at);
                    index += 1;
                }
            }
        }
    }
    // Query admission reports a custom declaration with too many or too few fields.
    let mut codes = [1; N];
    let mut at = 0;
    write(shape, &mut codes, &mut at);
    codes
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Scope {
    Frame,
    Preparation,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Position {
    pub group: u32,
    pub query: u32,
}

use cartel_core::frame;
use o3::buffer::{
    pool,
    write::{self, Sink as _, SpareExt as _, edit::Edit},
};

use crate::protocol::messages;

pub(crate) const SYNC: &[u8; 5] = &[messages::Fe::SYNC, 0, 0, 0, 4];
const EXECUTE: &[u8; 10] = &[messages::Fe::EXECUTE, 0, 0, 0, 9, 0, 0, 0, 0, 0];
pub(crate) const COPY_DONE_SYNC: &[u8; 10] = &[
    messages::Fe::COPY_DONE,
    0,
    0,
    0,
    4,
    SYNC[0],
    SYNC[1],
    SYNC[2],
    SYNC[3],
    SYNC[4],
];

fn wire_length(value: usize) -> Result<i32, Error> {
    const MAXIMUM: usize = i32::MAX as usize;
    if value > MAXIMUM {
        Err(Error::FrameLength { actual: value })
    } else {
        Ok(value as i32)
    }
}

fn write_cstr(out: &mut Writer<'_>, value: &str) -> Result<(), Error> {
    out.write_slices([value.as_bytes(), &[0]])
}

fn write_search_path(out: &mut Writer<'_>, schema: &str) -> Result<(), Error> {
    out.write_byte(b'"')?;
    let mut parts = schema.split('"');
    if let Some(first) = parts.next() {
        out.write_slice(first.as_bytes())?;
    }
    for part in parts.by_ref() {
        out.write_slice(b"\"\"")?;
        out.write_slice(part.as_bytes())?;
    }
    out.write_slice(b"\",public")?;
    out.write_byte(0)
}

#[repr(transparent)]
#[doc(hidden)]
pub struct Writer<'a>(write::Spare<'a>);

impl write::Sink for Writer<'_> {
    type Error = Error;

    fn write_slices<const N: usize>(&mut self, slices: [&[u8]; N]) -> Result<(), Error> {
        self.0.write_slices(slices).map_err(Error::capacity)
    }
}

impl Writer<'_> {
    #[inline]
    pub(in crate::protocol) fn write_words(&mut self, words: &[[u8; 4]]) -> Result<(), Error> {
        self.0.write_slice(words.as_flattened()).map_err(|error| {
            let (_, room) = error.into_parts();
            Error::Capacity {
                required: std::num::NonZero::<usize>::MIN.saturating_add(3),
                capacity: room % 4,
            }
        })
    }

    /// Commits initialized partial writes before the frame's secret clearing.
    pub(crate) fn with<T>(
        span: &mut pool::Span,
        write: impl FnOnce(&mut Writer<'_>) -> Result<T, Error>,
    ) -> Result<T, Error> {
        let mut writer = Writer(span.spare());
        let result = write(&mut writer);
        writer.0.commit();
        result
    }

    /// The retained owner bounds the prefix and its appended suffix together.
    pub(in crate::protocol) fn patch<const HEADER: bool, T>(
        &mut self,
        write: impl FnOnce(&mut Writer<'_>) -> Result<T, Error>,
    ) -> Result<T, Error> {
        let mut edit = Edit::try_new(self.0.spare(), &[0; 4]).map_err(Error::capacity)?;
        let mut writer = Writer(edit.spare());
        let result = write(&mut writer).and_then(|value| {
            let length = writer.0.len();
            let length = if HEADER { length + 4 } else { length };
            Ok((value, wire_length(length)?))
        });
        let (value, length) = result.inspect_err(|_| {
            frame::Sensitivity::Secret.clear(writer.0.as_mut_slice());
        })?;
        writer.0.commit();
        *edit.prefix() = length.to_be_bytes();
        edit.commit();
        Ok(value)
    }

    pub(crate) fn startup(
        &mut self,
        user: &str,
        database: &str,
        app: &str,
        schema: Option<&str>,
        timeout: u32,
    ) -> Result<(), Error> {
        self.patch::<true, _>(|out| {
            out.write_slice(&messages::Fe::STARTUP_PROTOCOL.to_be_bytes())?;
            write_cstr(out, "user")?;
            write_cstr(out, user)?;
            write_cstr(out, "database")?;
            write_cstr(out, database)?;
            write_cstr(out, "application_name")?;
            write_cstr(out, app)?;
            write_cstr(out, "client_encoding")?;
            write_cstr(out, "UTF8")?;
            if timeout > 0 {
                write_cstr(out, "statement_timeout")?;
                out.write_u32(timeout)?;
                out.write_byte(0)?;
            }
            if let Some(schema) = schema {
                write_cstr(out, "search_path")?;
                write_search_path(out, schema)?;
            }
            out.write_byte(0)
        })
    }

    pub(crate) fn sync(&mut self) -> Result<(), Error> {
        self.write_slices([&SYNC[..1], &SYNC[1..]])
    }

    pub(crate) fn write_u32(&mut self, value: u32) -> Result<(), Error> {
        let mut digits = core::fmt::NumBuffer::new();
        self.write_slice(value.format_into(&mut digits).as_bytes())
    }

    pub(crate) fn copy_data(&mut self, data: &[u8]) -> Result<(), Error> {
        let length = data.len() + 4;
        self.write_byte(messages::Fe::COPY_DATA)?;
        self.write_slice(&wire_length(length)?.to_be_bytes())?;
        self.write_slice(data)
    }

    pub(crate) fn copy_done(&mut self) -> Result<(), Error> {
        self.write_slices([&COPY_DONE_SYNC[..1], &COPY_DONE_SYNC[1..5]])
    }

    pub(crate) fn sasl_first(
        &mut self,
        name: &str,
        write: impl FnOnce(&mut Writer<'_>) -> Result<(), Error>,
    ) -> Result<(), Error> {
        self.write_byte(messages::Fe::PASSWORD)?;
        self.patch::<true, _>(|out| {
            write_cstr(out, name)?;
            out.patch::<false, _>(write)
        })
    }

    pub(crate) fn sasl_final<T>(
        &mut self,
        write: impl FnOnce(&mut Writer<'_>) -> Result<T, Error>,
    ) -> Result<T, Error> {
        self.write_byte(messages::Fe::PASSWORD)?;
        self.patch::<true, _>(write)
    }

    fn run(
        &mut self,
        prefix: &bind::Prefix<'_>,
        results: bind::Codes<'_>,
        write: impl FnOnce(&mut Writer<'_>) -> Result<(), Error>,
    ) -> Result<(), Error> {
        self.write_byte(messages::Fe::BIND)?;
        self.patch::<true, _>(|out| {
            prefix.write(&mut out.0)?;
            write(out)?;
            results.write(&mut out.0)
        })?;
        self.write_slice(EXECUTE)
    }

    pub(crate) fn unnamed(
        &mut self,
        write: impl FnOnce(&mut Writer<'_>) -> Result<(), Error>,
    ) -> Result<(), Error> {
        self.write_byte(messages::Fe::PARSE)?;
        self.patch::<true, _>(|out| {
            out.write_byte(0)?;
            write(out)?;
            out.write_byte(0)?;
            out.write_slice(&0i16.to_be_bytes())
        })?;
        self.run(
            &bind::Prefix { name: "", formats: bind::Codes(&[1]), params: 0 },
            bind::Codes(&[1]),
            |_| Ok(()),
        )
    }
}

const _: () = assert!(size_of::<Writer<'_>>() == size_of::<write::Spare<'_>>());
const _: () = assert!(size_of::<Writer<'_>>() <= size_of::<frame::Buffer>());
const _: () = assert!(size_of::<Position>() == size_of::<u64>());

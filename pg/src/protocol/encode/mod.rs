mod error;

pub use error::Error;

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
    out.push(b'"')?;
    let mut parts = schema.split('"');
    if let Some(first) = parts.next() {
        out.extend(first.as_bytes())?;
    }
    for part in parts.by_ref() {
        out.extend(b"\"\"")?;
        out.extend(part.as_bytes())?;
    }
    out.extend(b"\",public")?;
    out.push(0)
}

#[repr(transparent)]
pub(crate) struct Writer<'a>(write::Spare<'a>);

impl Writer<'_> {
    /// Commits initialized partial writes before the frame's secret clearing.
    pub(crate) fn encode<T>(
        mut cursor: pool::Cursor,
        kind: frame::Sensitivity,
        write: impl FnOnce(&mut Writer<'_>) -> Result<T, Error>,
    ) -> Result<(frame::Buffer, T), Error> {
        let mut writer = Writer(cursor.spare());
        let result = write(&mut writer);
        writer.0.commit();
        let frame = frame::Buffer::new(cursor, kind);
        Ok((frame, result?))
    }

    pub(crate) fn push(&mut self, byte: u8) -> Result<(), Error> {
        self.0.write_byte(byte).map_err(Error::capacity)
    }

    pub(crate) fn extend(&mut self, bytes: &[u8]) -> Result<(), Error> {
        self.0.write_slice(bytes).map_err(Error::capacity)
    }

    pub(in crate::protocol) fn write_slices<const N: usize>(
        &mut self,
        slices: [&[u8]; N],
    ) -> Result<(), Error> {
        self.0.write_slices(slices).map_err(Error::capacity)
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
            out.extend(&messages::Fe::STARTUP_PROTOCOL.to_be_bytes())?;
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
                out.push(0)?;
            }
            if let Some(schema) = schema {
                write_cstr(out, "search_path")?;
                write_search_path(out, schema)?;
            }
            out.push(0)
        })
    }

    pub(crate) fn sync(&mut self) -> Result<(), Error> {
        self.write_slices([&SYNC[..1], &SYNC[1..]])
    }

    pub(crate) fn write_u32(&mut self, value: u32) -> Result<(), Error> {
        let mut digits = core::fmt::NumBuffer::new();
        self.extend(value.format_into(&mut digits).as_bytes())
    }

    pub(crate) fn copy_data(&mut self, data: &[u8]) -> Result<(), Error> {
        let length = data.len() + 4;
        self.push(messages::Fe::COPY_DATA)?;
        self.extend(&wire_length(length)?.to_be_bytes())?;
        self.extend(data)
    }

    pub(crate) fn copy_done(&mut self) -> Result<(), Error> {
        self.write_slices([&COPY_DONE_SYNC[..1], &COPY_DONE_SYNC[1..5]])
    }

    pub(crate) fn sasl_first(
        &mut self,
        name: &str,
        write: impl FnOnce(&mut Writer<'_>) -> Result<(), Error>,
    ) -> Result<(), Error> {
        self.push(messages::Fe::PASSWORD)?;
        self.patch::<true, _>(|out| {
            write_cstr(out, name)?;
            out.patch::<false, _>(write)
        })
    }

    pub(crate) fn sasl_final<T>(
        &mut self,
        write: impl FnOnce(&mut Writer<'_>) -> Result<T, Error>,
    ) -> Result<T, Error> {
        self.push(messages::Fe::PASSWORD)?;
        self.patch::<true, _>(write)
    }

    pub(crate) fn parse_with(
        &mut self,
        write: impl FnOnce(&mut Writer<'_>) -> Result<(), Error>,
    ) -> Result<(), Error> {
        self.push(messages::Fe::PARSE)?;
        self.patch::<true, _>(|out| {
            out.push(0)?;
            write(out)?;
            out.push(0)?;
            out.extend(&0i16.to_be_bytes())
        })
    }

    pub(crate) fn bind<T>(
        &mut self,
        name: &str,
        formats: &[u16],
        params: i16,
        results: &[u16],
        write: impl FnOnce(&mut Writer<'_>) -> Result<T, Error>,
    ) -> Result<T, Error> {
        self.push(messages::Fe::BIND)?;
        self.patch::<true, _>(|out| {
            out.push(0)?;
            write_cstr(out, name)?;
            out.write_formats(formats)?;
            out.extend(&params.to_be_bytes())?;
            let value = write(out)?;
            out.write_formats(results)?;
            Ok(value)
        })
    }

    fn write_formats(&mut self, formats: &[u16]) -> Result<(), Error> {
        // Bind callers retain query admission or supply the fixed singleton format.
        self.extend(&(formats.len() as i16).to_be_bytes())?;
        for format in formats {
            self.extend(&format.to_be_bytes())?;
        }
        Ok(())
    }

    pub(crate) fn execute(&mut self) -> Result<(), Error> {
        const MESSAGE: [u8; 10] = [messages::Fe::EXECUTE, 0, 0, 0, 9, 0, 0, 0, 0, 0];
        self.extend(&MESSAGE)
    }
}

const _: () = assert!(size_of::<Writer<'_>>() == size_of::<write::Spare<'_>>());
const _: () = assert!(size_of::<Writer<'_>>() <= size_of::<frame::Buffer>());
const _: () = assert!(size_of::<Position>() == size_of::<u64>());

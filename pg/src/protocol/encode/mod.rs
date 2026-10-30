mod error;
pub(super) mod number;

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

enum Count16 {
    ParseTypes,
    ParameterFormats,
    ResultFormats,
}

fn wire_length(value: usize) -> Result<number::Length, Error> {
    const MAXIMUM: usize = i32::MAX as usize;
    if value > MAXIMUM {
        Err(Error::FrameLength { actual: value })
    } else {
        Ok(number::Length(value as i32))
    }
}

fn count16(value: usize, field: Count16) -> Result<number::Count16, Error> {
    const MAXIMUM: usize = i16::MAX as usize;
    if value > MAXIMUM {
        Err(match field {
            Count16::ParseTypes => Error::ParseTypes { actual: value },
            Count16::ParameterFormats => Error::ParameterFormats { actual: value },
            Count16::ResultFormats => Error::ResultFormats { actual: value },
        })
    } else {
        Ok(number::Count16(value as i16))
    }
}

fn write_cstr(out: &mut Writer<'_>, value: &str) -> Result<(), Error> {
    out.extend(value.as_bytes())?;
    out.push(0)
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
        *edit.prefix() = length.0.to_be_bytes();
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
        self.write_slices([&[messages::Fe::SYNC], &4i32.to_be_bytes()])
    }

    pub(crate) fn write_u32(&mut self, mut value: u32) -> Result<(), Error> {
        let mut digits = [0; 10];
        let mut start = digits.len();
        loop {
            start -= 1;
            digits[start] = b'0' + (value % 10) as u8;
            value /= 10;
            if value == 0 {
                break;
            }
        }
        self.extend(&digits[start..])
    }

    pub(crate) fn copy_data(&mut self, data: &[u8]) -> Result<(), Error> {
        let length = data.len() + 4;
        self.push(messages::Fe::COPY_DATA)?;
        self.extend(&wire_length(length)?.0.to_be_bytes())?;
        self.extend(data)
    }

    pub(crate) fn copy_done(&mut self) -> Result<(), Error> {
        self.write_slices([&[messages::Fe::COPY_DONE], &4i32.to_be_bytes()])
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
        name: &str,
        write: impl FnOnce(&mut Writer<'_>) -> Result<(), Error>,
        oids: &[u32],
    ) -> Result<(), Error> {
        let count = count16(oids.len(), Count16::ParseTypes)?;
        self.push(messages::Fe::PARSE)?;
        self.patch::<true, _>(|out| {
            write_cstr(out, name)?;
            write(out)?;
            out.push(0)?;
            out.extend(&count.0.to_be_bytes())?;
            for oid in oids {
                out.extend(&oid.to_be_bytes())?;
            }
            Ok(())
        })
    }

    pub(crate) fn bind<T>(
        &mut self,
        portal: &str,
        name: &str,
        formats: &[u16],
        params: i16,
        results: &[u16],
        write: impl FnOnce(&mut Writer<'_>) -> Result<T, Error>,
    ) -> Result<T, Error> {
        let format_n = count16(formats.len(), Count16::ParameterFormats)?;
        self.push(messages::Fe::BIND)?;
        self.patch::<true, _>(|out| {
            write_cstr(out, portal)?;
            write_cstr(out, name)?;
            out.extend(&format_n.0.to_be_bytes())?;
            for format in formats {
                out.extend(&format.to_be_bytes())?;
            }
            out.extend(&params.to_be_bytes())?;
            let value = write(out)?;
            let count = count16(results.len(), Count16::ResultFormats)?;
            out.extend(&count.0.to_be_bytes())?;
            for format in results {
                out.extend(&format.to_be_bytes())?;
            }
            Ok(value)
        })
    }

    pub(crate) fn execute(&mut self) -> Result<(), Error> {
        const MESSAGE: [u8; 10] = [messages::Fe::EXECUTE, 0, 0, 0, 9, 0, 0, 0, 0, 0];
        self.extend(&MESSAGE)
    }
}

const _: () = assert!(size_of::<Writer<'_>>() == size_of::<write::Spare<'_>>());
const _: () = assert!(size_of::<Writer<'_>>() <= size_of::<frame::Buffer>());
const _: () = assert!(size_of::<Position>() == size_of::<u64>());

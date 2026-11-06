use crate::{
    protocol::{Length, Reason, Wire},
    schema::array,
};
use std::{convert::identity, ops};

pub mod checked;

pub struct RowReader<'a> {
    buf: Wire<'a>,
    columns: ops::Range<u16>,
}

pub struct Scalar<'a, 'b>(&'a mut RowReader<'b>);

pub struct Data<'a, 'b>(&'a mut RowReader<'b>);

pub struct Optional<'a, 'b>(&'a mut RowReader<'b>);

pub struct Arrays<'a, 'b>(&'a mut RowReader<'b>);

struct Column<'a> {
    buf: Wire<'a>,
    index: u16,
}

enum Field<'a> {
    Data(Column<'a>),
    Null { column: u16 },
}

impl<'a> RowReader<'a> {
    #[inline]
    pub(crate) fn new(bytes: Wire<'a>, want: u16) -> Result<Self, crate::Error> {
        let Some((head, tail)) = bytes.split_array::<2>() else {
            return Err(crate::Error::Protocol(Reason::RowTruncated {
                actual: bytes.length(),
            }));
        };
        let count = i16::from_be_bytes(*head);
        if count < 0 {
            return Err(crate::Error::Protocol(Reason::NegativeColumnCount {
                actual: count,
            }));
        }
        let count = count as u16;
        if count != want {
            return Err(crate::Error::Protocol(Reason::ColumnCount {
                actual: count,
                expected: want,
            }));
        }
        Ok(Self {
            buf: tail,
            columns: 0..count,
        })
    }

    pub(crate) fn nullable<T>(
        bytes: Wire<'a>,
        decode: impl FnOnce(&mut Self) -> Result<T, crate::Error>,
    ) -> Result<Option<T>, crate::Error> {
        let mut reader = Self::new(bytes, 1)?;
        let value = match reader.buf.split_array::<4>() {
            Some((head, tail)) if *head == [u8::MAX; 4] => {
                reader.buf = tail;
                reader.columns.start = 1;
                None
            }
            _ => Some(decode(&mut reader)?),
        };
        reader.finish()?;
        Ok(value)
    }

    pub(crate) fn finish(self) -> Result<(), crate::Error> {
        if !self.columns.is_empty() {
            return Err(crate::Error::Protocol(Reason::RowUnread {
                remaining: self.columns.end - self.columns.start,
            }));
        }
        if !self.buf.is_empty() {
            return Err(crate::Error::Protocol(Reason::RowTrailing {
                actual: self.buf.length(),
            }));
        }
        Ok(())
    }

    #[inline]
    fn take_column(&mut self) -> Result<Field<'a>, crate::Error> {
        let mut columns = self.columns.clone();
        let index = columns
            .next()
            .ok_or(crate::Error::Protocol(Reason::RowDecoderColumns {
                declared: self.columns.end,
            }))?;
        let Some((head, tail)) = self.buf.split_array::<4>() else {
            return Err(crate::Error::Protocol(Reason::ColumnLength {
                column: index,
                actual: self.buf.length(),
            }));
        };
        let len = i32::from_be_bytes(*head);
        self.buf = tail;
        self.columns = columns;
        if len == -1 {
            return Ok(Field::Null { column: index });
        }
        if len < 0 {
            return Err(crate::Error::Protocol(Reason::NegativeColumnLength {
                column: index,
                actual: len,
            }));
        }
        let n = len as u32;
        let Some((head, tail)) = self.buf.split_at(n as usize) else {
            return Err(crate::Error::Protocol(Reason::ColumnBytes {
                column: index,
                actual: self.buf.length(),
                required: Length::from_u32(n),
            }));
        };
        self.buf = tail;
        Ok(Field::Data(Column { buf: head, index }))
    }

    #[inline]
    fn require(&mut self) -> Result<Column<'a>, crate::Error> {
        match self.take_column()? {
            Field::Data(column) => Ok(column),
            Field::Null { column } => Err(crate::Error::Protocol(Reason::ColumnNull { column })),
        }
    }

    #[inline]
    pub fn read_i32(&mut self) -> Result<i32, crate::Error> {
        Ok(i32::from_be_bytes(self.require()?.fixed()?))
    }

    pub fn scalar(&mut self) -> Scalar<'_, 'a> {
        Scalar(self)
    }

    pub fn data(&mut self) -> Data<'_, 'a> {
        Data(self)
    }

    pub fn optional(&mut self) -> Optional<'_, 'a> {
        Optional(self)
    }

    pub fn arrays(&mut self) -> Arrays<'_, 'a> {
        Arrays(self)
    }
}

impl Column<'_> {
    fn width<const N: usize>() -> Result<Length, crate::Error> {
        u32::try_from(N)
            .map(Length::from_u32)
            .map_err(|_| crate::Error::Protocol(Reason::RequiredLength { required: N }))
    }

    fn fixed<const N: usize>(&self) -> Result<[u8; N], crate::Error> {
        match self.buf.as_slice().try_into() {
            Ok(bytes) => Ok(bytes),
            Err(_) => Err(crate::Error::Protocol(Reason::ColumnWidth {
                column: self.index,
                actual: self.buf.length(),
                expected: Self::width::<N>()?,
            })),
        }
    }

    fn read_bool(&self) -> Result<bool, crate::Error> {
        match self.fixed::<1>()? {
            [0] => Ok(false),
            [1] => Ok(true),
            [actual] => Err(crate::Error::Protocol(Reason::Bool {
                column: self.index,
                actual,
            })),
        }
    }

    fn take_fixed<const N: usize>(&mut self) -> Result<[u8; N], crate::Error> {
        let Some((head, tail)) = self.buf.split_array::<N>() else {
            return Err(crate::Error::Protocol(Reason::ArrayHeader {
                column: self.index,
                actual: self.buf.length(),
                required: Self::width::<N>()?,
            }));
        };
        self.buf = tail;
        Ok(*head)
    }
}

impl Scalar<'_, '_> {
    pub fn read_fixed<const N: usize>(&mut self) -> Result<[u8; N], crate::Error> {
        self.0.require()?.fixed()
    }

    pub fn read_bool(&mut self) -> Result<bool, crate::Error> {
        self.0.require()?.read_bool()
    }

    pub fn read_i16(&mut self) -> Result<i16, crate::Error> {
        Ok(i16::from_be_bytes(self.read_fixed()?))
    }

    #[inline]
    pub fn read_i64(&mut self) -> Result<i64, crate::Error> {
        Ok(i64::from_be_bytes(self.read_fixed()?))
    }

    pub fn read_f32(&mut self) -> Result<f32, crate::Error> {
        Ok(f32::from_be_bytes(self.read_fixed()?))
    }

    #[inline]
    pub fn read_f64(&mut self) -> Result<f64, crate::Error> {
        Ok(f64::from_be_bytes(self.read_fixed()?))
    }
}

impl<'b> Data<'_, 'b> {
    pub fn read_bytes(&mut self) -> Result<&'b [u8], crate::Error> {
        Ok(self.0.require()?.buf.as_slice())
    }

    pub fn read_text(&mut self) -> Result<&'b str, crate::Error> {
        let column = self.0.require()?;
        text(column.index, column.buf)
    }

    pub fn read_json(&mut self) -> Result<&'b str, crate::Error> {
        let column = self.0.require()?;
        json(column.index, column.buf)
    }
}

impl<'b> Optional<'_, 'b> {
    fn fixed<const N: usize, T>(
        &mut self,
        decode: impl FnOnce([u8; N]) -> T,
    ) -> Result<Option<T>, crate::Error> {
        let column = match self.0.take_column()? {
            Field::Data(column) => column,
            Field::Null { .. } => return Ok(None),
        };
        Ok(Some(decode(column.fixed()?)))
    }

    pub fn read_bool(&mut self) -> Result<Option<bool>, crate::Error> {
        let column = match self.0.take_column()? {
            Field::Data(column) => column,
            Field::Null { .. } => return Ok(None),
        };
        Ok(Some(column.read_bool()?))
    }

    pub fn read_i64(&mut self) -> Result<Option<i64>, crate::Error> {
        self.fixed::<8, _>(i64::from_be_bytes)
    }

    pub fn read_i32(&mut self) -> Result<Option<i32>, crate::Error> {
        self.fixed::<4, _>(i32::from_be_bytes)
    }

    pub fn read_text(&mut self) -> Result<Option<&'b str>, crate::Error> {
        let column = match self.0.take_column()? {
            Field::Data(column) => column,
            Field::Null { .. } => return Ok(None),
        };
        text(column.index, column.buf).map(Some)
    }

    pub fn read_json(&mut self) -> Result<Option<&'b str>, crate::Error> {
        let column = match self.0.take_column()? {
            Field::Data(column) => column,
            Field::Null { .. } => return Ok(None),
        };
        json(column.index, column.buf).map(Some)
    }

    pub fn read_bytes(&mut self) -> Result<Option<&'b [u8]>, crate::Error> {
        match self.0.take_column()? {
            Field::Data(column) => Ok(Some(column.buf.as_slice())),
            Field::Null { .. } => Ok(None),
        }
    }

    pub fn read_i16(&mut self) -> Result<Option<i16>, crate::Error> {
        self.fixed::<2, _>(i16::from_be_bytes)
    }

    pub fn read_f32(&mut self) -> Result<Option<f32>, crate::Error> {
        self.fixed::<4, _>(f32::from_be_bytes)
    }

    pub fn read_f64(&mut self) -> Result<Option<f64>, crate::Error> {
        self.fixed::<8, _>(f64::from_be_bytes)
    }

    pub fn read_uuid(&mut self) -> Result<Option<[u8; 16]>, crate::Error> {
        self.fixed::<16, _>(identity)
    }
}

impl Column<'_> {
    fn array_header(&mut self, want: ArrayElement) -> Result<usize, crate::Error> {
        let [a, b, c, d, e, f, g, h, i, j, k, l] = self.take_fixed::<12>()?;
        let ndim = i32::from_be_bytes([a, b, c, d]);
        let nulls = i32::from_be_bytes([e, f, g, h]);
        let oid = u32::from_be_bytes([i, j, k, l]);
        match want {
            ArrayElement::Exact(want) if oid != want => {
                return Err(crate::Error::Protocol(Reason::ArrayOid {
                    column: self.index,
                    actual: oid,
                    expected: want,
                }));
            }
            ArrayElement::String if !matches!(oid, 19 | 25 | 1042 | 1043) => {
                return Err(crate::Error::Protocol(Reason::ArrayTextOid {
                    column: self.index,
                    actual: oid,
                }));
            }
            ArrayElement::Exact(_) | ArrayElement::String => {}
        }
        if nulls != 0 {
            return Err(crate::Error::Protocol(Reason::ArrayNulls {
                column: self.index,
                actual: nulls,
            }));
        }
        if ndim == 0 {
            return Ok(0);
        }
        if ndim != 1 {
            return Err(crate::Error::Protocol(Reason::ArrayDimensions {
                column: self.index,
                actual: ndim,
            }));
        }
        let [a, b, c, d, _, _, _, _] = self.take_fixed::<8>()?;
        let dim = i32::from_be_bytes([a, b, c, d]);
        if dim < 0 {
            return Err(crate::Error::Protocol(Reason::NegativeArrayDimension {
                column: self.index,
                actual: dim,
            }));
        }
        let dim = dim as u32;
        if dim as usize > self.buf.len() / 4 {
            return Err(crate::Error::Protocol(Reason::ArrayPayload {
                column: self.index,
                elements: dim,
                actual: self.buf.length(),
            }));
        }
        Ok(dim as usize)
    }
}

#[derive(Clone, Copy)]
enum ArrayElement {
    Exact(u32),
    String,
}

impl<'b> Arrays<'_, 'b> {
    pub fn read_i64(&mut self) -> Result<array::Array<'b, i64>, crate::Error> {
        let mut column = self.0.require()?;
        let len = column.array_header(ArrayElement::Exact(20))?;
        Ok(array::Array::new(column.buf, len))
    }

    pub fn read_i32(&mut self) -> Result<array::Array<'b, i32>, crate::Error> {
        let mut column = self.0.require()?;
        let len = column.array_header(ArrayElement::Exact(23))?;
        Ok(array::Array::new(column.buf, len))
    }

    pub fn read_text(&mut self) -> Result<array::Array<'b, &'b str>, crate::Error> {
        let mut column = self.0.require()?;
        let len = column.array_header(ArrayElement::String)?;
        Ok(array::Array::new(column.buf, len))
    }
}

fn text(column: u16, bytes: Wire<'_>) -> Result<&str, crate::Error> {
    match bytes.try_text() {
        Ok(value) => Ok(value.as_str()),
        Err(error) => Err(crate::Error::Protocol(Reason::TextUtf8 {
            column,
            valid: error.valid,
            error: error.error,
        })),
    }
}

fn json(column: u16, bytes: Wire<'_>) -> Result<&str, crate::Error> {
    let Some((version, value)) = bytes.split_first() else {
        return Err(crate::Error::Protocol(Reason::JsonTruncated {
            column,
            actual: Length::from_u32(0),
        }));
    };
    if version != 0x01 {
        return Err(crate::Error::Protocol(Reason::JsonVersion {
            column,
            actual: version,
        }));
    }
    match value.try_text() {
        Ok(value) => Ok(value.as_str()),
        Err(error) => Err(crate::Error::Protocol(Reason::JsonUtf8 {
            column,
            valid: error.valid,
            error: error.error,
        })),
    }
}

const _: () = assert!(std::mem::size_of::<RowReader<'_>>() == 3 * std::mem::size_of::<usize>());
const _: () = assert!(std::mem::size_of::<Column<'_>>() == 3 * std::mem::size_of::<usize>());
const _: () = assert!(std::mem::size_of::<Scalar<'_, '_>>() == std::mem::size_of::<usize>());
const _: () = assert!(std::mem::size_of::<Data<'_, '_>>() == std::mem::size_of::<usize>());
const _: () = assert!(std::mem::size_of::<Optional<'_, '_>>() == std::mem::size_of::<usize>());
const _: () = assert!(std::mem::size_of::<Arrays<'_, '_>>() == std::mem::size_of::<usize>());

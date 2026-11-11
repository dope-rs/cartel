use crate::{
    protocol::{Length, Reason, Wire},
    schema::array,
};
use std::ops;

pub mod checked;

pub struct RowReader<'a> {
    buf: Wire<'a>,
    columns: ops::Range<u16>,
}

#[doc(hidden)]
pub struct Column<'a> {
    buf: Wire<'a>,
    index: u16,
}

pub(crate) enum Field<'a> {
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
    pub(crate) fn take_column(&mut self) -> Result<Field<'a>, crate::Error> {
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
    pub(crate) fn require(&mut self) -> Result<Column<'a>, crate::Error> {
        match self.take_column()? {
            Field::Data(column) => Ok(column),
            Field::Null { column } => Err(crate::Error::Protocol(Reason::ColumnNull { column })),
        }
    }
}

impl Column<'_> {
    fn width<const N: usize>() -> Result<Length, crate::Error> {
        u32::try_from(N)
            .map(Length::from_u32)
            .map_err(|_| crate::Error::Protocol(Reason::RequiredLength { required: N }))
    }

    pub(crate) fn fixed<const N: usize>(&self) -> Result<[u8; N], crate::Error> {
        match self.buf.as_slice().try_into() {
            Ok(bytes) => Ok(bytes),
            Err(_) => Err(crate::Error::Protocol(Reason::ColumnWidth {
                column: self.index,
                actual: self.buf.length(),
                expected: Self::width::<N>()?,
            })),
        }
    }

    #[inline]
    pub(crate) fn read_bool(&self) -> Result<bool, crate::Error> {
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

impl Column<'_> {
    #[inline]
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
pub(crate) enum ArrayElement {
    Exact(u32),
    String,
}

impl<'row> Column<'row> {
    pub(crate) fn array<T: array::Element<'row>>(
        mut self,
        want: ArrayElement,
    ) -> Result<array::Array<'row, T>, crate::Error> {
        let len = self.array_header(want)?;
        Ok(array::Array::new(self.buf, len))
    }

    pub(crate) fn bytes(self) -> &'row [u8] {
        self.buf.as_slice()
    }

    #[inline]
    pub(crate) fn text(self) -> Result<&'row str, crate::Error> {
        match self.buf.try_text() {
            Ok(value) => Ok(value.as_str()),
            Err(error) => Err(crate::Error::Protocol(Reason::TextUtf8 {
                column: self.index,
                valid: error.valid,
                error: error.error,
            })),
        }
    }

    #[inline]
    pub(crate) fn json(self) -> Result<&'row str, crate::Error> {
        let Some((version, value)) = self.buf.split_first() else {
            return Err(crate::Error::Protocol(Reason::JsonTruncated {
                column: self.index,
                actual: Length::from_u32(0),
            }));
        };
        if version != 0x01 {
            return Err(crate::Error::Protocol(Reason::JsonVersion {
                column: self.index,
                actual: version,
            }));
        }
        match value.try_text() {
            Ok(value) => Ok(value.as_str()),
            Err(error) => Err(crate::Error::Protocol(Reason::JsonUtf8 {
                column: self.index,
                valid: error.valid,
                error: error.error,
            })),
        }
    }
}

const _: () = assert!(std::mem::size_of::<RowReader<'_>>() == 3 * std::mem::size_of::<usize>());
const _: () = assert!(std::mem::size_of::<Column<'_>>() == 3 * std::mem::size_of::<usize>());

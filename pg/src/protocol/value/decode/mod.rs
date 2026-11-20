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

pub(crate) struct Column<'a> {
    buf: Wire<'a>,
    index: u16,
}

fn width<const N: usize>() -> Result<Length, Reason> {
    u32::try_from(N)
        .map(Length::from_u32)
        .map_err(|_| Reason::RequiredLength { required: N })
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
        if i32::from(count) != i32::from(want) {
            return Err(crate::Error::Protocol(if count < 0 {
                Reason::NegativeColumnCount { actual: count }
            } else {
                Reason::ColumnCount {
                    actual: count as u16,
                    expected: want,
                }
            }));
        }
        Ok(Self {
            buf: tail,
            columns: 0..want,
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
    fn header(&mut self) -> Result<(u16, i32), Reason> {
        let index = self.columns.start;
        if self.columns.is_empty() {
            return Err(Reason::RowDecoderColumns {
                declared: self.columns.end,
            });
        }
        let Some((head, tail)) = self.buf.split_array::<4>() else {
            return Err(Reason::ColumnLength {
                column: index,
                actual: self.buf.length(),
            });
        };
        self.buf = tail;
        self.columns.start += 1;
        Ok((index, i32::from_be_bytes(*head)))
    }

    #[inline]
    fn payload(&mut self, index: u16, length: i32) -> Result<Column<'a>, Reason> {
        let Ok(n) = u32::try_from(length) else {
            return Err(if length == -1 {
                Reason::ColumnNull { column: index }
            } else {
                Reason::NegativeColumnLength {
                    column: index,
                    actual: length,
                }
            });
        };
        let Some((buf, tail)) = self.buf.split_at(n as usize) else {
            return Err(Reason::ColumnBytes {
                column: index,
                actual: self.buf.length(),
                required: Length::from_u32(n),
            });
        };
        self.buf = tail;
        Ok(Column { buf, index })
    }

    #[inline]
    pub(crate) fn column(&mut self) -> Result<Column<'a>, Reason> {
        let (index, length) = self.header()?;
        self.payload(index, length)
    }

    #[inline]
    pub(crate) fn fixed<const N: usize>(&mut self) -> Result<[u8; N], Reason> {
        let (index, length) = self.header()?;
        if usize::try_from(length) == Ok(N)
            && let Some((bytes, tail)) = self.buf.split_array::<N>()
        {
            self.buf = tail;
            return Ok(*bytes);
        }
        let column = self.payload(index, length)?;
        Err(Reason::ColumnWidth {
            column: index,
            actual: column.buf.length(),
            expected: width::<N>()?,
        })
    }

    #[inline]
    pub(crate) fn read_bool(&mut self) -> Result<bool, Reason> {
        let column = self.columns.start;
        match self.fixed::<1>()? {
            [0] => Ok(false),
            [1] => Ok(true),
            [actual] => Err(Reason::Bool { column, actual }),
        }
    }
}

impl Column<'_> {
    fn take_fixed<const N: usize>(&mut self) -> Result<[u8; N], Reason> {
        let Some((head, tail)) = self.buf.split_array::<N>() else {
            return Err(Reason::ArrayHeader {
                column: self.index,
                actual: self.buf.length(),
                required: width::<N>()?,
            });
        };
        self.buf = tail;
        Ok(*head)
    }

    #[inline]
    fn array_header(&mut self, want: ArrayElement) -> Result<usize, Reason> {
        let [a, b, c, d, e, f, g, h, i, j, k, l] = self.take_fixed::<12>()?;
        let ndim = i32::from_be_bytes([a, b, c, d]);
        let nulls = i32::from_be_bytes([e, f, g, h]);
        let oid = u32::from_be_bytes([i, j, k, l]);
        match want {
            ArrayElement::Exact(want) if oid != want => {
                return Err(Reason::ArrayOid {
                    column: self.index,
                    actual: oid,
                    expected: want,
                });
            }
            ArrayElement::String if !matches!(oid, 19 | 25 | 1042 | 1043) => {
                return Err(Reason::ArrayTextOid {
                    column: self.index,
                    actual: oid,
                });
            }
            ArrayElement::Exact(_) | ArrayElement::String => {}
        }
        if nulls != 0 {
            return Err(Reason::ArrayNulls {
                column: self.index,
                actual: nulls,
            });
        }
        if ndim == 0 {
            return Ok(0);
        }
        if ndim != 1 {
            return Err(Reason::ArrayDimensions {
                column: self.index,
                actual: ndim,
            });
        }
        let [a, b, c, d, _, _, _, _] = self.take_fixed::<8>()?;
        let dim = i32::from_be_bytes([a, b, c, d]);
        if dim < 0 {
            return Err(Reason::NegativeArrayDimension {
                column: self.index,
                actual: dim,
            });
        }
        let dim = dim as u32;
        if dim as usize > self.buf.len() / 4 {
            return Err(Reason::ArrayPayload {
                column: self.index,
                elements: dim,
                actual: self.buf.length(),
            });
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
    ) -> Result<array::Array<'row, T>, Reason> {
        let len = self.array_header(want)?;
        Ok(array::Array::new(self.buf, len))
    }

    pub(crate) fn bytes(self) -> &'row [u8] {
        self.buf.as_slice()
    }

    #[inline]
    pub(crate) fn text(self) -> Result<&'row str, Reason> {
        match self.buf.try_text() {
            Ok(value) => Ok(value.as_str()),
            Err(error) => Err(Reason::TextUtf8 {
                column: self.index,
                valid: error.valid,
                error: error.error,
            }),
        }
    }

    #[inline]
    pub(crate) fn json(self) -> Result<&'row str, Reason> {
        let Some((version, value)) = self.buf.split_first() else {
            return Err(Reason::JsonTruncated {
                column: self.index,
                actual: Length::from_u32(0),
            });
        };
        if version != 0x01 {
            return Err(Reason::JsonVersion {
                column: self.index,
                actual: version,
            });
        }
        match value.try_text() {
            Ok(value) => Ok(value.as_str()),
            Err(error) => Err(Reason::JsonUtf8 {
                column: self.index,
                valid: error.valid,
                error: error.error,
            }),
        }
    }
}

const _: () = assert!(std::mem::size_of::<RowReader<'_>>() == 3 * std::mem::size_of::<usize>());
const _: () = assert!(std::mem::size_of::<Column<'_>>() == 3 * std::mem::size_of::<usize>());

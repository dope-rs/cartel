use crate::protocol::encode;

fn wire_length(value: usize) -> Result<i32, encode::Error> {
    const MAXIMUM: usize = i32::MAX as usize;
    if value > MAXIMUM {
        Err(encode::Error::ValueLength { actual: value })
    } else {
        Ok(value as i32)
    }
}

fn count32(value: usize) -> Result<i32, encode::Error> {
    const MAXIMUM: usize = i32::MAX as usize;
    if value > MAXIMUM {
        Err(encode::Error::ArrayElements { actual: value })
    } else {
        Ok(value as i32)
    }
}

#[inline]
fn write_with_len(out: &mut encode::Writer<'_>, payload: &[u8]) -> Result<(), encode::Error> {
    out.write_slices([&wire_length(payload.len())?.to_be_bytes(), payload])
}

fn write_array(
    values: &mut BindWriter<'_, '_>,
    oid: u32,
    element: u32,
    len: usize,
    write: impl FnOnce(&mut encode::Writer<'_>) -> Result<(), encode::Error>,
) -> Result<(), encode::Error> {
    values.write(oid, 1, |out| {
        let count = count32(len)?;
        out.patch::<false, _>(|out| {
            out.write_slices([
                &1i32.to_be_bytes(),
                &0i32.to_be_bytes(),
                &element.to_be_bytes(),
                &count.to_be_bytes(),
                &1i32.to_be_bytes(),
            ])?;
            write(out)
        })
    })
}

/// Encoding retains the exact parameter domains admitted for its query type.
pub struct BindWriter<'a, 'out> {
    out: &'a mut encode::Writer<'out>,
    oids: &'a [u32],
    formats: &'a [u16],
    index: usize,
}

impl<'a, 'out> BindWriter<'a, 'out> {
    pub(crate) fn new<Q: crate::schema::TypedQuery>(
        out: &'a mut encode::Writer<'out>,
        _: &crate::schema::Admission<Q>,
    ) -> Self {
        Self {
            out,
            oids: Q::PARAM_OIDS,
            formats: Q::PARAM_FORMAT_CODES,
            index: 0,
        }
    }

    pub fn write_null(&mut self) -> Result<(), encode::Error> {
        self.check_any()?;
        self.out.extend(&(-1i32).to_be_bytes())?;
        self.index += 1;
        Ok(())
    }

    pub fn scalar(&mut self) -> Scalar<'_, 'a, 'out> {
        Scalar(self)
    }

    pub fn data(&mut self) -> Data<'_, 'a, 'out> {
        Data(self)
    }

    pub fn array(&mut self) -> Array<'_, 'a, 'out> {
        Array(self)
    }

    pub(crate) fn finish(self) -> Result<(), encode::Error> {
        if self.index == self.oids.len() {
            Ok(())
        } else {
            Err(encode::Error::ParameterValueCount {
                expected: self.oids.len(),
                actual: self.index,
            })
        }
    }

    fn write(
        &mut self,
        oid: u32,
        format: u16,
        write: impl FnOnce(&mut encode::Writer<'_>) -> Result<(), encode::Error>,
    ) -> Result<(), encode::Error> {
        self.check(oid, format)?;
        write(self.out)?;
        self.index += 1;
        Ok(())
    }

    #[inline]
    fn check(&self, oid: u32, format: u16) -> Result<(), encode::Error> {
        let index = self.index;
        let Some(expected) = self.oids.get(index).copied() else {
            return Err(encode::Error::ParameterValueCount {
                expected: self.oids.len(),
                actual: self.oids.len() + 1,
            });
        };
        if oid != expected {
            return Err(encode::Error::ParameterOid {
                index,
                expected,
                actual: oid,
            });
        }
        let expected = match self.formats {
            [] => 0,
            [format] => *format,
            formats => {
                // SAFETY: admission binds these formats to the exact OID count;
                // the same retained OID slice admitted this index above.
                unsafe { *formats.get_unchecked(index) }
            }
        };
        if format != expected {
            return Err(encode::Error::ParameterEncoding {
                index,
                expected,
                actual: format,
            });
        }
        Ok(())
    }

    fn check_any(&self) -> Result<(), encode::Error> {
        if self.index >= self.oids.len() {
            return Err(encode::Error::ParameterValueCount {
                expected: self.oids.len(),
                actual: self.oids.len() + 1,
            });
        }
        Ok(())
    }
}

#[repr(transparent)]
pub struct Scalar<'a, 'values, 'out>(&'a mut BindWriter<'values, 'out>);

impl Scalar<'_, '_, '_> {
    #[inline]
    pub fn write_bool(&mut self, value: bool) -> Result<(), encode::Error> {
        self.0
            .write(16, 1, |out| write_with_len(out, &[value as u8]))
    }

    #[inline]
    pub fn write_i16(&mut self, value: i16) -> Result<(), encode::Error> {
        self.0
            .write(21, 1, |out| write_with_len(out, &value.to_be_bytes()))
    }

    #[inline]
    pub fn write_i32(&mut self, value: i32) -> Result<(), encode::Error> {
        self.0
            .write(23, 1, |out| write_with_len(out, &value.to_be_bytes()))
    }

    #[inline]
    pub fn write_i64(&mut self, value: i64) -> Result<(), encode::Error> {
        self.0
            .write(20, 1, |out| write_with_len(out, &value.to_be_bytes()))
    }

    #[inline]
    pub fn write_f32(&mut self, value: f32) -> Result<(), encode::Error> {
        self.0
            .write(700, 1, |out| write_with_len(out, &value.to_be_bytes()))
    }

    #[inline]
    pub fn write_f64(&mut self, value: f64) -> Result<(), encode::Error> {
        self.0
            .write(701, 1, |out| write_with_len(out, &value.to_be_bytes()))
    }
}

#[repr(transparent)]
pub struct Data<'a, 'values, 'out>(&'a mut BindWriter<'values, 'out>);

impl Data<'_, '_, '_> {
    #[inline]
    pub fn write_text(&mut self, value: &str) -> Result<(), encode::Error> {
        self.0
            .write(25, 1, |out| write_with_len(out, value.as_bytes()))
    }

    #[inline]
    pub fn write_ltree(&mut self, value: &str) -> Result<(), encode::Error> {
        self.0
            .write(25, 0, |out| write_with_len(out, value.as_bytes()))
    }

    pub fn write_jsonb(&mut self, value: &str) -> Result<(), encode::Error> {
        self.0.write(3802, 1, |out| {
            let len = value.len() + 1;
            out.write_slices([&wire_length(len)?.to_be_bytes(), &[0x01], value.as_bytes()])
        })
    }

    #[inline]
    pub fn write_bytes(&mut self, value: &[u8]) -> Result<(), encode::Error> {
        self.0.write(17, 1, |out| write_with_len(out, value))
    }

    #[inline]
    pub fn write_uuid(&mut self, value: [u8; 16]) -> Result<(), encode::Error> {
        self.0.write(2950, 1, |out| write_with_len(out, &value))
    }

    #[inline]
    pub fn write_time(&mut self, value: crate::Timestamp) -> Result<(), encode::Error> {
        self.0
            .write(1114, 1, |out| write_with_len(out, &value.0.to_be_bytes()))
    }

    #[inline]
    pub fn write_date(&mut self, value: crate::Date) -> Result<(), encode::Error> {
        self.0
            .write(1082, 1, |out| write_with_len(out, &value.0.to_be_bytes()))
    }
}

#[repr(transparent)]
pub struct Array<'a, 'values, 'out>(&'a mut BindWriter<'values, 'out>);

impl Array<'_, '_, '_> {
    pub fn write_i64(&mut self, items: &[i64]) -> Result<(), encode::Error> {
        write_array(self.0, 1016, 20, items.len(), |out| {
            for value in items {
                write_with_len(out, &value.to_be_bytes())?;
            }
            Ok(())
        })
    }

    pub fn write_i32(&mut self, items: &[i32]) -> Result<(), encode::Error> {
        write_array(self.0, 1007, 23, items.len(), |out| {
            for value in items {
                write_with_len(out, &value.to_be_bytes())?;
            }
            Ok(())
        })
    }

    pub fn write_text(&mut self, items: &[&str]) -> Result<(), encode::Error> {
        write_array(self.0, 1009, 25, items.len(), |out| {
            for value in items {
                write_with_len(out, value.as_bytes())?;
            }
            Ok(())
        })
    }

    pub fn write_i16(&mut self, items: &[i16]) -> Result<(), encode::Error> {
        write_array(self.0, 1005, 21, items.len(), |out| {
            for value in items {
                write_with_len(out, &value.to_be_bytes())?;
            }
            Ok(())
        })
    }

    pub fn write_f32(&mut self, items: &[f32]) -> Result<(), encode::Error> {
        write_array(self.0, 1021, 700, items.len(), |out| {
            for value in items {
                write_with_len(out, &value.to_be_bytes())?;
            }
            Ok(())
        })
    }

    pub fn write_f64(&mut self, items: &[f64]) -> Result<(), encode::Error> {
        write_array(self.0, 1022, 701, items.len(), |out| {
            for value in items {
                write_with_len(out, &value.to_be_bytes())?;
            }
            Ok(())
        })
    }

    pub fn write_bool(&mut self, items: &[bool]) -> Result<(), encode::Error> {
        write_array(self.0, 1000, 16, items.len(), |out| {
            for value in items {
                write_with_len(out, &[*value as u8])?;
            }
            Ok(())
        })
    }
}

const _: () = {
    assert!(size_of::<BindWriter<'_, '_>>() == 6 * size_of::<usize>());
    assert!(size_of::<Scalar<'_, '_, '_>>() == size_of::<&mut BindWriter<'_, '_>>());
    assert!(size_of::<Data<'_, '_, '_>>() == size_of::<&mut BindWriter<'_, '_>>());
    assert!(size_of::<Array<'_, '_, '_>>() == size_of::<&mut BindWriter<'_, '_>>());
};

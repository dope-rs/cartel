use crate::protocol::encode::Error;

#[doc(hidden)]
pub struct Formats<'a> {
    values: &'a [u16],
    columns: u16,
    index: u16,
}

impl<'a> Formats<'a> {
    pub(crate) const fn new(values: &'a [u16], columns: u16) -> Self {
        Self {
            values,
            columns,
            index: 0,
        }
    }

    pub(crate) fn finish(self) -> Result<(), Error> {
        if self.index == self.columns {
            Ok(())
        } else {
            Err(Error::ResultValueCount {
                expected: usize::from(self.columns),
                actual: usize::from(self.index),
            })
        }
    }

    pub(super) fn take(&mut self, expected: u16) -> Result<(), Error> {
        let index = usize::from(self.index);
        if self.index >= self.columns {
            return Err(Error::ResultValueCount {
                expected: usize::from(self.columns),
                actual: index + 1,
            });
        }
        let actual = match self.values {
            [] => 0,
            [format] => *format,
            values => values[index],
        };
        if actual != expected {
            return Err(Error::ResultEncoding {
                index,
                expected,
                actual,
            });
        }
        self.index += 1;
        Ok(())
    }
}

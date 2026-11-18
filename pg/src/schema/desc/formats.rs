use crate::protocol::encode::Error;

#[doc(hidden)]
pub enum Formats {
    Value(u16),
    Row(&'static [Formats]),
}

impl Formats {
    pub(crate) const fn validate(&self, values: &[u16], columns: u16) -> Result<(), Error> {
        let mut cursor = Cursor {
            values,
            columns,
            index: 0,
        };
        match cursor.read(self) {
            Err(error) => Err(error),
            Ok(()) if cursor.index != columns => Err(Error::ResultValueCount {
                expected: columns as usize,
                actual: cursor.index as usize,
            }),
            Ok(()) => Ok(()),
        }
    }
}

struct Cursor<'a> {
    values: &'a [u16],
    columns: u16,
    index: u16,
}

impl Cursor<'_> {
    const fn read(&mut self, formats: &Formats) -> Result<(), Error> {
        match formats {
            Formats::Value(expected) => self.take(*expected),
            Formats::Row(fields) => {
                let mut index = 0;
                while index < fields.len() {
                    if let Err(error) = self.read(&fields[index]) {
                        return Err(error);
                    }
                    index += 1;
                }
                Ok(())
            }
        }
    }

    const fn take(&mut self, expected: u16) -> Result<(), Error> {
        let index = self.index as usize;
        if self.index >= self.columns {
            return Err(Error::ResultValueCount {
                expected: self.columns as usize,
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

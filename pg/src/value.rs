use o3::buffer::Shared;

use crate::Error;
use crate::wire::Sink;

const ARRAY_PREALLOC_CAP: usize = 4096;

pub struct RowReader<'a> {
    buf: &'a [u8],
    payload: &'a Shared,
}

impl<'a> RowReader<'a> {
    pub(super) fn new(payload: &'a Shared) -> Self {
        Self {
            buf: &payload[2..],
            payload,
        }
    }

    fn take_len(&mut self) -> Result<Option<usize>, Error> {
        if self.buf.len() < 4 {
            return Err(Error::Protocol("row column len truncated"));
        }
        let len = i32::from_be_bytes(Self::fixed_bytes(&self.buf[0..4])?);
        self.buf = &self.buf[4..];
        if len == -1 {
            return Ok(None);
        }
        if len < 0 {
            return Err(Error::Protocol("negative column length"));
        }
        Ok(Some(len as usize))
    }

    fn take_bytes(&mut self, n: usize) -> Result<&'a [u8], Error> {
        if self.buf.len() < n {
            return Err(Error::Protocol("row column bytes truncated"));
        }
        let (head, tail) = self.buf.split_at(n);
        self.buf = tail;
        Ok(head)
    }

    fn read_fixed<const N: usize>(&mut self) -> Result<[u8; N], Error> {
        let Some(len) = self.take_len()? else {
            return Err(Error::UnexpectedNull);
        };
        if len != N {
            return Err(Error::Protocol("unexpected column width"));
        }
        let bytes = self.take_bytes(N)?;
        Self::fixed_bytes(bytes)
    }

    fn read_opt_fixed<const N: usize, T>(
        &mut self,
        decode: impl FnOnce([u8; N]) -> T,
    ) -> Result<Option<T>, Error> {
        let Some(len) = self.take_len()? else {
            return Ok(None);
        };
        if len != N {
            return Err(Error::Protocol("unexpected column width"));
        }
        let bytes = self.take_bytes(N)?;
        Ok(Some(decode(Self::fixed_bytes(bytes)?)))
    }

    fn read_array_fixed<const N: usize, T>(
        &mut self,
        width_err: &'static str,
        decode: impl Fn([u8; N]) -> T,
    ) -> Result<Vec<T>, Error> {
        let n = self.array_header()?;
        let mut out = Vec::with_capacity(n.min(ARRAY_PREALLOC_CAP));
        for _ in 0..n {
            let len = self.take_len()?.ok_or(Error::UnexpectedNull)?;
            if len != N {
                return Err(Error::Protocol(width_err));
            }
            let bytes = self.take_bytes(N)?;
            out.push(decode(Self::fixed_bytes(bytes)?));
        }
        Ok(out)
    }

    pub fn read_bool(&mut self) -> Result<bool, Error> {
        let b = self.read_fixed::<1>()?;
        Ok(b[0] != 0)
    }

    pub fn read_i16(&mut self) -> Result<i16, Error> {
        Ok(i16::from_be_bytes(self.read_fixed()?))
    }

    pub fn read_i32(&mut self) -> Result<i32, Error> {
        Ok(i32::from_be_bytes(self.read_fixed()?))
    }

    pub fn read_i64(&mut self) -> Result<i64, Error> {
        Ok(i64::from_be_bytes(self.read_fixed()?))
    }

    pub fn read_f32(&mut self) -> Result<f32, Error> {
        Ok(f32::from_be_bytes(self.read_fixed()?))
    }

    pub fn read_f64(&mut self) -> Result<f64, Error> {
        Ok(f64::from_be_bytes(self.read_fixed()?))
    }

    pub fn read_uuid(&mut self) -> Result<[u8; 16], Error> {
        self.read_fixed()
    }

    pub fn read_timestamp(&mut self) -> Result<i64, Error> {
        Ok(i64::from_be_bytes(self.read_fixed()?))
    }

    pub fn read_date(&mut self) -> Result<i32, Error> {
        Ok(i32::from_be_bytes(self.read_fixed()?))
    }

    pub fn read_opt_timestamp(&mut self) -> Result<Option<i64>, Error> {
        self.read_opt_fixed::<8, _>(i64::from_be_bytes)
    }

    pub fn read_opt_date(&mut self) -> Result<Option<i32>, Error> {
        self.read_opt_fixed::<4, _>(i32::from_be_bytes)
    }

    pub fn read_bytes(&mut self) -> Result<&'a [u8], Error> {
        let len = self.take_len()?.ok_or(Error::UnexpectedNull)?;
        self.take_bytes(len)
    }

    pub fn read_text(&mut self) -> Result<&'a str, Error> {
        let bytes = self.read_bytes()?;
        std::str::from_utf8(bytes).map_err(|_| Error::Protocol("invalid utf-8 in text column"))
    }

    pub fn read_text_shared(&mut self) -> Result<crate::Text, Error> {
        let len = self.take_len()?.ok_or(Error::UnexpectedNull)?;
        let start = self.payload.len() - self.buf.len();
        self.take_bytes(len)?;
        crate::Text::from_shared(self.payload.slice(start..start + len))
            .map_err(|_| Error::Protocol("invalid utf-8 in text column"))
    }

    pub fn read_jsonb(&mut self) -> Result<crate::Jsonb, Error> {
        let len = self.take_len()?.ok_or(Error::UnexpectedNull)?;
        if len < 1 {
            return Err(Error::Protocol("jsonb column too short"));
        }
        let start = self.payload.len() - self.buf.len();
        let bytes = self.take_bytes(len)?;
        if bytes[0] != 0x01 {
            return Err(Error::Protocol("unsupported jsonb wire version"));
        }
        crate::Jsonb::from_shared(self.payload.slice(start + 1..start + len))
            .map_err(|_| Error::Protocol("invalid utf-8 in jsonb column"))
    }

    pub fn read_opt_bool(&mut self) -> Result<Option<bool>, Error> {
        self.read_opt_fixed::<1, _>(|b| b[0] != 0)
    }

    pub fn read_opt_i64(&mut self) -> Result<Option<i64>, Error> {
        self.read_opt_fixed::<8, _>(i64::from_be_bytes)
    }

    pub fn read_opt_i32(&mut self) -> Result<Option<i32>, Error> {
        self.read_opt_fixed::<4, _>(i32::from_be_bytes)
    }

    pub fn read_opt_text(&mut self) -> Result<Option<&'a str>, Error> {
        let Some(len) = self.take_len()? else {
            return Ok(None);
        };
        let bytes = self.take_bytes(len)?;
        std::str::from_utf8(bytes)
            .map(Some)
            .map_err(|_| Error::Protocol("invalid utf-8 in text column"))
    }

    pub fn read_opt_bytes(&mut self) -> Result<Option<&'a [u8]>, Error> {
        let Some(len) = self.take_len()? else {
            return Ok(None);
        };
        Ok(Some(self.take_bytes(len)?))
    }

    pub fn read_opt_i16(&mut self) -> Result<Option<i16>, Error> {
        self.read_opt_fixed::<2, _>(i16::from_be_bytes)
    }

    pub fn read_opt_f32(&mut self) -> Result<Option<f32>, Error> {
        self.read_opt_fixed::<4, _>(f32::from_be_bytes)
    }

    pub fn read_opt_f64(&mut self) -> Result<Option<f64>, Error> {
        self.read_opt_fixed::<8, _>(f64::from_be_bytes)
    }

    pub fn read_opt_uuid(&mut self) -> Result<Option<[u8; 16]>, Error> {
        self.read_opt_fixed::<16, _>(|b| b)
    }

    fn array_header(&mut self) -> Result<usize, Error> {
        let payload_len = self.take_len()?.ok_or(Error::UnexpectedNull)?;
        let head = self.take_bytes(12)?;
        let ndim = i32::from_be_bytes(Self::fixed_bytes(&head[0..4])?);
        let has_nulls = i32::from_be_bytes(Self::fixed_bytes(&head[4..8])?);
        if ndim == 0 {
            if payload_len != 12 {
                return Err(Error::Protocol("empty array payload length mismatch"));
            }
            return Ok(0);
        }
        if ndim != 1 {
            return Err(Error::Protocol("array column must be 1-dimensional"));
        }
        if has_nulls != 0 {
            return Err(Error::Protocol(
                "array column with NULL elements not supported",
            ));
        }
        let dim_head = self.take_bytes(8)?;
        let dim = i32::from_be_bytes(Self::fixed_bytes(&dim_head[0..4])?);
        if dim < 0 {
            return Err(Error::Protocol("negative array dimension"));
        }
        let dim = dim as usize;
        if dim > self.buf.len() / 4 {
            return Err(Error::Protocol("array dimension exceeds payload"));
        }
        Ok(dim)
    }

    pub fn read_array_i64(&mut self) -> Result<Vec<i64>, Error> {
        self.read_array_fixed::<8, _>(
            "array element width mismatch (expected 8)",
            i64::from_be_bytes,
        )
    }

    pub fn read_array_i32(&mut self) -> Result<Vec<i32>, Error> {
        self.read_array_fixed::<4, _>(
            "array element width mismatch (expected 4)",
            i32::from_be_bytes,
        )
    }

    pub fn read_array_text(&mut self) -> Result<Vec<String>, Error> {
        let n = self.array_header()?;
        let mut out = Vec::with_capacity(n.min(ARRAY_PREALLOC_CAP));
        for _ in 0..n {
            let len = self.take_len()?.ok_or(Error::UnexpectedNull)?;
            let bytes = self.take_bytes(len)?;
            let s = std::str::from_utf8(bytes)
                .map_err(|_| Error::Protocol("invalid utf-8 in text-array element"))?;
            out.push(s.to_owned());
        }
        Ok(out)
    }

    fn read_range_inner<T, F>(&mut self, decode_elem: F) -> Result<crate::Range<T>, Error>
    where
        F: Fn(&[u8]) -> Result<T, Error>,
    {
        let total = self.take_len()?.ok_or(Error::UnexpectedNull)?;
        let body = self.take_bytes(total)?;
        let (flags, mut cur) = body
            .split_first()
            .ok_or(Error::Protocol("range header empty"))?;
        let flags = *flags;
        const EMPTY: u8 = 0x01;
        const LB_INC: u8 = 0x02;
        const UB_INC: u8 = 0x04;
        const LB_INF: u8 = 0x08;
        const UB_INF: u8 = 0x10;
        if flags & EMPTY != 0 {
            return Ok(crate::Range::empty());
        }
        let take_bound = |cur: &mut &[u8]| -> Result<T, Error> {
            if cur.len() < 4 {
                return Err(Error::Protocol("range bound length truncated"));
            }
            let n = i32::from_be_bytes(Self::fixed_bytes(&cur[0..4])?);
            *cur = &cur[4..];
            if n < 0 {
                return Err(Error::Protocol("negative range bound length"));
            }
            let n = n as usize;
            if cur.len() < n {
                return Err(Error::Protocol("range bound bytes truncated"));
            }
            let (head, tail) = cur.split_at(n);
            *cur = tail;
            decode_elem(head)
        };
        let lower = if flags & LB_INF != 0 {
            crate::RangeBound::Unbounded
        } else {
            let v = take_bound(&mut cur)?;
            if flags & LB_INC != 0 {
                crate::RangeBound::Inclusive(v)
            } else {
                crate::RangeBound::Exclusive(v)
            }
        };
        let upper = if flags & UB_INF != 0 {
            crate::RangeBound::Unbounded
        } else {
            let v = take_bound(&mut cur)?;
            if flags & UB_INC != 0 {
                crate::RangeBound::Inclusive(v)
            } else {
                crate::RangeBound::Exclusive(v)
            }
        };
        Ok(crate::Range {
            lower,
            upper,
            empty: false,
        })
    }

    pub fn read_int4_range(&mut self) -> Result<crate::Range<i32>, Error> {
        self.read_range_inner(|b| {
            if b.len() != 4 {
                return Err(Error::Protocol("int4 range bound width != 4"));
            }
            Ok(i32::from_be_bytes(Self::fixed_bytes(b)?))
        })
    }

    pub fn read_int8_range(&mut self) -> Result<crate::Range<i64>, Error> {
        self.read_range_inner(|b| {
            if b.len() != 8 {
                return Err(Error::Protocol("int8 range bound width != 8"));
            }
            Ok(i64::from_be_bytes(Self::fixed_bytes(b)?))
        })
    }

    fn fixed_bytes<const N: usize>(bytes: &[u8]) -> Result<[u8; N], Error> {
        bytes
            .try_into()
            .map_err(|_| Error::Protocol("unexpected column width"))
    }
}

pub struct BindWriter<'a, S: Sink> {
    out: &'a mut S,
}

impl<'a, S: Sink> BindWriter<'a, S> {
    pub(super) fn new(out: &'a mut S) -> Self {
        Self { out }
    }

    pub fn write_null(&mut self) {
        self.out.extend_from_slice(&(-1i32).to_be_bytes());
    }

    fn write_with_len(&mut self, payload: &[u8]) {
        self.out
            .extend_from_slice(&(payload.len() as i32).to_be_bytes());
        self.out.extend_from_slice(payload);
    }

    pub fn write_bool(&mut self, v: bool) {
        self.write_with_len(&[v as u8]);
    }

    pub fn write_i16(&mut self, v: i16) {
        self.write_with_len(&v.to_be_bytes());
    }

    pub fn write_i32(&mut self, v: i32) {
        self.write_with_len(&v.to_be_bytes());
    }

    pub fn write_i64(&mut self, v: i64) {
        self.write_with_len(&v.to_be_bytes());
    }

    pub fn write_f32(&mut self, v: f32) {
        self.write_with_len(&v.to_be_bytes());
    }

    pub fn write_f64(&mut self, v: f64) {
        self.write_with_len(&v.to_be_bytes());
    }

    pub fn write_text(&mut self, s: &str) {
        self.write_with_len(s.as_bytes());
    }

    pub fn write_ltree(&mut self, s: &str) {
        self.write_with_len(s.as_bytes());
    }

    pub fn write_jsonb(&mut self, s: &str) {
        let payload_len = 1 + s.len();
        self.out
            .extend_from_slice(&(payload_len as i32).to_be_bytes());
        self.out.push(0x01);
        self.out.extend_from_slice(s.as_bytes());
    }

    pub fn write_bytes(&mut self, b: &[u8]) {
        self.write_with_len(b);
    }

    pub fn write_uuid(&mut self, u: [u8; 16]) {
        self.write_with_len(&u);
    }

    pub fn write_timestamp(&mut self, t: crate::Timestamp) {
        self.write_with_len(&t.0.to_be_bytes());
    }

    pub fn write_date(&mut self, d: crate::Date) {
        self.write_with_len(&d.0.to_be_bytes());
    }

    fn write_array_header(&mut self, total_payload_len: usize, elem_oid: u32, n: usize) {
        self.out
            .extend_from_slice(&(total_payload_len as i32).to_be_bytes());
        self.out.extend_from_slice(&1i32.to_be_bytes());
        self.out.extend_from_slice(&0i32.to_be_bytes());
        self.out.extend_from_slice(&elem_oid.to_be_bytes());
        self.out.extend_from_slice(&(n as i32).to_be_bytes());
        self.out.extend_from_slice(&1i32.to_be_bytes());
    }

    pub fn write_array_i64(&mut self, items: &[i64]) {
        const ELEM_OID: u32 = 20;
        let payload = 20 + items.len() * 12;
        self.write_array_header(payload, ELEM_OID, items.len());
        for x in items {
            self.out.extend_from_slice(&8i32.to_be_bytes());
            self.out.extend_from_slice(&x.to_be_bytes());
        }
    }

    pub fn write_array_i32(&mut self, items: &[i32]) {
        const ELEM_OID: u32 = 23;
        let payload = 20 + items.len() * 8;
        self.write_array_header(payload, ELEM_OID, items.len());
        for x in items {
            self.out.extend_from_slice(&4i32.to_be_bytes());
            self.out.extend_from_slice(&x.to_be_bytes());
        }
    }

    pub fn write_array_text(&mut self, items: &[&str]) {
        const ELEM_OID: u32 = 25;
        let payload = 20 + items.iter().map(|s| 4 + s.len()).sum::<usize>();
        self.write_array_header(payload, ELEM_OID, items.len());
        for s in items {
            self.out.extend_from_slice(&(s.len() as i32).to_be_bytes());
            self.out.extend_from_slice(s.as_bytes());
        }
    }

    pub fn write_array_i16(&mut self, items: &[i16]) {
        const ELEM_OID: u32 = 21;
        let payload = 20 + items.len() * 6;
        self.write_array_header(payload, ELEM_OID, items.len());
        for x in items {
            self.out.extend_from_slice(&2i32.to_be_bytes());
            self.out.extend_from_slice(&x.to_be_bytes());
        }
    }

    pub fn write_array_f32(&mut self, items: &[f32]) {
        const ELEM_OID: u32 = 700;
        let payload = 20 + items.len() * 8;
        self.write_array_header(payload, ELEM_OID, items.len());
        for x in items {
            self.out.extend_from_slice(&4i32.to_be_bytes());
            self.out.extend_from_slice(&x.to_be_bytes());
        }
    }

    pub fn write_array_f64(&mut self, items: &[f64]) {
        const ELEM_OID: u32 = 701;
        let payload = 20 + items.len() * 12;
        self.write_array_header(payload, ELEM_OID, items.len());
        for x in items {
            self.out.extend_from_slice(&8i32.to_be_bytes());
            self.out.extend_from_slice(&x.to_be_bytes());
        }
    }

    pub fn write_array_bool(&mut self, items: &[bool]) {
        const ELEM_OID: u32 = 16;
        let payload = 20 + items.len() * 5;
        self.write_array_header(payload, ELEM_OID, items.len());
        for x in items {
            self.out.extend_from_slice(&1i32.to_be_bytes());
            self.out.extend_from_slice(&[*x as u8]);
        }
    }

    fn write_range_inner<T, F>(&mut self, r: &crate::Range<T>, elem_width: usize, encode_elem: F)
    where
        F: Fn(&mut S, &T),
    {
        if r.empty {
            self.out.extend_from_slice(&1i32.to_be_bytes());
            self.out.extend_from_slice(&[0x01]);
            return;
        }
        const LB_INC: u8 = 0x02;
        const UB_INC: u8 = 0x04;
        const LB_INF: u8 = 0x08;
        const UB_INF: u8 = 0x10;
        let mut flags = 0u8;
        let mut payload_len = 1usize;
        match &r.lower {
            crate::RangeBound::Inclusive(_) => {
                flags |= LB_INC;
                payload_len += 4 + elem_width;
            }
            crate::RangeBound::Exclusive(_) => {
                payload_len += 4 + elem_width;
            }
            crate::RangeBound::Unbounded => flags |= LB_INF,
        }
        match &r.upper {
            crate::RangeBound::Inclusive(_) => {
                flags |= UB_INC;
                payload_len += 4 + elem_width;
            }
            crate::RangeBound::Exclusive(_) => {
                payload_len += 4 + elem_width;
            }
            crate::RangeBound::Unbounded => flags |= UB_INF,
        }
        self.out
            .extend_from_slice(&(payload_len as i32).to_be_bytes());
        self.out.extend_from_slice(&[flags]);
        for bound in [&r.lower, &r.upper] {
            let v = match bound {
                crate::RangeBound::Inclusive(v) | crate::RangeBound::Exclusive(v) => v,
                crate::RangeBound::Unbounded => continue,
            };
            self.out
                .extend_from_slice(&(elem_width as i32).to_be_bytes());
            encode_elem(self.out, v);
        }
    }

    pub fn write_int4_range(&mut self, r: &crate::Range<i32>) {
        self.write_range_inner(r, 4, |out, v| out.extend_from_slice(&v.to_be_bytes()));
    }

    pub fn write_int8_range(&mut self, r: &crate::Range<i64>) {
        self.write_range_inner(r, 8, |out, v| out.extend_from_slice(&v.to_be_bytes()));
    }
}

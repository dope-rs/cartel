use o3::buffer::storage;

pub struct Frame<'a> {
    tag: u8,
    body: &'a [u8],
}

impl<'a> Frame<'a> {
    pub const fn new(tag: u8, body: &'a [u8]) -> Self {
        Self { tag, body }
    }

    pub fn try_ingress(self, capacity: usize) -> Result<storage::Shared, storage::AllocError> {
        let complete = self.body.len() + 5;
        let mut ingress = storage::Owned::try_filled(capacity.max(complete), 0)?;
        ingress[0] = self.tag;
        ingress[1..5].copy_from_slice(&((self.body.len() + 4) as u32).to_be_bytes());
        ingress[5..complete].copy_from_slice(self.body);
        Ok(ingress.freeze())
    }

    pub fn encode(self) -> Vec<u8> {
        let mut frame = Vec::with_capacity(self.body.len() + 5);
        frame.push(self.tag);
        frame.extend_from_slice(&((self.body.len() + 4) as u32).to_be_bytes());
        frame.extend_from_slice(self.body);
        frame
    }
}

use syn::ext::IdentExt as _;

pub(super) struct Name(String);

impl Name {
    pub(super) fn new(ident: &syn::Ident) -> Self {
        const HEX: &[u8; 16] = b"0123456789abcdef";

        let ident = ident.unraw().to_string();
        let mut name = String::with_capacity(1 + ident.len() * 2);
        name.push('N');
        for byte in ident.bytes() {
            name.push(HEX[(byte >> 4) as usize] as char);
            name.push(HEX[(byte & 0x0f) as usize] as char);
        }
        Self(name)
    }

    pub(super) fn as_str(&self) -> &str {
        &self.0
    }
}

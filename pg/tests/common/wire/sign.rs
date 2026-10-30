use hmac::{Hmac, KeyInit as _, Mac as _, digest};
use sha2::Sha256;

pub fn try_mac(key: &[u8], value: &[u8]) -> Result<Vec<u8>, digest::InvalidLength> {
    let mut state = Hmac::<Sha256>::new_from_slice(key)?;
    state.update(value);
    Ok(state.finalize().into_bytes().to_vec())
}

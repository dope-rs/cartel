use crate::protocol::{Length, Offset, Reason, Wire, encode};
use crate::scram::Base64;
use base64::Engine as _;
use base64::engine::general_purpose;
use dope::core::{Backend, dope_sys::thread::Thread as _, driver::route};
use dope::manifold::client::conn;
use o3::buffer::bytes::bounded;
use sha2::block_api::compress256;
use std::mem;
use subtle::ConstantTimeEq as _;
use zeroize::Zeroizing;

pub(in crate::protocol) mod kdf;
mod parser;
mod secret32;

pub(crate) use kdf::Runtime;
use secret32::Secret32;

const MECH: &str = "SCRAM-SHA-256";
const PLUS: &str = "SCRAM-SHA-256-PLUS";
const CLIENT_NONCE_BYTES: usize = 24;
const PROOF_BYTES: usize = 44;

const _: () = {
    assert!(matches!(
        base64::encoded_len(18, false),
        Some(CLIENT_NONCE_BYTES)
    ));
    assert!(matches!(base64::encoded_len(32, true), Some(PROOF_BYTES)));
    assert!(matches!(base64::encoded_len(32, false), Some(len) if len == PROOF_BYTES - 1));
};

#[derive(Default)]
#[repr(u8)]
pub(super) enum Mechanisms {
    #[default]
    None,
    Plus,
    Sha256,
}

impl Mechanisms {
    pub(super) fn offer(&mut self, name: &str) {
        match name {
            MECH => *self = Self::Sha256,
            PLUS if matches!(self, Self::None) => *self = Self::Plus,
            PLUS => {}
            _ => {}
        }
    }

    pub(super) fn pick(self) -> Result<Mechanism, crate::Error> {
        match self {
            Self::Sha256 => Ok(Mechanism::Sha256),
            Self::Plus => Err(crate::Error::Auth(Reason::SaslPlus)),
            Self::None => Err(crate::Error::Auth(Reason::NoSasl)),
        }
    }
}

const _: () = {
    assert!(std::mem::size_of::<Mechanisms>() == std::mem::size_of::<bool>());
    assert!(std::mem::align_of::<Mechanisms>() == std::mem::align_of::<bool>());
};

pub(super) enum Mechanism {
    Sha256,
}

impl Mechanism {
    pub(super) const fn as_str(&self) -> &str {
        match self {
            Self::Sha256 => MECH,
        }
    }
}

struct ClientNonce([u8; CLIENT_NONCE_BYTES]);
pub(super) struct Signature(Secret32);

pub(in crate::protocol) struct Keys {
    client: Secret32,
    stored: Secret32,
    server: Secret32,
}

impl Keys {
    fn write(&mut self, inner: &[u32; 8], outer: &[u32; 8]) {
        let mut hash = Sha256::new(64);
        hash.state.copy_from_slice(inner);
        hash.update(b"Client Key");
        hash.hmac(outer, &mut self.client);
        hash.state.copy_from_slice(inner);
        *hash.len = 64;
        hash.update(b"Server Key");
        hash.hmac(outer, &mut self.server);
        hash.state.copy_from_slice(&IV);
        *hash.len = 0;
        hash.update(self.client.as_array());
        hash.finalize();
        write_state(&hash.state, self.stored.as_array_mut());
    }
}

struct Deriving {
    server_first: parser::Retained,
    key: kdf::KdfKey,
}

enum State {
    AwaitServerFirst,
    Deriving(Deriving),
    AwaitServerFinal(Signature),
    Complete,
}

pub(super) struct Scram<'a, 'd, const ID: u8> {
    pub(super) ticket: kdf::Ticket<'a, 'd, ID>,
    nonce: ClientNonce,
    state: State,
}

pub(super) struct ClientFirst<'a>(&'a [u8; CLIENT_NONCE_BYTES]);

pub(super) struct Derive<'a> {
    nonce: &'a ClientNonce,
    state: &'a Deriving,
}

impl ClientFirst<'_> {
    pub(super) fn write_to(&self, out: &mut encode::Writer) -> Result<(), encode::Error> {
        out.extend(b"n,,n=,r=")?;
        out.extend(self.0)
    }
}

impl<'a, 'd, const ID: u8> Scram<'a, 'd, ID> {
    pub(super) fn new(
        runtime: &'a Runtime<'d, ID>,
        owner: conn::Id<'d, route::KeyTag<ID>>,
    ) -> Result<Self, crate::Error> {
        let ticket = runtime.try_entry(owner).map_err(crate::Error::ScramSlot)?;
        let mut bytes = mem::MaybeUninit::uninit();
        let raw = Backend::entropy::<18>(&mut bytes).map_err(crate::Error::Random)?;
        Ok(Self {
            ticket,
            nonce: ClientNonce(encode_nonce(raw)),
            state: State::AwaitServerFirst,
        })
    }

    pub(super) fn client_first(&self) -> ClientFirst<'_> {
        ClientFirst(&self.nonce.0)
    }

    pub(super) fn begin(&mut self, password: &[u8], bytes: Wire<'_>) -> Result<(), crate::Error> {
        let State::AwaitServerFirst = &self.state else {
            return Err(crate::Error::Protocol(Reason::DuplicateServerFirst));
        };
        let parsed = parser::Parser::new(bytes)
            .server_first()
            .map_err(crate::Error::ServerFirst)?;
        parsed.with_parts(|parsed, nonce, salt, count| {
            if nonce.len() <= self.nonce.0.len() {
                return Err(crate::Error::ServerFirst(Reason::NonceLength {
                    actual: nonce.length(),
                    minimum: Length::from_u32(CLIENT_NONCE_BYTES as u32 + 1),
                }));
            }
            if let Some((offset, (actual, expected))) = (0u32..)
                .zip(
                    nonce
                        .as_slice()
                        .iter()
                        .copied()
                        .zip(self.nonce.0.iter().copied()),
                )
                .find(|item| item.1.0 != item.1.1)
            {
                return Err(crate::Error::ServerFirst(Reason::NonceByte {
                    offset: Offset::from_u32(offset),
                    actual,
                    expected,
                }));
            }

            let mut first = Sha256::new(64);
            let mut outer = Zeroizing::new(IV);
            hmac(password, &mut first.state, &mut outer);
            decode_base64(salt, |decoded| first.update(decoded))?;
            let iterations = kdf::try_iterations(count)?;
            self.state = State::Deriving(Deriving {
                server_first: parsed.try_retain()?,
                key: kdf::KdfKey {
                    first: Secret32::new([0; 32]),
                    iterations,
                },
            });
            let State::Deriving(state) = &mut self.state else {
                unreachable!("scram")
            };
            first.update(&1u32.to_be_bytes());
            first.hmac(&outer, &mut state.key.first);
            Ok(())
        })
    }

    pub(super) fn deriving(&self) -> Option<Derive<'_>> {
        match &self.state {
            State::Deriving(state) => Some(Derive {
                nonce: &self.nonce,
                state,
            }),
            State::AwaitServerFirst | State::AwaitServerFinal(_) | State::Complete => None,
        }
    }

    pub(super) fn await_final(&mut self, sig: &Signature) {
        self.state = State::AwaitServerFinal(Signature(sig.0.clone()));
    }

    pub(super) fn verify_final(&mut self, bytes: Wire<'_>) -> Result<(), crate::Error> {
        let State::AwaitServerFinal(Signature(expected)) = &self.state else {
            return Err(crate::Error::Protocol(Reason::FinalState));
        };
        let encoded = match parser::Parser::new(bytes)
            .server_final()
            .map_err(crate::Error::ServerFinal)?
        {
            parser::ServerFinal::Error(error) => {
                return Err(crate::Error::Scram(error));
            }
            parser::ServerFinal::Verifier(encoded) => encoded,
        };
        let mut actual = Zeroizing::new([0u8; 33]);
        let len = general_purpose::STANDARD
            .decode_slice(encoded.as_slice(), &mut actual[..])
            .map_err(|error| crate::Error::ServerFinalBase64(error.into()))?;
        if len != expected.as_array().len() {
            // SAFETY: successful slice decoding returns an initialized prefix
            // of the fixed 33-byte output, which also retains the u32 bound.
            let bytes =
                Wire::from_raw(unsafe { bounded::raw::Input::new(actual.get_unchecked(..len)) });
            return Err(crate::Error::ServerFinal(Reason::SignatureLength {
                actual: bytes.length(),
                expected: Length::from_u32(32),
            }));
        }
        if !bool::from(expected.as_array().ct_eq(&actual[..32])) {
            return Err(crate::Error::ServerFinal(Reason::SignatureMismatch));
        }
        self.state = State::Complete;
        Ok(())
    }

    pub(super) fn finish(&self) -> Result<(), crate::Error> {
        if !matches!(self.state, State::Complete) {
            return Err(crate::Error::Protocol(Reason::AuthBeforeFinal));
        }
        Ok(())
    }
}

impl Derive<'_> {
    pub(super) const fn key(&self) -> &kdf::KdfKey {
        &self.state.key
    }

    pub(super) fn write_final(
        &self,
        keys: &Keys,
        out: &mut encode::Writer,
    ) -> Result<Signature, encode::Error> {
        let first = &self.state.server_first;
        let nonce = first.nonce();
        let mut proof = Secret32::new([0; 32]);
        hmac_auth_message(
            keys.stored.as_array(),
            &self.nonce.0,
            first.as_slice(),
            nonce,
            &mut proof,
        );
        for (left, right) in std::iter::zip(proof.as_array_mut(), keys.client.as_array()) {
            *left ^= *right;
        }
        let mut encoded = Zeroizing::new([b'='; PROOF_BYTES]);
        general_purpose::STANDARD
            .internal_encode(proof.as_array(), &mut encoded[..PROOF_BYTES - 1]);
        let mut expected = Secret32::new([0; 32]);
        hmac_auth_message(
            keys.server.as_array(),
            &self.nonce.0,
            first.as_slice(),
            nonce,
            &mut expected,
        );

        out.extend(b"c=biws,r=")?;
        out.extend(nonce)?;
        out.extend(b",p=")?;
        out.extend(encoded.as_slice())?;
        Ok(Signature(expected.clone()))
    }
}

fn decode_base64(encoded: Wire<'_>, mut write: impl FnMut(&[u8])) -> Result<(), crate::Error> {
    let mut chunks = (0u32..).zip(encoded.as_slice().chunks(4)).peekable();
    while let Some((index, chunk)) = chunks.next() {
        if chunks.peek().is_some()
            && let Some((position, _)) = (0u32..).zip(chunk).find(|(_, byte)| **byte == b'=')
        {
            let offset = index * 4 + position;
            return Err(crate::Error::ServerFirst(Reason::SaltPadding {
                offset: Offset::from_u32(offset),
            }));
        }
        let mut decoded = [0u8; 3];
        let len = general_purpose::STANDARD
            .decode_slice(chunk, &mut decoded)
            .map_err(|error| {
                let mut error = Base64::from(error);
                let offset = match &mut error {
                    Base64::InvalidByte { index, .. }
                    | Base64::InvalidLength(index)
                    | Base64::InvalidLastSymbol { offset: index, .. } => index,
                    Base64::InvalidPadding | Base64::OutputSliceTooSmall => {
                        return crate::Error::ServerFirstBase64(error);
                    }
                };
                *offset += (index * 4) as usize;
                crate::Error::ServerFirstBase64(error)
            })?;
        write(&decoded[..len]);
    }
    Ok(())
}

fn encode_nonce(input: &[u8; 18]) -> [u8; CLIENT_NONCE_BYTES] {
    let mut output = [0u8; CLIENT_NONCE_BYTES];
    general_purpose::STANDARD.internal_encode(input, &mut output);
    output
}

const IV: [u32; 8] = [
    0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a, 0x510e527f, 0x9b05688c, 0x1f83d9ab, 0x5be0cd19,
];

fn hmac(key: &[u8], inner: &mut [u32; 8], outer: &mut [u32; 8]) {
    let mut block = Zeroizing::new([0u8; 64]);
    if key.len() > block.len() {
        let mut hash = Sha256::new(0);
        hash.update(key);
        hash.finalize();
        write_state(&hash.state, &mut block.as_chunks_mut::<32>().0[0]);
    } else {
        block[..key.len()].copy_from_slice(key);
    }
    for (seed, pad) in [inner, outer].into_iter().zip([0x36, 0x36 ^ 0x5c]) {
        for byte in block.iter_mut() {
            *byte ^= pad;
        }
        compress256(seed, std::slice::from_ref(&*block));
    }
}

struct Sha256 {
    state: Zeroizing<[u32; 8]>,
    block: Zeroizing<[u8; 64]>,
    len: Zeroizing<u64>,
}

impl Sha256 {
    fn new(len: u64) -> Self {
        Self {
            state: Zeroizing::new(IV),
            block: Zeroizing::new([0; 64]),
            len: Zeroizing::new(len),
        }
    }

    fn update(&mut self, mut input: &[u8]) {
        let pos = (*self.len & 63) as usize;
        *self.len = self.len.wrapping_add(input.len() as u64);
        if pos != 0 {
            let take = (64 - pos).min(input.len());
            self.block[pos..pos + take].copy_from_slice(&input[..take]);
            input = &input[take..];
            if pos + take != 64 {
                return;
            }
            compress256(&mut self.state, std::slice::from_ref(&*self.block));
        }
        let (blocks, tail) = input.as_chunks::<64>();
        if !blocks.is_empty() {
            compress256(&mut self.state, blocks);
        }
        self.block[..tail.len()].copy_from_slice(tail);
    }

    fn finalize(&mut self) {
        pad(&mut self.state, &mut self.block, *self.len);
        compress256(&mut self.state, std::slice::from_ref(&*self.block));
    }

    fn hmac(&mut self, outer: &[u32; 8], output: &mut Secret32) {
        self.finalize();
        write_state(&self.state, &mut self.block.as_chunks_mut::<32>().0[0]);
        self.state.copy_from_slice(outer);
        *self.len = 96;
        self.finalize();
        write_state(&self.state, output.as_array_mut());
    }
}

fn pad(state: &mut [u32; 8], block: &mut [u8; 64], len: u64) {
    let pos = (len & 63) as usize;
    block[pos] = 0x80;
    block[pos + 1..].fill(0);
    if pos >= 56 {
        compress256(state, std::slice::from_ref(block));
        block.fill(0);
    }
    block[56..].copy_from_slice(&len.wrapping_mul(8).to_be_bytes());
}

fn write_state(state: &[u32; 8], output: &mut [u8; 32]) {
    for (word, bytes) in state.iter().zip(output.as_chunks_mut::<4>().0) {
        *bytes = word.to_be_bytes();
    }
}

fn hmac_auth_message(
    key: &[u8; 32],
    client: &[u8; CLIENT_NONCE_BYTES],
    first: &[u8],
    server: &[u8],
    output: &mut Secret32,
) {
    let mut mac = Sha256::new(64);
    let mut outer = Zeroizing::new(IV);
    hmac(key, &mut mac.state, &mut outer);
    mac.update(b"n=,r=");
    mac.update(client);
    mac.update(b",");
    mac.update(first);
    mac.update(b",c=biws,r=");
    mac.update(server);
    mac.hmac(&outer, output);
}

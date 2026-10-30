use crate::protocol::{Length, Offset, Reason, Wire, encode};
use crate::scram::Base64;
use base64::Engine as _;
use base64::engine::general_purpose;
use dope::core::{Backend, dope_sys::thread::Thread as _, driver::route};
use dope::{core::driver::route::table::conn::Capacity, manifold::client::conn};
use hmac::digest::consts::U64;
use hmac::{Hmac, Mac as _, digest};
use o3::{alloc, base::cell::region, buffer::bytes::bounded, depot::queue::indexed::shared};
use sha2::{Digest as _, Sha256};
use std::mem;
use zeroize::{Zeroize as _, Zeroizing};

pub(in crate::protocol) mod kdf;
mod parser;
mod secret32;

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
pub(super) struct Signature(digest::CtOutput<Hmac<Sha256>>);

#[derive(Clone)]
pub(in crate::protocol) struct Keys {
    client: secret32::Secret32,
    stored: secret32::Secret32,
    server: secret32::Secret32,
}

impl Keys {
    fn new(salted: kdf::Salted) -> Self {
        let key = hmac::<Hmac<Sha256>>(salted.as_array());
        let client = key.clone().chain_update(b"Client Key").finalize();
        let server = key.chain_update(b"Server Key").finalize();
        let client = secret32::Secret32::copy(client.as_bytes().as_ref());
        Self {
            stored: sha256(client.as_array()),
            client,
            server: secret32::Secret32::copy(server.as_bytes().as_ref()),
        }
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

            let mut first = hmac::<Hmac<Sha256>>(password);
            decode_base64(salt, |decoded| first.update(decoded))?;
            first.update(&1u32.to_be_bytes());
            let output = first.finalize();
            let key = kdf::KdfKey::from_server(output.as_bytes().as_ref(), count)?;
            self.state = State::Deriving(Deriving {
                server_first: parsed.try_retain()?,
                key,
            });
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

    pub(super) fn await_final(&mut self, sig: Signature) {
        self.state = State::AwaitServerFinal(sig);
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
        if len != expected.as_bytes().len() {
            // SAFETY: successful slice decoding returns an initialized prefix
            // of the fixed 33-byte output, which also retains the u32 bound.
            let bytes =
                Wire::from_raw(unsafe { bounded::raw::Input::new(actual.get_unchecked(..len)) });
            return Err(crate::Error::ServerFinal(Reason::SignatureLength {
                actual: bytes.length(),
                expected: Length::from_u32(32),
            }));
        }
        let mut digest = digest::Output::<Hmac<Sha256>>::default();
        digest.copy_from_slice(&actual[..len]);
        if expected != &digest::CtOutput::new(digest) {
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
        keys: Keys,
        out: &mut encode::Writer,
    ) -> Result<Signature, encode::Error> {
        let first = &self.state.server_first;
        let nonce = first.nonce();
        let mask = hmac_auth_message(
            keys.stored.as_array(),
            &self.nonce.0,
            first.as_slice(),
            nonce,
        );
        let mut proof = keys.client;
        for (left, right) in proof.iter_mut().zip(mask.as_bytes()) {
            *left ^= *right;
        }
        let encoded = Zeroizing::new(encode_proof(&proof));
        let expected = hmac_auth_message(
            keys.server.as_array(),
            &self.nonce.0,
            first.as_slice(),
            nonce,
        );

        out.extend(b"c=biws,r=")?;
        out.extend(nonce)?;
        out.extend(b",p=")?;
        out.extend(encoded.as_slice())?;
        Ok(Signature(expected))
    }
}

pub(crate) struct Runtime<'d, const ID: u8> {
    waiters: shared::Fifo<conn::Id<'d, route::KeyTag<ID>>>,
    state: region::Value<region::Identity<'d>, kdf::Coordinator<'d, ID>>,
}

impl<'d, const ID: u8> Runtime<'d, ID> {
    pub(in crate::protocol) fn try_new(
        cap: Capacity,
        proof: region::Proof<region::Identity<'d>>,
    ) -> Result<Self, alloc::Error> {
        Ok(Self {
            waiters: shared::Fifo::try_new(cap.entries().get())?,
            state: proof.bind(kdf::Coordinator::new()),
        })
    }

    fn try_entry(
        &self,
        owner: conn::Id<'d, route::KeyTag<ID>>,
    ) -> Result<kdf::Ticket<'_, 'd, ID>, shared::Bounds> {
        let waiter = self.waiters.try_entry(owner.index().into_usize())?;
        Ok(kdf::Ticket {
            runtime: self,
            waiter,
            owner,
        })
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

fn encode_proof(input: &secret32::Secret32) -> [u8; PROOF_BYTES] {
    let mut output = [b'='; PROOF_BYTES];
    general_purpose::STANDARD.internal_encode(input.as_array(), &mut output[..PROOF_BYTES - 1]);
    output
}

fn hmac<M>(key: &[u8]) -> M
where
    M: hmac::KeyInit<KeySize = U64>,
{
    let mut block = digest::Key::<M>::default();
    if key.len() > block.len() {
        let mut hash = sha2::Sha256::digest(key);
        block[..hash.len()].copy_from_slice(hash.as_slice());
        hash.as_mut_slice().zeroize();
    } else {
        block[..key.len()].copy_from_slice(key);
    }
    let output = M::new(&block);
    block.as_mut_slice().zeroize();
    output
}

fn sha256(input: &[u8; 32]) -> secret32::Secret32 {
    let mut hash = sha2::Sha256::new();
    hash.update(input);
    let mut output = hash.finalize();
    let secret = secret32::Secret32::copy(output.as_ref());
    output.as_mut_slice().zeroize();
    secret
}

fn hmac_auth_message(
    key: &[u8; 32],
    client: &[u8; CLIENT_NONCE_BYTES],
    first: &[u8],
    server: &[u8],
) -> digest::CtOutput<Hmac<Sha256>> {
    let mut mac = hmac::<Hmac<Sha256>>(key);
    mac.update(b"n=,r=");
    mac.update(client);
    mac.update(b",");
    mac.update(first);
    mac.update(b",c=biws,r=");
    mac.update(server);
    mac.finalize()
}

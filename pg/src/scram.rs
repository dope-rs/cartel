use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use hmac::{Hmac, KeyInit, Mac};
use rand::TryRng;
use sha2::{Digest, Sha256};

use crate::Error;

const MECH: &str = "SCRAM-SHA-256";
const MECH_PLUS: &str = "SCRAM-SHA-256-PLUS";
const MAX_SCRAM_ITERATIONS: u32 = 1_000_000;
const GS2_HEADER: &str = "n,,";
const GS2_HEADER_B64: &str = "biws";

type Hs = Hmac<Sha256>;

pub(super) struct Scram {
    password: Vec<u8>,
    client_nonce: String,
    client_first_bare: String,
    auth_message: String,
    server_signature: [u8; 32],
}

impl Scram {
    pub(super) fn new(password: &str) -> Result<Self, Error> {
        let mut nonce_raw = [0u8; 18];
        rand::rngs::SysRng
            .try_fill_bytes(&mut nonce_raw)
            .map_err(|error| Error::Auth(format!("OS RNG unavailable: {error}")))?;
        let client_nonce = STANDARD.encode(nonce_raw);
        let client_first_bare = format!("n=,r={client_nonce}");
        Ok(Self {
            password: password.as_bytes().to_vec(),
            client_nonce,
            client_first_bare,
            auth_message: String::new(),
            server_signature: [0u8; 32],
        })
    }

    pub(super) fn pick_mechanism(&self, offered: &[&str]) -> Result<&'static str, Error> {
        if offered.contains(&MECH) {
            Ok(MECH)
        } else if offered.contains(&MECH_PLUS) {
            Err(Error::Auth(
                "server only offers SCRAM-SHA-256-PLUS (channel binding) which is not supported"
                    .into(),
            ))
        } else {
            Err(Error::Auth(format!(
                "no compatible SASL mechanism offered: {:?}",
                offered
            )))
        }
    }

    pub(super) fn client_first(&self) -> String {
        format!("{GS2_HEADER}{}", self.client_first_bare)
    }

    pub(super) fn client_final(&mut self, server_first: &[u8]) -> Result<String, Error> {
        let server_first_str = std::str::from_utf8(server_first)
            .map_err(|_| Error::Auth("server-first not utf-8".into()))?;
        let mut nonce_b64 = "";
        let mut salt_b64 = "";
        let mut iterations = 0u32;
        for attr in server_first_str.split(',') {
            if let Some(v) = attr.strip_prefix("r=") {
                nonce_b64 = v;
            } else if let Some(v) = attr.strip_prefix("s=") {
                salt_b64 = v;
            } else if let Some(v) = attr.strip_prefix("i=") {
                iterations = v
                    .parse()
                    .map_err(|_| Error::Auth("server-first: bad iteration count".into()))?;
            }
        }
        if nonce_b64.is_empty() || salt_b64.is_empty() || iterations == 0 {
            return Err(Error::Auth("server-first missing fields".into()));
        }
        if iterations > MAX_SCRAM_ITERATIONS {
            return Err(Error::Auth(
                "server-first: iteration count too large".into(),
            ));
        }
        if !nonce_b64.starts_with(&self.client_nonce) {
            return Err(Error::Auth(
                "server nonce does not extend client nonce".into(),
            ));
        }
        let salt = STANDARD
            .decode(salt_b64.as_bytes())
            .map_err(|_| Error::Auth("server-first: bad base64 salt".into()))?;

        let salted = pbkdf2_sha256_32(&self.password, &salt, iterations);
        let client_key = hmac_sha256(&salted, b"Client Key");
        let stored_key = sha256(&client_key);
        let server_key = hmac_sha256(&salted, b"Server Key");

        let client_final_no_proof = format!("c={GS2_HEADER_B64},r={nonce_b64}");
        self.auth_message = format!(
            "{},{},{}",
            self.client_first_bare, server_first_str, client_final_no_proof
        );

        let client_signature = hmac_sha256(&stored_key, self.auth_message.as_bytes());
        let mut client_proof = client_key;
        for (a, b) in client_proof.iter_mut().zip(client_signature.iter()) {
            *a ^= *b;
        }
        let client_proof_b64 = STANDARD.encode(client_proof);
        let server_signature_v = hmac_sha256(&server_key, self.auth_message.as_bytes());
        self.server_signature = server_signature_v;

        Ok(format!("{client_final_no_proof},p={client_proof_b64}"))
    }

    pub(super) fn verify_server_final(&self, server_final: &[u8]) -> Result<(), Error> {
        let s = std::str::from_utf8(server_final)
            .map_err(|_| Error::Auth("server-final not utf-8".into()))?;
        let v_b64 = s
            .split(',')
            .find_map(|attr| attr.strip_prefix("v="))
            .ok_or(Error::Auth("server-final missing v=".into()))?;
        let sig = STANDARD
            .decode(v_b64.as_bytes())
            .map_err(|_| Error::Auth("server-final: bad base64 v=".into()))?;
        if sig.as_slice() != self.server_signature.as_slice() {
            return Err(Error::Auth("server signature mismatch".into()));
        }
        Ok(())
    }
}

fn sha256(input: &[u8]) -> [u8; 32] {
    let mut h = Sha256::new();
    h.update(input);
    h.finalize().into()
}

fn hmac_sha256(key: &[u8], data: &[u8]) -> [u8; 32] {
    let mut m = <Hs as KeyInit>::new_from_slice(key).expect("hmac key length always valid");
    m.update(data);
    m.finalize().into_bytes().into()
}

fn pbkdf2_sha256_32(password: &[u8], salt: &[u8], iterations: u32) -> [u8; 32] {
    let mut u = {
        let mut m =
            <Hs as KeyInit>::new_from_slice(password).expect("hmac key length always valid");
        m.update(salt);
        m.update(&1u32.to_be_bytes());
        m.finalize().into_bytes()
    };
    let mut t = u;
    for _ in 1..iterations {
        u = {
            let mut m =
                <Hs as KeyInit>::new_from_slice(password).expect("hmac key length always valid");
            m.update(&u);
            m.finalize().into_bytes()
        };
        for (a, b) in t.iter_mut().zip(u.iter()) {
            *a ^= *b;
        }
    }
    t.into()
}

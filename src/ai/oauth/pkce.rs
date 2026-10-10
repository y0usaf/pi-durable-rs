use ring::digest::{self, SHA256};
use ring::rand::{SecureRandom, SystemRandom};

const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";

/// A PKCE pair: the secret `verifier` and its S256 `challenge`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Pkce {
    pub verifier: String,
    pub challenge: String,
}

/// A fresh 32-byte random verifier and its challenge.
pub fn generate() -> Result<Pkce, String> {
    let mut bytes = [0u8; 32];
    SystemRandom::new()
        .fill(&mut bytes)
        .map_err(|_| "system random source failed".to_owned())?;
    let verifier = base64url(&bytes);
    let challenge = challenge(&verifier);
    Ok(Pkce {
        verifier,
        challenge,
    })
}

/// The S256 challenge for a verifier: base64url(SHA-256(verifier)).
pub fn challenge(verifier: &str) -> String {
    base64url(digest::digest(&SHA256, verifier.as_bytes()).as_ref())
}

/// Unpadded base64url (RFC 4648 section 5).
pub fn base64url(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 4 / 3 + 3);
    for chunk in bytes.chunks(3) {
        let b0 = chunk[0];
        let b1 = chunk.get(1).copied().unwrap_or(0);
        let b2 = chunk.get(2).copied().unwrap_or(0);
        let n = (u32::from(b0) << 16) | (u32::from(b1) << 8) | u32::from(b2);
        for shift in 0..=chunk.len() {
            let index = (n >> (18 - 6 * shift)) & 63;
            out.push(ALPHABET[index as usize] as char);
        }
    }
    out
}

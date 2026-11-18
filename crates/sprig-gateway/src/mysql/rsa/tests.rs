//! The key pair against `mysql_common`'s client-side encryption (what
//! `mysql_async` sends), PEM round trips, and refusals.

use std::sync::OnceLock;

use super::*;

/// One generated key for the module's tests (generation takes a moment in
/// a debug build).
fn key() -> &'static RsaKeyPair {
    static KEY: OnceLock<RsaKeyPair> = OnceLock::new();
    KEY.get_or_init(|| RsaKeyPair::generate(GENERATED_BITS).expect("generate"))
}

fn private_exponent(k: &RsaKeyPair) -> BigUint {
    let phi = (&k.p - 1u32) * (&k.q - 1u32);
    k.e.modinv(&phi).expect("d")
}

/// The key as PKCS#1 DER, with `p` replaced by `p + bump`.
fn pkcs1_der_bumped(k: &RsaKeyPair, bump: u32) -> Vec<u8> {
    let d = private_exponent(k);
    let ints = [
        BigUint::from(0u32),
        k.n.clone(),
        k.e.clone(),
        d.clone(),
        &k.p + bump,
        k.q.clone(),
        &d % (&k.p - 1u32),
        &d % (&k.q - 1u32),
        k.qinv.clone(),
    ];
    der_tlv(0x30, &ints.iter().flat_map(der_uint).collect::<Vec<u8>>())
}

/// The key as PKCS#1 DER.
fn pkcs1_der(k: &RsaKeyPair) -> Vec<u8> {
    pkcs1_der_bumped(k, 0)
}

fn pem(label: &str, der: &[u8]) -> String {
    let b64 = STANDARD.encode(der);
    let mut s = format!("-----BEGIN {label}-----\n");
    for c in b64.as_bytes().chunks(64) {
        s.push_str(std::str::from_utf8(c).expect("ascii"));
        s.push('\n');
    }
    s.push_str("-----END ");
    s.push_str(label);
    s.push_str("-----\n");
    s
}

#[test]
fn a_generated_key_has_the_size_asked_for() {
    let k = key();
    assert_eq!(k.n.bits(), GENERATED_BITS);
    assert_eq!(k.size(), 256);
    assert_eq!(k.e, BigUint::from(65_537u32));
}

#[test]
fn the_public_pem_is_what_clients_parse() {
    let k = key();
    let pem = k.public_pem();
    let text = std::str::from_utf8(&pem).expect("utf8");
    assert!(text.starts_with("-----BEGIN PUBLIC KEY-----\n"));
    assert!(text.ends_with("-----END PUBLIC KEY-----\n"));
    let parsed = mysql_common::crypto::rsa::PublicKey::from_pem(&pem);
    assert_eq!(parsed.modulus(), &k.n);
    assert_eq!(parsed.exponent(), &k.e);
}

#[test]
fn what_a_client_encrypts_decrypts() {
    let k = key();
    let pem = k.public_pem();
    for plain in [
        &b"s\0"[..],
        b"a longer password, with spaces\0",
        &[0u8; 1],
        &[0xA5; 200],
    ] {
        let ct = mysql_common::crypto::encrypt(plain, &pem);
        assert_eq!(ct.len(), k.size());
        let got = k.decrypt(&ct).expect("decrypts");
        assert_eq!(&got[..], plain);
    }
}

#[test]
fn a_damaged_or_foreign_ciphertext_doesnt_decrypt() {
    let k = key();
    let ct = mysql_common::crypto::encrypt(b"secret\0", &k.public_pem());
    // Any byte changed.
    for i in [0, 1, 100, ct.len() - 1] {
        let mut bad = ct.clone();
        bad[i] ^= 0x01;
        assert!(k.decrypt(&bad).is_none(), "byte {i}");
    }
    // Wrong lengths, and a value above the modulus.
    assert!(k.decrypt(&ct[1..]).is_none());
    assert!(k.decrypt(&[ct.clone(), vec![0]].concat()).is_none());
    assert!(k.decrypt(&vec![0xFF; k.size()]).is_none());
    // A clear-text password of the key's length.
    assert!(k.decrypt(&[b'p'; 256]).is_none());
}

#[test]
fn pkcs1_and_pkcs8_files_load_the_same_key() {
    let k = key();
    let pkcs1 = pkcs1_der(k);
    let from1 = RsaKeyPair::from_pem(pem("RSA PRIVATE KEY", &pkcs1).as_bytes()).expect("pkcs1");
    assert_eq!(from1.public_pem(), k.public_pem());
    let alg = der_tlv(
        0x30,
        &[der_tlv(0x06, RSA_ENCRYPTION_OID), vec![0x05, 0x00]].concat(),
    );
    let pkcs8 = der_tlv(
        0x30,
        &[der_uint(&BigUint::from(0u32)), alg, der_tlv(0x04, &pkcs1)].concat(),
    );
    let from8 = RsaKeyPair::from_pem(pem("PRIVATE KEY", &pkcs8).as_bytes()).expect("pkcs8");
    assert_eq!(from8.public_pem(), k.public_pem());
    // The loaded key decrypts what the generated one's clients send.
    let ct = mysql_common::crypto::encrypt(b"pw\0", &k.public_pem());
    assert_eq!(&from8.decrypt(&ct).expect("decrypts")[..], b"pw\0");
}

#[test]
fn unusable_key_files_are_refused_with_a_reason() {
    let k = key();
    let cases: Vec<(String, &str)> =
        vec![
        ("hello".into(), "no \"-----BEGIN"),
        (
            "-----BEGIN ENCRYPTED PRIVATE KEY-----\nAAAA\n-----END ENCRYPTED PRIVATE KEY-----\n"
                .into(),
            "encrypted",
        ),
        (pem("RSA PRIVATE KEY", b"\x30\x03\x02\x01\x00"), "PKCS#1"),
        ("-----BEGIN RSA PRIVATE KEY-----\n!!\n-----END RSA PRIVATE KEY-----\n".into(), "base64"),
        ("-----BEGIN RSA PRIVATE KEY-----\nAAAA\n".into(), "no \"-----END"),
    ];
    for (text, want) in cases {
        let e = RsaKeyPair::from_pem(text.as_bytes()).expect_err(want);
        assert!(e.contains(want), "{e} should mention {want}");
    }
    // A wrong prime: the values don't fit.
    let der = pkcs1_der_bumped(k, 2);
    let e = RsaKeyPair::from_pem(pem("RSA PRIVATE KEY", &der).as_bytes()).expect_err("bad");
    assert!(e.contains("don't fit"), "{e}");
}

#[test]
fn a_small_key_is_refused() {
    let small = RsaKeyPair::generate(1024).expect("generate");
    let der = pkcs1_der(&small);
    let e = RsaKeyPair::from_pem(pem("RSA PRIVATE KEY", &der).as_bytes()).expect_err("small");
    assert!(e.contains("too small"), "{e}");
}

#[tokio::test]
async fn a_key_file_is_read_again_when_it_changes() {
    let dir = std::env::temp_dir().join(format!("sprig-rsa-test-{}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("dir");
    let path = dir.join("key.pem");
    let path_s = path.to_str().expect("utf8").to_owned();
    // Missing: no key.
    assert!(key_for(&path_s).await.is_none());
    std::fs::write(&path, pem("RSA PRIVATE KEY", &pkcs1_der(key()))).expect("write");
    let got = key_for(&path_s).await.expect("loaded");
    assert_eq!(got.public_pem(), key().public_pem());
    // Replaced by garbage of another length: refused until fixed.
    std::fs::write(&path, "not a key").expect("write");
    assert!(key_for(&path_s).await.is_none());
    let _ = std::fs::remove_dir_all(&dir);
}

//! External tokens: signatures with locally made keys, a JWKS served by
//! the test itself, the refusals, key rotation and outages, mappings, and
//! the logins over MySQL, HTTP and MongoDB against a prebuilt catalog.

#![allow(clippy::unwrap_used, clippy::too_many_lines, clippy::expect_used)]

use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use base64::Engine as _;
use base64::engine::general_purpose::{STANDARD, URL_SAFE_NO_PAD};
use ring::rand::SystemRandom;
use ring::signature::{self, EcdsaKeyPair, Ed25519KeyPair, KeyPair, RsaKeyPair};
use serde_json::{Value, json};
use sprig_common::ids::{ClusterId, TenantId, UserId};
use sprig_common::time::{Clock, ManualClock};
use sprig_proto_mysql::Persona;
use sprig_proto_mysql::auth::{AuthPath, AuthVerdict, LoginFlow, Outcome};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use zeroize::Zeroizing;

use super::*;
use crate::account::{StaticTenants, TenantInfo};
use crate::accounts::BackendTarget;
use crate::cache::TenantAuth;
use crate::cache::tests::row;
use crate::mysql::tests::{Catalogs, snapshot};
use crate::mysql::{
    CatalogLogins, FixedPolicy, LoginRequest, LoginService, LoginSource, StaticBackends,
};
use crate::store::AuthMethods;

/// A 2048-bit RSA key made for these tests with `openssl genpkey` (PKCS#8,
/// DER, base64): ring signs with RSA keys but doesn't make them.
const RSA_PKCS8: &[&str] = &[
    "MIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQDP7U2knCql0BXr2DG7I8Klt7YJ",
    "aczTKWnMevc5GN/jd542U6TMdUIrw0F+/+JUhWy4+b1baxLcmBnBKU/a7+FY1zQme7i9ZYpj2H+m",
    "LcbUj40lpIMj5hrb7vwVqR3yORe4/ZLBg9BL1G6tuxjZw5cq8Oo67Z5qqkFgVTG3nnDjMab0mTEA",
    "SeUr3piH271tcn3RBvm/Lp5dlUcnXi+rHV2qxHrVKNc0k2E9P9t20SL/7cznGU+Oe688q4mD2Jpy",
    "bBV9L29uKUYNAgrqrNstTl4PFKH9YSQh/fbePWwmlJ19GQDjupCyHZhh5CVjzh4FXt1MPEnZf1z4",
    "0WmU74g5k1ajAgMBAAECggEAJhdNEAjj7zhtq6URhz0H3aAIIzVUpqKBlUo5nCYobd1uCHnOSP56",
    "PtYoW4G5Ds7amhB2VhcJf4wFu9M7o9oqKEJUdXBY6al+bsgsql7ce/hIXUsEE0GGTiA74iP3Lnaz",
    "F9qsVfM1545lMEKETPB98wnrfxwk11mVXHZ/Ayptd9E+QVk1NLWY0DODrH4U7CXmoEeSXQIWIY9Z",
    "NfSxF+ILm+TJcgUlWBJDkIT3CRm5J6DzR18Tmhz8OPsut9/oZ2eUH6Gy5qLqaT6thBlKCve9shFX",
    "IgohOW1UuKY1K4R9RhElBgRwvP0tlZzbo2mozHsxsk+hB4PgOGwUSZxxZtf44QKBgQDo+1peoKm7",
    "OUORsbV+c46INCsS5PhcvdWDUihSAcEW0hIDglwrDFVkYYpfQnYQ+H5iYII4CV8M9EQA1TFXqRfW",
    "0k52WEwqrSHIwTrCshHq+Sty814ovbEPhmluFAHyVO+Zd9pz3dx3bSIGfkEayq+jf/WE464lVP5s",
    "sz853N6ZCwKBgQDkeEEytgz2WeK3WG+mr0lErGCvh22AG2OXigdcLZRS1MPsEbXJFmHRTPazC4cp",
    "B1kCCaymUFjXjC45mGEPipkgiLlCQfaAUAgHw4x9dcg6EqXSNqJn7wOM73SDbi6w5vK1m7E6sWo6",
    "A8KQKC2GciqNZLSNdEi3GbtutWhBq0WnyQKBgFfTULi1NeaG6qrEY9d8NjGwAMDJobpMMW2kjeCE",
    "GsbAf9vky1vK9XG2t1O7mQhCtFnfaZc4KSWX+v6ZX1VdP8hSVROVq3i29TJZbQ4x1EFQlolppogT",
    "IejpUVsWnbk9PWkL3kSpUM+QSzBJbb9swpMOzH3A2kfmZ1aQp3fng94LAoGBAMrVNFTYKSPg4xWR",
    "LvaVsf+E5gWPvxg3uLBFu4kLch4jhJklk/UlqMwPfwmv11wGiO/1NNEmjek3K6a4cJtCmfW3THnV",
    "ZAnAg5CY/DY6BfwAqDS7im2pVcaepBs3C77EGoA+7BZGgh9lTSt8cAZnfv2uhVj7/r6H2s5LwXZj",
    "nuB5AoGASQnVUrBKV0e39v9s+5Mom4RMZYYjQoUT707gw4agTbXfKOerBJCSO94i1kS93AhYfMke",
    "EenV0e/y1FjPH5SqAvOBcMqF7p3BzRic9DyIDI5A5BBiw8kB8HAi6hDO9jSFBLsyF1lAluqrdop8",
    "5osZrZOAHMqlCGBNXnEYM99EDaw=",
];

const ISS: &str = "https://idp.test";
const AUD: &str = "sprig-app";

enum Signer {
    Rsa(RsaKeyPair),
    Ec(EcdsaKeyPair),
    Ed(Ed25519KeyPair),
}

struct Key {
    kid: String,
    alg: &'static str,
    signer: Signer,
}

impl Key {
    fn rsa(kid: &str) -> Self {
        let der = STANDARD.decode(RSA_PKCS8.concat()).unwrap();
        Self {
            kid: kid.into(),
            alg: "RS256",
            signer: Signer::Rsa(RsaKeyPair::from_pkcs8(&der).unwrap()),
        }
    }

    fn ec(kid: &str) -> Self {
        let rng = SystemRandom::new();
        let a = &signature::ECDSA_P256_SHA256_FIXED_SIGNING;
        let der = EcdsaKeyPair::generate_pkcs8(a, &rng).unwrap();
        Self {
            kid: kid.into(),
            alg: "ES256",
            signer: Signer::Ec(EcdsaKeyPair::from_pkcs8(a, der.as_ref(), &rng).unwrap()),
        }
    }

    fn ed(kid: &str) -> Self {
        let rng = SystemRandom::new();
        let der = Ed25519KeyPair::generate_pkcs8(&rng).unwrap();
        Self {
            kid: kid.into(),
            alg: "EdDSA",
            signer: Signer::Ed(Ed25519KeyPair::from_pkcs8(der.as_ref()).unwrap()),
        }
    }

    fn jwk(&self) -> Value {
        let b = |x: &[u8]| URL_SAFE_NO_PAD.encode(x);
        match &self.signer {
            Signer::Rsa(k) => {
                let c = signature::RsaPublicKeyComponents::<Vec<u8>>::from(k.public());
                json!({"kty": "RSA", "kid": self.kid, "use": "sig", "alg": "RS256",
                       "n": b(&c.n), "e": b(&c.e)})
            }
            Signer::Ec(k) => {
                let p = k.public_key().as_ref();
                json!({"kty": "EC", "kid": self.kid, "crv": "P-256",
                       "x": b(&p[1..33]), "y": b(&p[33..65])})
            }
            Signer::Ed(k) => json!({"kty": "OKP", "kid": self.kid, "crv": "Ed25519",
                                    "x": b(k.public_key().as_ref())}),
        }
    }

    fn sign_with(&self, header: &Value, claims: &Value) -> String {
        let h = URL_SAFE_NO_PAD.encode(header.to_string());
        let p = URL_SAFE_NO_PAD.encode(claims.to_string());
        let msg = format!("{h}.{p}");
        let rng = SystemRandom::new();
        let sig = match &self.signer {
            Signer::Rsa(k) => {
                let mut s = vec![0; k.public().modulus_len()];
                k.sign(&signature::RSA_PKCS1_SHA256, &rng, msg.as_bytes(), &mut s)
                    .unwrap();
                s
            }
            Signer::Ec(k) => k.sign(&rng, msg.as_bytes()).unwrap().as_ref().to_vec(),
            Signer::Ed(k) => k.sign(msg.as_bytes()).as_ref().to_vec(),
        };
        format!("{msg}.{}", URL_SAFE_NO_PAD.encode(sig))
    }

    fn sign(&self, claims: &Value) -> String {
        self.sign_with(&json!({"alg": self.alg, "kid": self.kid, "typ": "JWT"}), claims)
    }
}

fn jwks(keys: &[&Key]) -> String {
    json!({"keys": keys.iter().map(|k| k.jwk()).collect::<Vec<_>>()}).to_string()
}

/// A JWKS (and discovery) server on 127.0.0.1, run by the test.
struct Idp {
    base: String,
    jwks: Arc<Mutex<String>>,
    hits: Arc<Mutex<HashMap<String, usize>>>,
    down: Arc<AtomicBool>,
}

impl Idp {
    async fn start(keys: &[&Key]) -> Self {
        let l = tokio::net::TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
            .await
            .unwrap();
        let base = format!("http://{}", l.local_addr().unwrap());
        let jwks = Arc::new(Mutex::new(self::jwks(keys)));
        let hits: Arc<Mutex<HashMap<String, usize>>> = Arc::default();
        let down = Arc::new(AtomicBool::new(false));
        let (j, h, d, b) = (
            Arc::clone(&jwks),
            Arc::clone(&hits),
            Arc::clone(&down),
            base.clone(),
        );
        tokio::spawn(async move {
            loop {
                let Ok((mut s, _)) = l.accept().await else {
                    return;
                };
                let (j, h, d, b) = (Arc::clone(&j), Arc::clone(&h), Arc::clone(&d), b.clone());
                tokio::spawn(async move {
                    let mut buf = Vec::new();
                    let mut chunk = [0u8; 1024];
                    while !buf.windows(4).any(|w| w == b"\r\n\r\n") {
                        match s.read(&mut chunk).await {
                            Ok(0) | Err(_) => return,
                            Ok(n) => buf.extend_from_slice(&chunk[..n]),
                        }
                    }
                    let req = String::from_utf8_lossy(&buf);
                    let path = req.split_whitespace().nth(1).unwrap_or("").to_owned();
                    *h.lock().unwrap().entry(path.clone()).or_default() += 1;
                    let (status, body) = if d.load(Ordering::SeqCst) {
                        ("500 Internal Server Error", String::new())
                    } else if path == "/jwks" {
                        ("200 OK", j.lock().unwrap().clone())
                    } else if path == "/.well-known/openid-configuration" {
                        (
                            "200 OK",
                            json!({"issuer": b, "jwks_uri": format!("{b}/jwks")}).to_string(),
                        )
                    } else {
                        ("404 Not Found", String::new())
                    };
                    let resp = format!(
                        "HTTP/1.1 {status}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                        body.len()
                    );
                    let _ = s.write_all(resp.as_bytes()).await;
                    let _ = s.shutdown().await;
                });
            }
        });
        Self {
            base,
            jwks,
            hits,
            down,
        }
    }

    fn url(&self) -> String {
        format!("{}/jwks", self.base)
    }

    fn set(&self, keys: &[&Key]) {
        *self.jwks.lock().unwrap() = jwks(keys);
    }

    fn hits(&self, path: &str) -> usize {
        self.hits.lock().unwrap().get(path).copied().unwrap_or(0)
    }
}

fn spec(idp: &Idp, algs: &[&str]) -> IssuerSpec {
    IssuerSpec {
        issuer_id: Some(1),
        kind: IssuerKind::Oidc,
        issuer: ISS.into(),
        jwks_url: Some(idp.url()),
        audiences: vec![AUD.into()],
        algorithms: algs.iter().map(|a| (*a).to_owned()).collect(),
        protocols: TokenProtocol::ALL.to_vec(),
        client_id: Some("sprig-cli".into()),
        request_scopes: Some(vec!["openid".into()]),
        jit_users: false,
        max_lifetime_s: 3600,
        active: true,
        mappings: vec![
            ClaimMapping {
                priority: 10,
                kind: MappingKind::User,
                claim: "preferred_username".into(),
                match_kind: MatchKind::Any,
                match_value: None,
                target: "{preferred_username}".into(),
            },
            ClaimMapping {
                priority: 20,
                kind: MappingKind::Role,
                claim: "groups".into(),
                match_kind: MatchKind::Equals,
                match_value: Some("admins".into()),
                target: "app_admin".into(),
            },
        ],
    }
}

fn unix(t: SystemTime) -> i64 {
    i64::try_from(t.duration_since(UNIX_EPOCH).unwrap().as_secs()).unwrap()
}

fn claims(now: SystemTime, user: &str) -> Value {
    let n = unix(now);
    json!({"iss": ISS, "aud": AUD, "sub": format!("u-{user}"), "preferred_username": user,
           "groups": ["staff", "admins"], "org": "o-7", "iat": n, "exp": n + 600})
}

fn check<'a>(issuers: &'a [Arc<Issuer>], now: SystemTime) -> Check<'a> {
    Check {
        issuers,
        protocol: TokenProtocol::Http,
        skew: Duration::from_secs(60),
        jwks_refresh: Duration::from_secs(3600),
        now,
        cache: None,
    }
}

fn tokens() -> ExternalTokens {
    ExternalTokens::new(
        JwksCache::new(Arc::new(HttpFetcher::default())).with_min_refetch(Duration::ZERO),
    )
}

#[tokio::test]
async fn every_algorithm_verifies_with_a_local_jwks() {
    let (rsa, ec, ed) = (Key::rsa("r1"), Key::ec("e1"), Key::ed("d1"));
    let idp = Idp::start(&[&rsa, &ec, &ed]).await;
    let issuers = vec![Arc::new(Issuer::new(spec(&idp, &["RS256", "ES256", "EdDSA"])))];
    let t = tokens();
    let now = SystemTime::now();
    for k in [&rsa, &ec, &ed] {
        let v = t
            .verify(&k.sign(&claims(now, "alice")), &check(&issuers, now))
            .await
            .unwrap_or_else(|e| panic!("{}: {e:?}", k.alg));
        assert_eq!(v.user.as_deref(), Some("alice"));
        assert_eq!(v.roles, vec!["app_admin".to_owned()]);
        assert_eq!(v.claims["org"], json!("o-7"));
        assert_eq!(v.issuer, ISS);
    }
    // One fetch served all three (the key set was fresh).
    assert_eq!(idp.hits("/jwks"), 1);
}

#[tokio::test]
async fn bad_tokens_are_refused_with_their_reason() {
    let rsa = Key::rsa("r1");
    let other = Key::ec("e1");
    let idp = Idp::start(&[&rsa]).await;
    let issuers = vec![Arc::new(Issuer::new(spec(&idp, &["RS256"])))];
    let t = tokens();
    let now = SystemTime::now();
    let n = unix(now);
    let with = |f: &dyn Fn(&mut Value)| {
        let mut c = claims(now, "alice");
        f(&mut c);
        rsa.sign(&c)
    };
    let cases: Vec<(&str, String, Refusal)> = vec![
        ("expired", with(&|c| {
            c["iat"] = json!(n - 1000);
            c["exp"] = json!(n - 100);
        }), Refusal::Expired),
        ("wrong audience", with(&|c| c["aud"] = json!("someone-else")), Refusal::Audience),
        ("audience list", with(&|c| c["aud"] = json!(["x", "y"])), Refusal::Audience),
        ("wrong issuer", with(&|c| c["iss"] = json!("https://evil.test")), Refusal::UnknownIssuer),
        ("not yet valid", with(&|c| c["nbf"] = json!(n + 600)), Refusal::NotYetValid),
        ("issued in the future", with(&|c| c["iat"] = json!(n + 600)), Refusal::NotYetValid),
        ("too long", with(&|c| c["exp"] = json!(n + 7200)), Refusal::Lifetime),
        ("no exp", with(&|c| {
            c.as_object_mut().unwrap().remove("exp");
        }), Refusal::Malformed("no exp".into())),
        ("another key, same kid", other.sign_with(
            &json!({"alg": "ES256", "kid": "r1"}), &claims(now, "alice")), Refusal::Algorithm),
        ("unknown kid", rsa.sign_with(
            &json!({"alg": "RS256", "kid": "nope"}), &claims(now, "alice")), Refusal::UnknownKey),
    ];
    for (what, tok, want) in cases {
        let got = t.verify(&tok, &check(&issuers, now)).await;
        assert_eq!(got.as_ref().err(), Some(&want), "{what}: {got:?}");
    }
    // A forged signature: the claims changed after signing.
    let tok = rsa.sign(&claims(now, "alice"));
    let mut parts: Vec<String> = tok.split('.').map(str::to_owned).collect();
    parts[1] = URL_SAFE_NO_PAD.encode(claims(now, "mallory").to_string());
    let forged = parts.join(".");
    assert_eq!(
        t.verify(&forged, &check(&issuers, now)).await.err(),
        Some(Refusal::Signature)
    );
    // `none` and HMAC never verify, whatever the allow-list says.
    let body = URL_SAFE_NO_PAD.encode(claims(now, "alice").to_string());
    for alg in ["none", "HS256"] {
        let h = URL_SAFE_NO_PAD.encode(json!({"alg": alg, "kid": "r1"}).to_string());
        let tok = format!("{h}.{body}.{}", URL_SAFE_NO_PAD.encode(b"sig"));
        assert_eq!(
            t.verify(&tok, &check(&issuers, now)).await.err(),
            Some(Refusal::Algorithm),
            "{alg}"
        );
    }
    // A disabled issuer, and one not taking the protocol.
    let mut off = spec(&idp, &["RS256"]);
    off.active = false;
    let issuers_off = vec![Arc::new(Issuer::new(off))];
    let tok = rsa.sign(&claims(now, "alice"));
    assert_eq!(
        t.verify(&tok, &check(&issuers_off, now)).await.err(),
        Some(Refusal::UnknownIssuer)
    );
    let mut mysql_only = spec(&idp, &["RS256"]);
    mysql_only.protocols = vec![TokenProtocol::Mysql];
    let issuers_m = vec![Arc::new(Issuer::new(mysql_only))];
    assert_eq!(
        t.verify(&tok, &check(&issuers_m, now)).await.err(),
        Some(Refusal::UnknownIssuer)
    );
    assert!(matches!(
        t.verify("eyJ.not.atoken", &check(&issuers, now)).await,
        Err(Refusal::Malformed(_))
    ));
}

#[tokio::test]
async fn a_rotated_key_is_fetched_once_and_rate_limited() {
    let (k1, k2) = (Key::ec("k1"), Key::ec("k2"));
    let idp = Idp::start(&[&k1]).await;
    let issuers = vec![Arc::new(Issuer::new(spec(&idp, &["ES256"])))];
    let t = ExternalTokens::new(JwksCache::new(Arc::new(HttpFetcher::default())));
    let now = SystemTime::now();
    t.verify(&k1.sign(&claims(now, "a")), &check(&issuers, now))
        .await
        .unwrap();
    assert_eq!(idp.hits("/jwks"), 1);
    // The issuer rotates to k2; a k2 token is unknown until a refetch, which
    // the once-a-minute limit holds back here (the first fetch was just now).
    idp.set(&[&k1, &k2]);
    assert_eq!(
        t.verify(&k2.sign(&claims(now, "a")), &check(&issuers, now))
            .await
            .err(),
        Some(Refusal::UnknownKey)
    );
    assert_eq!(idp.hits("/jwks"), 1);
    // Without the limit, the unknown kid refetches once and verifies.
    idp.set(&[&k1]);
    let t = tokens();
    t.verify(&k1.sign(&claims(now, "a")), &check(&issuers, now))
        .await
        .unwrap();
    idp.set(&[&k1, &k2]);
    let before = idp.hits("/jwks");
    let v = t
        .verify(&k2.sign(&claims(now, "b")), &check(&issuers, now))
        .await
        .unwrap();
    assert_eq!(v.user.as_deref(), Some("b"));
    assert_eq!(idp.hits("/jwks"), before + 1);
    // k1 retired: its tokens stop verifying once the set is refetched.
    idp.set(&[&k2]);
    let short = Check {
        jwks_refresh: Duration::from_millis(1),
        ..check(&issuers, now)
    };
    tokio::time::sleep(Duration::from_millis(5)).await;
    assert_eq!(
        t.verify(&k1.sign(&claims(now, "a")), &short).await.err(),
        Some(Refusal::UnknownKey)
    );
}

#[tokio::test]
async fn cached_keys_outlive_an_outage_by_twice_the_refresh() {
    let k = Key::ed("d1");
    let idp = Idp::start(&[&k]).await;
    let issuers = vec![Arc::new(Issuer::new(spec(&idp, &["EdDSA"])))];
    let t = tokens();
    let now = SystemTime::now();
    let c = Check {
        jwks_refresh: Duration::from_millis(300),
        ..check(&issuers, now)
    };
    let tok = k.sign(&claims(now, "a"));
    t.verify(&tok, &c).await.unwrap();
    idp.down.store(true, Ordering::SeqCst);
    tokio::time::sleep(Duration::from_millis(350)).await;
    // Stale but within twice the refresh: the cached keys serve.
    t.verify(&tok, &c).await.unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(matches!(
        t.verify(&tok, &c).await,
        Err(Refusal::KeysUnavailable(_))
    ));
    idp.down.store(false, Ordering::SeqCst);
    t.verify(&tok, &c).await.unwrap();
}

#[tokio::test]
async fn discovery_finds_the_key_set() {
    let k = Key::rsa("r1");
    let idp = Idp::start(&[&k]).await;
    let mut s = spec(&idp, &["RS256"]);
    s.issuer.clone_from(&idp.base);
    s.jwks_url = None;
    let issuers = vec![Arc::new(Issuer::new(s))];
    let now = SystemTime::now();
    let mut c = claims(now, "alice");
    c["iss"] = json!(idp.base);
    tokens()
        .verify(&k.sign(&c), &check(&issuers, now))
        .await
        .unwrap();
    assert_eq!(idp.hits("/.well-known/openid-configuration"), 1);
    assert_eq!(idp.hits("/jwks"), 1);
}

#[test]
fn mappings_template_match_and_order() {
    let m = |priority, kind, claim: &str, mk, v: Option<&str>, target: &str| {
        mapping::Mapping::new(ClaimMapping {
            priority,
            kind,
            claim: claim.into(),
            match_kind: mk,
            match_value: v.map(str::to_owned),
            target: target.into(),
        })
        .unwrap()
    };
    let maps = vec![
        m(30, MappingKind::User, "email", MatchKind::Any, None, "{email}"),
        m(10, MappingKind::User, "azp", MatchKind::Equals, Some("svc-app"), "app-{azp}"),
        m(20, MappingKind::User, "email", MatchKind::Regex, Some(".*@corp\\.test"), "corp-{sub}"),
        m(5, MappingKind::Role, "realm_access.roles", MatchKind::Equals, Some("dev"), "developer"),
        m(6, MappingKind::Role, "realm_access.roles", MatchKind::Regex, Some("ad.*"), "admin"),
        m(7, MappingKind::Role, "missing", MatchKind::Any, None, "never"),
    ];
    let c = json!({"sub": "42", "email": "a@corp.test", "azp": "web",
                   "realm_access": {"roles": ["dev", "admin", "dev"]}});
    let r = mapping::map(&maps, &c);
    assert_eq!(r.user.as_deref(), Some("corp-42"));
    assert_eq!(r.roles, vec!["developer".to_owned(), "admin".to_owned()]);
    let r = mapping::map(&maps, &json!({"sub": "1", "azp": "svc-app", "email": "x@y"}));
    assert_eq!(r.user.as_deref(), Some("app-svc-app"));
    let r = mapping::map(&maps, &json!({"sub": "1", "email": "x@y"}));
    assert_eq!(r.user.as_deref(), Some("x@y"));
    // A template naming a missing claim doesn't match; nothing else does.
    assert_eq!(mapping::map(&maps, &json!({"sub": "1"})).user, None);
    assert_eq!(mapping::expand("u-{n}", &json!({"n": 7})).as_deref(), Some("u-7"));
    assert_eq!(mapping::expand("u-{n", &json!({"n": 7})), None);
    assert_eq!(mapping::expand("u-}", &json!({})), None);
}

#[test]
fn specs_refuse_what_never_verifies() {
    let base = || IssuerSpec {
        issuer_id: None,
        kind: IssuerKind::Oidc,
        issuer: "https://idp.example".into(),
        jwks_url: None,
        audiences: vec!["a".into()],
        algorithms: vec!["RS256".into()],
        protocols: TokenProtocol::ALL.to_vec(),
        client_id: None,
        request_scopes: None,
        jit_users: false,
        max_lifetime_s: 3600,
        active: true,
        mappings: Vec::new(),
    };
    base().validate().unwrap();
    for (what, f) in [
        ("none", &(|s: &mut IssuerSpec| s.algorithms = vec!["none".into()]) as &dyn Fn(&mut IssuerSpec)),
        ("HS256", &|s| s.algorithms = vec!["HS256".into()]),
        ("no audience", &|s| s.audiences.clear()),
        ("plain http", &|s| s.jwks_url = Some("http://idp.example/jwks".into())),
        ("no protocol", &|s| s.protocols.clear()),
        ("zero lifetime", &|s| s.max_lifetime_s = 0),
        ("bad regex", &|s| s.mappings.push(ClaimMapping {
            priority: 1,
            kind: MappingKind::User,
            claim: "sub".into(),
            match_kind: MatchKind::Regex,
            match_value: Some("(".into()),
            target: "{sub}".into(),
        })),
        ("value for any", &|s| s.mappings.push(ClaimMapping {
            priority: 1,
            kind: MappingKind::User,
            claim: "sub".into(),
            match_kind: MatchKind::Any,
            match_value: Some("x".into()),
            target: "{sub}".into(),
        })),
    ] {
        let mut s = base();
        f(&mut s);
        assert!(s.validate().is_err(), "{what}");
    }
    let mut s = base();
    s.jwks_url = Some("http://127.0.0.1:9/jwks".into());
    s.validate().unwrap();
    // The Admin API's body: defaults for everything but issuer and audiences.
    let s: IssuerSpec = serde_json::from_value(
        json!({"issuer": "https://idp.example", "audiences": ["a"]}),
    )
    .unwrap();
    assert_eq!(s.algorithms, vec!["RS256".to_owned()]);
    assert_eq!(s.protocols.len(), 4);
    assert!(s.active);
}

#[test]
fn jwt_shape_and_parse() {
    assert!(!Jws::looks_like(b"hunter2"));
    assert!(!Jws::looks_like(b"eyJhbGciOiJSUzI1NiJ9.e30"));
    assert!(Jws::looks_like(b"eyJhbGciOiJSUzI1NiJ9.e30.c2ln"));
    assert!(!Jws::looks_like(&[b'e'; jwt::MAX_TOKEN_BYTES + 1]));
    let crit = format!(
        "{}.{}.c2ln",
        URL_SAFE_NO_PAD.encode(r#"{"alg":"RS256","crit":["exp"]}"#),
        URL_SAFE_NO_PAD.encode("{}")
    );
    assert!(Jws::parse(&crit).is_err());
    let arr = format!(
        "{}.{}.c2ln",
        URL_SAFE_NO_PAD.encode(r#"{"alg":"RS256"}"#),
        URL_SAFE_NO_PAD.encode("[1]")
    );
    assert!(Jws::parse(&arr).is_err());
}

// ---- logins ----

const ACME: TenantId = TenantId(42);

struct World {
    svc: Arc<LoginService>,
    clock: Arc<ManualClock>,
    audit: Arc<crate::audit::MemoryAudit>,
    idp: Idp,
    key: Key,
}

/// Tenant `acme` (42): 1 tok@% with the token method (and a password),
/// 2 pw@% password only, 3 lockd@% token, account locked; issuer
/// [`ISS`] (RS256) mapping `preferred_username`.
async fn world() -> World {
    let key = Key::rsa("r1");
    let idp = Idp::start(&[&key]).await;
    let clock = Arc::new(ManualClock::new());
    let mut tok = row(1, "tok", "%", 1);
    tok.auth_methods = AuthMethods::PASSWORD.union(AuthMethods::TOKEN);
    let pw = row(2, "pw", "%", 1);
    let mut lockd = row(3, "lockd", "%", 1);
    lockd.auth_methods = AuthMethods::TOKEN;
    lockd.account_locked = true;
    let auth = TenantAuth::build(vec![tok, pw, lockd], &[], Vec::new())
        .with_issuers(Some(vec![spec(&idp, &["RS256"])]));
    let catalogs = Arc::new(Catalogs::default());
    catalogs
        .map
        .lock()
        .unwrap()
        .insert(ACME, snapshot(ACME, auth, 3));
    let tenants = Arc::new(StaticTenants::new([TenantInfo {
        id: ACME,
        name: "acme".into(),
        can_log_in: true,
        suspended: false,
    }]));
    let mut policy = FixedPolicy::default();
    policy
        .0
        .insert(crate::settings::DELAY_AFTER_FAILURES.key, sprig_settings::Value::Int(0));
    let logins = CatalogLogins::new(
        tenants,
        catalogs,
        Arc::new(StaticBackends::new(Some(BackendTarget {
            cluster: ClusterId::new("dc-a-c1").unwrap(),
            user: "sprig_t42".into(),
            credentials: "env:SPRIG_T42".parse().unwrap(),
        }))),
        Arc::new(policy),
    );
    let audit = crate::audit::MemoryAudit::new();
    let svc = LoginService::new(
        LoginSource::Catalog(Arc::new(logins)),
        Arc::new(crate::FastCache::new(
            Arc::clone(&clock) as Arc<dyn Clock>,
            crate::fast_cache::DEFAULT_TTL,
        )),
    )
    .with_clock(Arc::clone(&clock) as Arc<dyn Clock>)
    .with_audit(Arc::clone(&audit) as Arc<dyn crate::audit::AuditSink>)
    .with_external_tokens(Arc::new(tokens()));
    World {
        svc: Arc::new(svc),
        clock,
        audit,
        idp,
        key,
    }
}

fn req(name: &str) -> LoginRequest {
    LoginRequest {
        persona: Persona::MySql84,
        login_name: name.as_bytes().to_vec(),
        sni: None,
        client_ip: IpAddr::V4(Ipv4Addr::LOCALHOST),
        tls: true,
        client_cert: false,
        client_dn: None,
        can_handle_expired_password: false,
    }
}

async fn mysql_login(w: &World, name: &str, password: &str) -> (AuthVerdict, Option<Arc<Value>>) {
    let mut l = w.svc.start(req(name)).await;
    let ok = l
        .check_password(Zeroizing::new(password.as_bytes().to_vec()))
        .await;
    let outcome = if ok {
        Outcome::Verified
    } else {
        Outcome::WrongCredentials
    };
    let v = l.decide(outcome, AuthPath::Full).await;
    (v, l.token().map(|t| Arc::clone(&t.claims)))
}

#[tokio::test]
async fn mysql_takes_a_token_as_the_password_of_its_account() {
    let w = world().await;
    let now = w.clock.wall();
    let tok = w.key.sign(&claims(now, "tok"));
    let (v, claims_seen) = mysql_login(&w, "acme.tok", &tok).await;
    assert_eq!(v, AuthVerdict::Accept);
    assert_eq!(claims_seen.unwrap()["org"], json!("o-7"));
    let ev = w.audit.events();
    let last = ev.last().unwrap();
    assert_eq!(last.event, "auth.login_succeeded");
    assert_eq!(last.mechanism, Some("jwt"));
    // Another account's token: refused, with the reason in the audit.
    let (v, _) = mysql_login(&w, "acme.tok", &w.key.sign(&claims(now, "pw"))).await;
    assert!(matches!(v, AuthVerdict::Refuse(ref e) if e.code == 1045), "{v:?}");
    let last = w.audit.events().last().cloned().unwrap();
    assert_eq!(last.event, "auth.login_failed");
    assert_eq!(last.detail.as_deref(), Some("token: no claim mapping names this account"));
    // Expired.
    let mut c = claims(now, "tok");
    c["exp"] = json!(unix(now) - 3600);
    c["iat"] = json!(unix(now) - 4000);
    let (v, _) = mysql_login(&w, "acme.tok", &w.key.sign(&c)).await;
    assert!(matches!(v, AuthVerdict::Refuse(ref e) if e.code == 1045));
    assert_eq!(
        w.audit.events().last().unwrap().detail.as_deref(),
        Some("token: expired")
    );
    // An account without the token method: the token is just a wrong
    // password.
    let (v, t) = mysql_login(&w, "acme.pw", &w.key.sign(&claims(now, "pw"))).await;
    assert!(matches!(v, AuthVerdict::Refuse(ref e) if e.code == 1045));
    assert!(t.is_none());
    // ACCOUNT LOCK applies to token logins.
    let (v, _) = mysql_login(&w, "acme.lockd", &w.key.sign(&claims(now, "lockd"))).await;
    assert!(matches!(v, AuthVerdict::Refuse(ref e) if e.code == 3118), "{v:?}");
}

#[tokio::test]
async fn http_bearer_tokens_name_tenant_and_account() {
    let w = world().await;
    let now = w.clock.wall();
    let http = crate::http::HttpAuth::new(Arc::clone(&w.svc));
    let call = |auth: String, tenant: Option<&'static str>| {
        let http = http.clone();
        async move {
            http.authenticate(&crate::http::HttpRequest {
                authorization: Some(&auth),
                host: Some("127.0.0.1:8080"),
                sni: None,
                tenant_header: tenant,
                client_ip: IpAddr::V4(Ipv4Addr::LOCALHOST),
                tls: false,
                client_cert: false,
                client_dn: None,
            })
            .await
        }
    };
    let tok = w.key.sign(&claims(now, "tok"));
    let p = call(format!("Bearer {tok}"), Some("acme")).await.unwrap();
    assert_eq!((p.tenant, p.user_id, p.user.as_str()), (Some(ACME), Some(UserId(1)), "tok"));
    // No tenant named: 39786.
    let e = call(format!("Bearer {tok}"), None).await.unwrap_err();
    assert_eq!((e.status, e.error.code), (401, 39786));
    // The mapped account lacks the token method; an unknown account
    // without jit_users.
    for user in ["pw", "nobody"] {
        let e = call(format!("Bearer {}", w.key.sign(&claims(now, user))), Some("acme"))
            .await
            .unwrap_err();
        assert_eq!((e.status, e.error.code), (401, 1045), "{user}");
    }
    let mut c = claims(now, "tok");
    c["aud"] = json!("other");
    let e = call(format!("Bearer {}", w.key.sign(&c)), Some("acme"))
        .await
        .unwrap_err();
    assert_eq!((e.status, e.error.code), (401, 1045));
    assert_eq!(
        w.audit.events().last().unwrap().detail.as_deref(),
        Some("token: wrong audience")
    );
    // A locked account: 3118.
    let e = call(format!("Bearer {}", w.key.sign(&claims(now, "lockd"))), Some("acme"))
        .await
        .unwrap_err();
    assert_eq!(e.error.code, 3118);
    assert!(w.idp.hits("/jwks") >= 1);
}

#[tokio::test]
async fn mongo_logins_find_the_tenant_and_its_issuer() {
    let w = world().await;
    assert_eq!(w.svc.tenant_named(Some("acme.x"), None).as_deref(), Some("acme"));
    assert_eq!(w.svc.tenant_named(None, None), None);
    let i = w.svc.mongo_issuer("acme").await.unwrap();
    assert_eq!((i.issuer.as_str(), i.client_id.as_deref()), (ISS, Some("sprig-cli")));
    assert!(w.svc.mongo_issuer("nope").await.is_none());
    let now = w.clock.wall();
    let tok = w.key.sign(&claims(now, "tok"));
    let (p, v) = w
        .svc
        .token_login(&TokenLogin {
            protocol: TokenProtocol::Mongodb,
            label: "mongo",
            tenant: "acme",
            token: &tok,
            client_ip: IpAddr::V4(Ipv4Addr::LOCALHOST),
            tls: false,
            client_cert: false,
            client_dn: None,
        })
        .await
        .unwrap();
    assert_eq!(p.user, "tok");
    assert_eq!(v.roles, vec!["app_admin".to_owned()]);
    let ev = w.audit.events();
    assert!(ev.iter().any(|e| e.event == "auth.login_succeeded" && e.protocol == "mongo"));
}

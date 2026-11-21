//! Every collation: key order is the collation's comparison, keys step over
//! and never prefix each other, and the two `uca1400` weight tables agree on
//! every code point.

#![allow(clippy::unwrap_used, clippy::missing_panics_doc)]

use proptest::prelude::*;
use sprig_engine::keys::collation::{Collation, Pad};

fn key(c: Collation, s: &str) -> Vec<u8> {
    let mut out = Vec::new();
    c.encode_key(s.as_bytes(), &mut out);
    out
}

const ALPHABET: &[char] = &[
    ' ',
    ' ',
    '\t',
    '\u{1}',
    '\0',
    'a',
    'A',
    'b',
    '\u{e9}',
    '\u{c9}',
    '\u{df}',
    's',
    '\u{a0}',
    '\u{200b}',
    '\u{3000}',
    '\u{4e00}',
    '\u{1f600}',
    '\u{0301}',
    '\u{0306}',
    '\u{0418}',
    'L',
    'l',
    '\u{b7}',
    '\u{0fb2}',
    '\u{0f71}',
    '\u{0f80}',
    '\u{ff21}',
    '\u{fb01}',
    '\u{7f}',
];

fn text() -> impl Strategy<Value = String> {
    proptest::collection::vec(proptest::sample::select(ALPHABET), 0..10)
        .prop_map(|v| v.into_iter().collect())
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(3000))]

    #[test]
    fn key_order_is_the_comparison(a in text(), b in text()) {
        for &c in Collation::all() {
            let want = c.compare(a.as_bytes(), b.as_bytes());
            prop_assert_eq!(want, key(c, &a).cmp(&key(c, &b)), "{} {:?} {:?}", c.name(), a, b);
        }
    }

    #[test]
    fn keys_skip_and_never_prefix_each_other(a in text(), b in text()) {
        for &c in Collation::all() {
            let (ka, kb) = (key(c, &a), key(c, &b));
            let mut long = ka.clone();
            long.extend_from_slice(&[0x00, 0x40, 0xFF]);
            prop_assert_eq!(c.skip_key(&long).unwrap(), &[0x00, 0x40, 0xFF][..], "{}", c.name());
            if ka != kb {
                prop_assert!(!ka.starts_with(&kb) && !kb.starts_with(&ka), "{} {:?} {:?}", c.name(), a, b);
            }
            // A cut key is refused, not misread as a shorter one.
            for cut in 0..ka.len() {
                prop_assert!(c.skip_key(&ka[..cut]).is_err(), "{} {:?} cut at {}", c.name(), a, cut);
            }
        }
    }

    #[test]
    fn long_space_runs(k1 in 0usize..140, k2 in 0usize..140, high in any::<bool>()) {
        let tail = if high { "b" } else { "\t" };
        let a = format!("a{}{tail}", " ".repeat(k1));
        let b = format!("a{}{tail}", " ".repeat(k2));
        for &c in Collation::all().iter().filter(|c| c.pad() == Pad::Space) {
            prop_assert_eq!(c.compare(a.as_bytes(), b.as_bytes()), key(c, &a).cmp(&key(c, &b)), "{}", c.name());
        }
    }
}

/// `utf8mb4_uca1400_ai_ci` weighs from the table built out of the public
/// DUCET; the other `uca1400` collations weigh from the table probed from
/// the server. Level 1 must be the same for every code point.
#[test]
fn uca1400_tables_agree_on_every_code_point() {
    let (old, new) = (Collation::Utf8mb4Uca1400AiCi, Collation::Utf8mb4Uca1400AsCs);
    let (mut a, mut b) = (Vec::new(), Vec::new());
    let mut buf = [0u8; 4];
    let mut seen = 0u32;
    for c in (0..=0x10_FFFFu32).filter_map(char::from_u32) {
        let s = c.encode_utf8(&mut buf).as_bytes();
        a.clear();
        b.clear();
        old.weights(s, &mut a);
        new.weights(s, &mut b);
        assert_eq!(a, b, "U+{:04X}", u32::from(c));
        seen += 1;
    }
    assert_eq!(seen, 1_112_064);
}

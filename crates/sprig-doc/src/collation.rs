//! Collations.
//!
//! A command's `collation` option (`find`, `aggregate`, `count`,
//! `distinct`, the writes' statements, `findAndModify`) changes how
//! strings compare: [`Collation::compare`] orders them by their sort keys
//! ([`Collation::sort_key`]). The command runs inside [`scope`], a
//! task-local value that [`crate::keyenc::compare`] reads, so every
//! comparison the command makes (filters, sorts, `distinct`, `$in`,
//! ranges) follows it without each function taking a parameter. The SQL
//! translation pushes no string comparison down while a collation is
//! active ([`crate::translate`]), since MariaDB's `utf8mb4_bin` would leave
//! out rows the collation matches; such queries read their candidates by
//! the other conditions and recheck in the gateway.
//!
//! Weights: the Unicode Collation Algorithm's default table (DUCET 14.0.0,
//! three levels, with its contractions), the weight table the engine
//! already has for MariaDB's `uca1400` collations. ICU's root collation is
//! this table with small changes, and MongoDB's collations are ICU's. A
//! sort key is the string's level-1 weights, then (by `strength`) its
//! level-2 weights, a case level, level-3 weights, a quaternary level and
//! the code points, each level ended by a separator lower than any weight,
//! so the bytes of two keys compare as the strings do. Index entries under
//! a collation store these keys ([`crate::keyenc::key_in`]).
//!
//! What it covers: `strength` 1 to 5, `caseLevel`, `caseFirst: "upper"`,
//! `numericOrdering` (runs of decimal digits compare by value, leading
//! zeros ignored), `alternate: "shifted"` with `maxVariable` (`punct` or
//! `space`: spaces and punctuation are ignored below the quaternary level)
//! and `backwards` (level 2 read from the end, as French does). Locales'
//! own tailorings are not applied: every locale compares as the root
//! locale ("simple" means no collation). `normalization` is accepted: the
//! text is always read in its canonical decomposition.

use std::cmp::Ordering;
use std::future::Future;
use std::sync::OnceLock;

use bson::{Bson, Document, doc};
use sprig_engine::keys::collation::Collation as Weights;
use sprig_proto_mongo::errors::{MongoError, code};
use unicode_normalization::UnicodeNormalization;

/// The weight table: DUCET 14.0.0 at levels 1 to 3.
const TABLE: Weights = Weights::Utf8mb4Uca1400NopadAsCs;

/// The ICU version MongoDB 8.0 reports in a collation's `version`.
pub const ICU_VERSION: &str = "57.1";

/// A collation's comparison rules.
#[allow(clippy::struct_excessive_bools)] // ICU's attributes, each a switch
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Collation {
    /// ICU strength, 1 to 5.
    pub strength: u8,
    /// `caseLevel`: case counts even at strength 1 and 2.
    pub case_level: bool,
    /// `numericOrdering`.
    pub numeric: bool,
    /// `caseFirst: "upper"`.
    pub upper_first: bool,
    /// `alternate: "shifted"`.
    pub shifted: bool,
    /// `maxVariable: "space"` (only spaces are variable when shifted).
    pub max_space: bool,
    /// `backwards`: level 2 compared from the end.
    pub backwards: bool,
}

tokio::task_local! {
    static ACTIVE: Collation;
    /// Set while code that must compare without the command's collation
    /// runs (row-level security policies).
    static SIMPLE: ();
}

/// The collation of the command running on this task, if any.
#[must_use]
pub fn active() -> Option<Collation> {
    if SIMPLE.try_with(|()| ()).is_ok() {
        return None;
    }
    ACTIVE.try_with(|c| *c).ok()
}

/// Runs the synchronous `f` with the simple collation, whatever the
/// command's is: a row-level security policy compares as written, so a
/// client's collation can't widen what it matches.
pub fn simple<R>(f: impl FnOnce() -> R) -> R {
    SIMPLE.sync_scope((), f)
}

/// Runs `f` with `c` as the active collation (none: as it is).
pub async fn scope<F: Future>(c: Option<Collation>, f: F) -> F::Output {
    match c {
        Some(c) => ACTIVE.scope(c, f).await,
        None => f.await,
    }
}

/// Runs the synchronous `f` with `c` as the active collation.
pub fn sync_scope<R>(c: Option<Collation>, f: impl FnOnce() -> R) -> R {
    match c {
        Some(c) => ACTIVE.sync_scope(c, f),
        None => f(),
    }
}

fn bad(m: impl Into<String>) -> MongoError {
    MongoError::new(code::BAD_VALUE, m)
}

impl Collation {
    /// Parses a `collation` option: `None` for the simple collation
    /// (`{locale: "simple"}`).
    ///
    /// # Errors
    /// 2 `BadValue` for a missing locale or an invalid option.
    pub fn parse(d: &Document) -> Result<Option<Self>, MongoError> {
        let locale = match d.get("locale") {
            Some(Bson::String(s)) => s.as_str(),
            Some(_) => return Err(bad("collation locale must be a string")),
            None => return Err(bad("Missing expected field \"locale\"")),
        };
        let mut c = Self {
            strength: 3,
            case_level: false,
            numeric: false,
            upper_first: false,
            shifted: false,
            max_space: false,
            backwards: false,
        };
        for (k, v) in d {
            match (k.as_str(), v) {
                ("strength", v) => {
                    let n = crate::filter::as_i64(v)
                        .filter(|n| (1..=5).contains(n))
                        .ok_or_else(|| bad("collation strength must be an integer from 1 to 5"))?;
                    c.strength = u8::try_from(n).unwrap_or(3);
                }
                ("caseLevel", Bson::Boolean(b)) => c.case_level = *b,
                ("numericOrdering", Bson::Boolean(b)) => c.numeric = *b,
                ("backwards", Bson::Boolean(b)) => c.backwards = *b,
                ("caseFirst", Bson::String(s)) => match s.as_str() {
                    "upper" => c.upper_first = true,
                    "lower" | "off" => {}
                    _ => return Err(bad(format!("unsupported caseFirst value: {s}"))),
                },
                ("alternate", Bson::String(s)) => match s.as_str() {
                    "shifted" => c.shifted = true,
                    "non-ignorable" => {}
                    _ => return Err(bad(format!("unsupported alternate value: {s}"))),
                },
                ("maxVariable", Bson::String(s)) => match s.as_str() {
                    "space" => c.max_space = true,
                    "punct" => {}
                    _ => return Err(bad(format!("unsupported maxVariable value: {s}"))),
                },
                ("locale", _)
                | ("normalization", Bson::Boolean(_))
                | ("version", Bson::String(_)) => {}
                _ => return Err(bad(format!("unknown or invalid collation field: {k}"))),
            }
        }
        if locale == "simple" {
            if d.len() > 1 {
                return Err(bad(
                    "If the locale is \"simple\", no other collation options may be specified",
                ));
            }
            return Ok(None);
        }
        if locale.is_empty() {
            return Err(bad("Field 'locale' is invalid in: { locale: \"\" }"));
        }
        Ok(Some(c))
    }

    /// The collation of a command or statement document's `collation`
    /// field.
    ///
    /// # Errors
    /// As [`Self::parse`]; 2 when the field isn't a document.
    pub fn of(d: &Document) -> Result<Option<Self>, MongoError> {
        match d.get("collation") {
            None => Ok(None),
            Some(Bson::Document(c)) => Self::parse(c),
            Some(_) => Err(MongoError::new(
                code::TYPE_MISMATCH,
                "collation must be an object",
            )),
        }
    }

    /// Compares two strings under this collation.
    #[must_use]
    pub fn compare(self, a: &str, b: &str) -> Ordering {
        if a == b {
            return Ordering::Equal;
        }
        self.sort_key(a).cmp(&self.sort_key(b))
    }

    /// The sort key of `s`: byte strings compare as their strings do under
    /// this collation, and are equal exactly when the strings are.
    #[must_use]
    pub fn sort_key(self, s: &str) -> Vec<u8> {
        let mut out = Vec::with_capacity(s.len() * 4 + 8);
        self.write_key(s, &mut out);
        out
    }

    fn write_key(self, s: &str, out: &mut Vec<u8>) {
        let consts = consts();
        let segs = self.segments(s, consts);
        let mut w = Vec::with_capacity(s.len());
        // Level 1.
        for seg in &segs {
            match seg {
                Seg::Text(t) => {
                    w.clear();
                    TABLE.level_weights(t.as_bytes(), 0, &mut w);
                    put_weights(out, &w);
                }
                Seg::Digits(ds) => {
                    let lead = ds.iter().take_while(|d| **d == 0).count();
                    let ds = &ds[lead.min(ds.len() - 1)..];
                    put_u16(out, consts.zero);
                    put_u16(out, u32::try_from(ds.len()).unwrap_or(u32::MAX));
                    out.extend(ds.iter().map(|d| d + 1));
                }
                Seg::Var(_) => {}
            }
        }
        out.extend_from_slice(&[0, 0]);
        // Level 2.
        if self.strength >= 2 {
            w.clear();
            for seg in &segs {
                if let Seg::Text(t) = seg {
                    TABLE.level_weights(t.as_bytes(), 1, &mut w);
                }
            }
            if self.backwards {
                w.reverse();
            }
            put_weights(out, &w);
            out.extend_from_slice(&[0, 0]);
        }
        // The case level: one byte per level-1 weight, upper case when
        // the weight's level-3 partner is a capital form (so `ß` and `ss`,
        // or `ǅ` and `Dž`, have the same case pattern, as in ICU).
        if self.case_level {
            let (mut p, mut t3) = (Vec::new(), Vec::new());
            let mut buf = [0u8; 4];
            for seg in &segs {
                if let Seg::Text(t) = seg {
                    for ch in t.chars() {
                        let one = ch.encode_utf8(&mut buf).as_bytes();
                        p.clear();
                        t3.clear();
                        TABLE.level_weights(one, 0, &mut p);
                        TABLE.level_weights(one, 2, &mut t3);
                        for i in 0..p.iter().filter(|x| **x != 0).count() {
                            let upper = t3.get(i).is_some_and(|x| (0x08..=0x0C).contains(x));
                            out.push(if upper == self.upper_first { 1 } else { 2 });
                        }
                    }
                }
            }
            out.push(0);
        }
        // Level 3.
        if self.strength >= 3 {
            w.clear();
            for seg in &segs {
                if let Seg::Text(t) = seg {
                    TABLE.level_weights(t.as_bytes(), 2, &mut w);
                }
            }
            if self.upper_first {
                // DUCET's capital forms (0x08-0x0C) before everything else.
                for x in &mut w {
                    *x = if (0x08..=0x0C).contains(x) {
                        *x - 7
                    } else {
                        *x + 0x20
                    };
                }
            }
            put_weights(out, &w);
            out.extend_from_slice(&[0, 0]);
        }
        // Level 4: what `shifted` set aside, in place.
        if self.strength >= 4 && self.shifted {
            for seg in &segs {
                match seg {
                    Seg::Text(t) => {
                        w.clear();
                        TABLE.level_weights(t.as_bytes(), 0, &mut w);
                        for x in &w {
                            if *x != 0 {
                                put_u16(out, 0xFFFF);
                            }
                        }
                    }
                    Seg::Digits(_) => put_u16(out, 0xFFFF),
                    Seg::Var(ps) => put_weights(out, ps),
                }
            }
            out.extend_from_slice(&[0, 0]);
        }
        // Level 5: the code points.
        if self.strength >= 5 {
            for ch in s.nfd() {
                let cp = u32::from(ch) + 1;
                out.extend_from_slice(&cp.to_be_bytes()[1..]);
            }
        }
    }

    /// `s` in its canonical decomposition, split into runs of text, runs
    /// of decimal digits (`numericOrdering`) and variable characters
    /// (`alternate: "shifted"`).
    fn segments(self, s: &str, k: &Consts) -> Vec<Seg> {
        let mut segs: Vec<Seg> = Vec::new();
        let mut w = Vec::with_capacity(4);
        let top = if self.max_space {
            k.space_top
        } else {
            k.punct_top
        };
        for ch in s.nfd() {
            let may_digit =
                self.numeric && (ch.is_ascii_digit() || (!ch.is_ascii() && ch.is_numeric()));
            let may_var = self.shifted && !ch.is_alphanumeric();
            if may_digit || may_var {
                w.clear();
                let mut buf = [0u8; 4];
                TABLE.level_weights(ch.encode_utf8(&mut buf).as_bytes(), 0, &mut w);
                if may_digit
                    && let [p] = w.as_slice()
                    && (k.zero..=k.nine).contains(p)
                {
                    let d = u8::try_from(*p - k.zero).unwrap_or(0);
                    match segs.last_mut() {
                        Some(Seg::Digits(ds)) => ds.push(d),
                        _ => segs.push(Seg::Digits(vec![d])),
                    }
                    continue;
                }
                if may_var {
                    if !w.is_empty() && w.iter().all(|p| *p <= top) {
                        match segs.last_mut() {
                            Some(Seg::Var(ps)) => ps.extend_from_slice(&w),
                            _ => segs.push(Seg::Var(w.clone())),
                        }
                        continue;
                    }
                    // A mark after a variable character goes with it.
                    if w.is_empty() && matches!(segs.last(), Some(Seg::Var(_))) {
                        continue;
                    }
                }
            }
            match segs.last_mut() {
                Some(Seg::Text(t)) => t.push(ch),
                _ => segs.push(Seg::Text(ch.to_string())),
            }
        }
        segs
    }

    /// The collation option as MongoDB shows it (in `listIndexes` and
    /// `listCollections`): every field, defaults filled in. `None` for the
    /// simple collation.
    ///
    /// # Errors
    /// As [`Self::parse`].
    pub fn normalize(d: &Document) -> Result<Option<Document>, MongoError> {
        let Some(c) = Self::parse(d)? else {
            return Ok(None);
        };
        let locale = d.get_str("locale").unwrap_or_default();
        let case_first = match d.get_str("caseFirst") {
            Ok("upper") => "upper",
            Ok("lower") => "lower",
            _ => "off",
        };
        Ok(Some(doc! {
            "locale": locale,
            "caseLevel": c.case_level,
            "caseFirst": case_first,
            "strength": i32::from(c.strength),
            "numericOrdering": c.numeric,
            "alternate": if c.shifted { "shifted" } else { "non-ignorable" },
            "maxVariable": if c.max_space { "space" } else { "punct" },
            "normalization": d.get_bool("normalization").unwrap_or(false),
            "backwards": c.backwards,
            "version": ICU_VERSION,
        }))
    }
}

/// A piece of a string, for its sort key.
enum Seg {
    Text(String),
    /// Decimal digits by value, most significant first.
    Digits(Vec<u8>),
    /// The level-1 weights of variable characters.
    Var(Vec<u32>),
}

/// Weights read from the table once.
struct Consts {
    /// The primaries of `0` and `9`: digits of every script share them.
    zero: u32,
    nine: u32,
    /// The highest variable primary for `maxVariable: "space"` (the
    /// space's) and `"punct"` (below the first symbol, `` ` ``).
    space_top: u32,
    punct_top: u32,
}

fn consts() -> &'static Consts {
    static C: OnceLock<Consts> = OnceLock::new();
    C.get_or_init(|| {
        let one = |s: &str| {
            let mut w = Vec::new();
            TABLE.level_weights(s.as_bytes(), 0, &mut w);
            w.first().copied().unwrap_or(0)
        };
        Consts {
            zero: one("0"),
            nine: one("9"),
            space_top: one(" "),
            punct_top: one("`").saturating_sub(1),
        }
    })
}

fn put_u16(out: &mut Vec<u8>, w: u32) {
    let w = u16::try_from(w).unwrap_or(u16::MAX);
    out.extend_from_slice(&w.to_be_bytes());
}

fn put_weights(out: &mut Vec<u8>, w: &[u32]) {
    for x in w {
        if *x != 0 {
            put_u16(out, *x);
        }
    }
}

#[cfg(test)]
mod tests {
    use bson::doc;

    use super::*;

    fn c(d: &Document) -> Collation {
        Collation::parse(d).unwrap().unwrap()
    }

    #[test]
    fn strengths() {
        let s1 = c(&doc! {"locale": "en_US", "strength": 1});
        let s2 = c(&doc! {"locale": "en_US", "strength": 2});
        let s3 = c(&doc! {"locale": "en_US"});
        assert_eq!(s1.compare("ping", "PING"), Ordering::Equal);
        assert_eq!(s1.compare("café", "CAFE"), Ordering::Equal);
        assert_eq!(s2.compare("ping", "PING"), Ordering::Equal);
        assert_ne!(s2.compare("café", "cafe"), Ordering::Equal);
        assert_eq!(s3.compare("a", "A"), Ordering::Less);
        assert_eq!(s3.compare("A", "b"), Ordering::Less);
        assert_eq!(s3.compare("ping", "pong"), Ordering::Less);
        let up = c(&doc! {"locale": "en", "caseFirst": "upper"});
        assert_eq!(up.compare("a", "A"), Ordering::Greater);
        assert_eq!(up.compare("A", "b"), Ordering::Less);
        // Punctuation sorts before digits, digits before letters (ICU's
        // root order), not by code point.
        assert_eq!(s3.compare("_", "0"), Ordering::Less);
        assert_eq!(s3.compare("9", "a"), Ordering::Less);
        assert_eq!(s1.compare("\u{df}", "ss"), Ordering::Equal);
    }

    #[test]
    fn case_level_and_identical() {
        let s1c = c(&doc! {"locale": "en", "strength": 1, "caseLevel": true});
        assert_eq!(s1c.compare("cafe", "café"), Ordering::Equal);
        assert_ne!(s1c.compare("cafe", "Cafe"), Ordering::Equal);
        // MongoDB 8.0's answers: case follows the weights, not the letters.
        assert_eq!(s1c.compare("\u{df}", "ss"), Ordering::Equal);
        assert_ne!(s1c.compare("\u{1c5}", "\u{1c6}"), Ordering::Equal);
        let s3 = c(&doc! {"locale": "en", "strength": 3});
        let s5 = c(&doc! {"locale": "en", "strength": 5});
        // Canonically equal: equal at every level.
        assert_eq!(s5.compare("e\u{301}", "\u{e9}"), Ordering::Equal);
        assert_eq!(s3.compare("a", "a"), Ordering::Equal);
    }

    #[test]
    fn shifted_ignores_punctuation() {
        let sh = c(&doc! {"locale": "en", "alternate": "shifted"});
        assert_eq!(sh.compare("a b", "ab"), Ordering::Equal);
        assert_eq!(sh.compare("a-b", "ab"), Ordering::Equal);
        let sp = c(&doc! {"locale": "en", "alternate": "shifted", "maxVariable": "space"});
        assert_eq!(sp.compare("a b", "ab"), Ordering::Equal);
        assert_ne!(sp.compare("a-b", "ab"), Ordering::Equal);
        let q = c(&doc! {"locale": "en", "alternate": "shifted", "strength": 4});
        assert_ne!(q.compare("a b", "ab"), Ordering::Equal);
        let plain = c(&doc! {"locale": "en"});
        assert_ne!(plain.compare("a b", "ab"), Ordering::Equal);
    }

    #[test]
    fn numeric_ordering() {
        let n = c(&doc! {"locale": "en", "numericOrdering": true});
        assert_eq!(n.compare("item10", "item9"), Ordering::Greater);
        assert_eq!(n.compare("item010", "item10"), Ordering::Equal);
        assert_eq!(n.compare("2", "10"), Ordering::Less);
        assert_eq!(n.compare("10", "1a"), Ordering::Greater);
        let plain = c(&doc! {"locale": "en"});
        assert_eq!(plain.compare("item10", "item9"), Ordering::Less);
    }

    #[test]
    fn sort_keys_order_as_compare() {
        let words = [
            "", " ", "a", "A", "á", "Á", "ab", "a b", "a-b", "B", "b", "café", "CAFE", "cafe",
            "item2", "item10", "item010", "z", "Z", "ß", "ss", "1", "01", "_x", "x_",
        ];
        for d in [
            doc! {"locale": "en", "strength": 1},
            doc! {"locale": "en", "strength": 2},
            doc! {"locale": "en"},
            doc! {"locale": "en", "caseFirst": "upper", "caseLevel": true},
            doc! {"locale": "en", "numericOrdering": true, "alternate": "shifted"},
            doc! {"locale": "fr", "backwards": true, "strength": 4, "alternate": "shifted"},
            doc! {"locale": "en", "strength": 5},
        ] {
            let k = c(&d);
            for a in words {
                for b in words {
                    let by_key = k.sort_key(a).cmp(&k.sort_key(b));
                    assert_eq!(k.compare(a, b), by_key, "{d} {a:?} {b:?}");
                    assert_eq!(by_key, k.sort_key(b).cmp(&k.sort_key(a)).reverse());
                }
            }
        }
    }

    #[test]
    fn simple_and_errors() {
        assert!(
            Collation::parse(&doc! {"locale": "simple"})
                .unwrap()
                .is_none()
        );
        assert!(Collation::parse(&doc! {"strength": 1}).is_err());
        assert!(Collation::parse(&doc! {"locale": "en", "strength": 9}).is_err());
        assert!(Collation::parse(&doc! {"locale": "en", "bogus": 1}).is_err());
        assert!(Collation::parse(&doc! {"locale": "en", "alternate": "x"}).is_err());
        let n = Collation::normalize(&doc! {"locale": "en_US", "strength": 2})
            .unwrap()
            .unwrap();
        assert_eq!(n.get_i32("strength"), Ok(2));
        assert_eq!(n.get_str("caseFirst"), Ok("off"));
        assert_eq!(n.get_str("version"), Ok(ICU_VERSION));
        assert!(
            Collation::normalize(&doc! {"locale": "simple"})
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn the_scope_reaches_keyenc() {
        let ci = c(&doc! {"locale": "en", "strength": 2});
        let eq = scope(Some(ci), async {
            crate::keyenc::equal(&Bson::String("ping".into()), &Bson::String("PING".into()))
        })
        .await;
        assert!(eq);
        assert!(!crate::keyenc::equal(
            &Bson::String("ping".into()),
            &Bson::String("PING".into())
        ));
    }
}

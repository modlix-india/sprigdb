//! Expected values are the answers of MySQL 8.4 and MariaDB 11.8 (both
//! agree) for the same rows and queries, written as they print.

#![allow(clippy::unreadable_literal, clippy::excessive_precision)]

use std::sync::Arc;

use super::*;

fn an() -> Analyzer {
    Analyzer::new(Rules::default(), Arc::new(LowerFold))
}

/// Table `e` of the probes.
fn table_e() -> (Analyzer, MemIndex) {
    let a = an();
    let mut ix = MemIndex::default();
    for t in [
        "mysql database tutorial",
        "mysql mysql performance tuning",
        "postgres database guide",
        "database database database",
        "redis cache tutorial",
        "the quick brown fox",
        "mysql security",
        "database security guide tutorial",
    ] {
        ix.add(&a, &[t]);
    }
    (a, ix)
}

/// Scores of rows 1..=n (0 for rows not found), as f32.
fn scores(ix: &MemIndex, q: &Node, n: usize) -> Vec<f32> {
    let mut out = vec![0f32; n];
    for (d, s) in rank(q, ix) {
        out[usize::try_from(d).unwrap_or(0)] = s;
    }
    out
}

fn b(ix: &MemIndex, a: &Analyzer, q: &str) -> Vec<f32> {
    let node = parse_boolean(q, a).unwrap_or_else(|e| panic!("{q}: {e}"));
    scores(ix, &node, 8)
}

fn nat(ix: &MemIndex, a: &Analyzer, q: &str) -> Vec<f32> {
    scores(ix, &parse_natural(q, a), 8)
}

fn close(got: &[f32], want: &[f64], what: &str) {
    assert_eq!(got.len(), want.len(), "{what}");
    for (i, (g, w)) in got.iter().zip(want).enumerate() {
        #[allow(clippy::cast_possible_truncation)]
        let w = *w as f32;
        let ok = if w == 0.0 {
            *g == 0.0
        } else {
            ((g - w) / w).abs() < 1e-6
        };
        assert!(ok, "{what}: row {} got {g} want {w} (all {got:?})", i + 1);
    }
}

#[test]
fn words() {
    let spans = |t: &str| -> Vec<String> {
        word_spans(t)
            .into_iter()
            .map(|(s, e)| t[s..e].to_owned())
            .collect()
    };
    assert_eq!(
        spans("don't stop o'neil it's 'q' foo_bar foo-bar x1y2 3.14"),
        [
            "don", "t", "stop", "o", "neil", "it", "s", "q", "foo_bar", "foo", "bar", "x1y2", "3",
            "14"
        ]
    );
    assert_eq!(spans("café naïve über, ü"), ["café", "naïve", "über", "ü"]);
    let a = an();
    assert!(!a.term("the").1 && !a.term("ab").1 && a.term("abc").1);
    assert!(!a.term(&"x".repeat(85)).1 && a.term(&"x".repeat(84)).1);
}

#[test]
fn natural_scores() {
    let (a, ix) = table_e();
    // log10(8/3)^2, two rows hold it twice.
    close(
        &nat(&ix, &a, "mysql"),
        &[
            0.18144935369491577,
            0.36289870738983154,
            0.,
            0.,
            0.,
            0.,
            0.18144935369491577,
            0.,
        ],
        "mysql",
    );
    // Operators are words in natural mode; quotes are phrases.
    close(
        &nat(&ix, &a, "mysql -security"),
        &[
            0.18144935369491577,
            0.36289870738983154,
            0.,
            0.,
            0.,
            0.,
            0.5439255833625793,
            0.3624762296676636,
        ],
        "operators",
    );
    close(
        &nat(&ix, &a, "\"mysql security\" guide"),
        &[
            0.,
            0.,
            0.3624762296676636,
            0.,
            0.,
            0.,
            0.5439255833625793,
            0.3624762296676636,
        ],
        "phrase",
    );
    // A repeated word raises its df.
    close(
        &nat(&ix, &a, "mysql \"mysql security\""),
        &[
            0.015609688125550747,
            0.031219376251101494,
            0.,
            0.,
            0.,
            0.,
            0.3780859112739563,
            0.,
        ],
        "repeat",
    );
    close(
        &nat(&ix, &a, "data*"),
        &[0.; 8],
        "no prefix in natural mode",
    );
}

#[test]
fn repeated_words_and_the_floor() {
    let a = an();
    let mut ix = MemIndex::default();
    for t in [
        "apple banana cherry",
        "apple apple date",
        "banana elder fig",
        "zzz qqq",
        "apple",
    ] {
        ix.add(&a, &[t]);
    }
    let q = |q: &str| scores(&ix, &parse_natural(q, &a), 5);
    close(
        &q("apple"),
        &[
            0.0492168664932251,
            0.0984337329864502,
            0.,
            0.,
            0.0492168664932251,
        ],
        "apple",
    );
    close(
        &q("apple apple"),
        &[
            0.006269669625908136,
            0.012539339251816273,
            0.,
            0.,
            0.006269669625908136,
        ],
        "apple apple",
    );
    close(
        &q("apple apple apple"),
        &[
            0.06516405194997787,
            0.13032810389995575,
            0.,
            0.,
            0.06516405194997787,
        ],
        "apple x3",
    );
    let mut c = MemIndex::default();
    for _ in 0..9 {
        c.add(&a, &["common"]);
    }
    c.add(&a, &["common rare"]);
    let s = scores(&c, &parse_natural("common", &a), 10);
    close(&s[..1], &[0.000000001885928302414186], "floor");
    let s = scores(&c, &parse_natural("rare", &a), 10);
    close(&s[9..], &[1.0], "rare");
}

#[test]
fn columns_add_up() {
    let a = an();
    let mut ix = MemIndex::default();
    ix.add(&a, &["red apple", "red red car"]);
    ix.add(&a, &["blue sky", "green apple"]);
    ix.add(&a, &["the cat", "don't stop o'neil it's"]);
    ix.add(&a, &["ab abc abcd", "foo_bar foo-bar 123 12 x1y2"]);
    ix.add(&a, &["cafe naive", "cafe naive uber"]);
    ix.add(&a, &["", "nothing here"]);
    let q = |q: &str| scores(&ix, &parse_natural(q, &a), 6);
    close(&q("red"), &[1.8165581226348877, 0., 0., 0., 0., 0.], "red");
    close(
        &q("apple"),
        &[0.22764469683170319, 0.22764469683170319, 0., 0., 0., 0.],
        "apple",
    );
    for w in ["the cat", "don't", "stop", "o'neil"] {
        close(&q(w), &[0., 0., 0.6055193543434143, 0., 0., 0.], w);
    }
    for w in ["abc", "foo_bar", "foo", "123", "x1y2"] {
        close(&q(w), &[0., 0., 0., 0.6055193543434143, 0., 0.], w);
    }
    for w in ["ab", "12"] {
        close(&q(w), &[0.; 6], w);
    }
}

#[test]
fn boolean_scores() {
    let (a, ix) = table_e();
    let cases: &[(&str, [f64; 8])] = &[
        (
            "+mysql +database",
            [0.27206841111183167, 0., 0., 0., 0., 0., 0., 0.],
        ),
        (
            "+mysql -security",
            [
                0.18144935369491577,
                0.36289870738983154,
                0.,
                0.,
                0.,
                0.,
                0.,
                0.,
            ],
        ),
        (
            "mysql database",
            [
                0.27206841111183167,
                0.36289870738983154,
                0.0906190574169159,
                0.2718571722507477,
                0.,
                0.,
                0.18144935369491577,
                0.0906190574169159,
            ],
        ),
        (
            "+database >tutorial <guide",
            [
                1.2720685005187988,
                0.,
                -0.5469046831130981,
                0.2718571722507477,
                0.,
                0.,
                0.,
                0.6345446109771729,
            ],
        ),
        (
            "tu*",
            [
                0.0906190574169159,
                0.0906190574169159,
                0.,
                0.,
                0.0906190574169159,
                0.,
                0.,
                0.0906190574169159,
            ],
        ),
        (
            "\"database tutorial\"",
            [0.27206841111183167, 0., 0., 0., 0., 0., 0., 0.],
        ),
        (
            "+database +(tutorial guide)",
            [
                0.27206841111183167,
                0.,
                0.45309528708457947,
                0.,
                0.,
                0.,
                0.,
                0.6345446109771729,
            ],
        ),
        ("-mysql", [0.; 8]),
        ("~database", [0.; 8]),
        (
            "guide ~tutorial",
            [
                0.,
                0.,
                0.3624762296676636,
                0.,
                0.,
                0.,
                0.,
                -0.45607441663742065,
            ],
        ),
        (
            "~tutorial guide",
            [
                0.,
                0.,
                0.3624762296676636,
                0.,
                0.,
                0.,
                0.,
                0.3624762296676636,
            ],
        ),
        (
            "mysql ~tutorial ~database",
            [
                -0.5464822053909302,
                0.36289870738983154,
                0.,
                0.,
                0.,
                0.,
                0.18144935369491577,
                0.,
            ],
        ),
        (
            "+mysql ~database",
            [
                0.18144935369491577,
                0.36289870738983154,
                0.,
                0.,
                0.,
                0.,
                0.18144935369491577,
                0.,
            ],
        ),
        (
            "<database <tutorial",
            [
                -0.727931559085846,
                0.,
                -0.9093809127807617,
                -0.7281428575515747,
                -0.8185506463050842,
                0.,
                0.,
                -0.727931559085846,
            ],
        ),
        (
            "+database (+tutorial guide)",
            [
                0.27206841111183167,
                0.,
                0.0906190574169159,
                0.2718571722507477,
                0.18144935369491577,
                0.,
                0.,
                0.6345446109771729,
            ],
        ),
        (
            "tutorial >(guide security)",
            [
                0.18144935369491577,
                0.,
                1.3624762296676636,
                0.,
                0.18144935369491577,
                0.,
                1.3624762296676636,
                1.9064018726348877,
            ],
        ),
        (
            "\"database guide\" @3",
            [
                0.,
                0.,
                0.45309528708457947,
                0.,
                0.,
                0.,
                0.,
                0.45309528708457947,
            ],
        ),
        (
            "\"database tutorial\" @3",
            [0.27206841111183167, 0., 0., 0., 0., 0., 0., 0.],
        ),
        ("+the +fox", [0.; 8]),
        (
            "\"the quick\"",
            [0., 0., 0., 0., 0., 0.8155715465545654, 0., 0.],
        ),
        ("\"quick fox\"", [0.; 8]),
        (
            "mysql+security",
            [
                0.,
                0.,
                0.,
                0.,
                0.,
                0.,
                0.5439255833625793,
                0.3624762296676636,
            ],
        ),
        (
            "database database",
            [
                0.000000001885928302414186,
                0.,
                0.000000001885928302414186,
                0.000000005657784907242558,
                0.,
                0.,
                0.,
                0.000000001885928302414186,
            ],
        ),
    ];
    for (q, want) in cases {
        close(&b(&ix, &a, q), want, q);
    }
}

#[test]
fn boolean_syntax_errors() {
    let a = an();
    let err = |q: &str| match parse_boolean(q, &a) {
        Err(SyntaxError(m)) => m,
        Ok(n) => panic!("{q} parsed: {n:?}"),
    };
    assert_eq!(err(">>database"), "syntax error, unexpected '>'");
    assert_eq!(err("+"), "syntax error, unexpected $end");
    assert_eq!(err("(mysql"), "syntax error, unexpected $end");
    assert_eq!(err("((mysql)"), "syntax error, unexpected $end");
    assert_eq!(
        err("mysql)"),
        "syntax error, unexpected ')', expecting $end"
    );
    assert_eq!(
        err("*"),
        "syntax error, unexpected $end, expecting FTS_TERM or FTS_NUMB or '*'"
    );
    assert_eq!(
        err("mysql**"),
        "syntax error, unexpected $end, expecting FTS_TERM or FTS_NUMB or '*'"
    );
    assert_eq!(
        err("\"mysql security\"*"),
        "syntax error, unexpected $end, expecting FTS_TERM or FTS_NUMB or '*'"
    );
    assert_eq!(err("+-mysql"), "syntax error, unexpected '-'");
    assert_eq!(err("@3"), "syntax error, unexpected '@', expecting $end");
    assert_eq!(
        err("mysql @3"),
        "syntax error, unexpected '@', expecting $end"
    );
    assert_eq!(
        err("\"mysql security\" @"),
        "syntax error, unexpected $end, expecting FTS_NUMB"
    );
    assert_eq!(
        err("\"mysql security\" @x"),
        "syntax error, unexpected FTS_TERM, expecting FTS_NUMB"
    );
    assert_eq!(err("mysql~"), "syntax error, unexpected $end");
    for ok in [
        "()",
        "+()",
        "\"\"",
        "\"unclosed mysql",
        "+ mysql",
        "mysql*foo",
        "123 mysql",
    ] {
        assert!(parse_boolean(ok, &a).is_ok(), "{ok}");
    }
}

#[test]
fn phrases_count_every_word() {
    let a = an();
    let mut ix = MemIndex::default();
    for t in [
        "alpha the beta",
        "alpha xy beta",
        "alpha, beta",
        "alpha beta",
        "alpha gamma beta",
        "beta alpha",
        "alpha... --- beta",
        "alpha gamma delta beta",
        "zeta eta",
        "iota kappa",
    ] {
        ix.add(&a, &[t]);
    }
    let found = |q: &str| -> Vec<u64> {
        let n = parse_boolean(q, &a).unwrap_or_else(|e| panic!("{e}"));
        rank(&n, &ix).into_iter().map(|(d, _)| d + 1).collect()
    };
    assert_eq!(found("\"alpha beta\""), [3, 4, 7]);
    assert_eq!(found("\"alpha the beta\""), [1]);
    assert_eq!(found("\"alpha xy beta\""), [2]);
    assert_eq!(found("\"alpha beta\" @1"), Vec::<u64>::new());
    assert_eq!(found("\"alpha beta\" @2"), [3, 4, 6, 7]);
    assert_eq!(found("\"alpha beta\" @3"), [1, 2, 3, 4, 5, 6, 7]);
    assert_eq!(found("\"alpha beta\" @4"), [1, 2, 3, 4, 5, 6, 7, 8]);
    assert_eq!(found("\"alpha delta beta\" @5"), [8]);
    assert_eq!(found("\"alp* beta\""), Vec::<u64>::new());
}

#[test]
fn apostrophes_split_and_phrases_drop_leading_stop_words() {
    let a = an();
    let mut ix = MemIndex::default();
    for t in [
        "don't stop",
        "don stop",
        "dont stop",
        "it's fine",
        "its fine",
        "o'neil here",
        "neil here",
        "gamma ray",
        "of gamma",
        "alpha beta",
    ] {
        ix.add(&a, &[t]);
    }
    let q = |s: &str, boolean: bool| -> Vec<f32> {
        let n = if boolean {
            parse_boolean(s, &a).unwrap_or_else(|e| panic!("{e}"))
        } else {
            parse_natural(s, &a)
        };
        scores(&ix, &n, 10)
    };
    let both = 0.488_559_07;
    close(
        &q("don", false),
        &[both, both, 0., 0., 0., 0., 0., 0., 0., 0.].map(f64::from),
        "don",
    );
    close(
        &q("don't", false),
        &[both, both, 0., 0., 0., 0., 0., 0., 0., 0.].map(f64::from),
        "don't",
    );
    close(&q("it's", false), &[0.; 10], "it's");
    close(
        &q("\"of gamma\"", true),
        &[0., 0., 0., 0., 0., 0., 0., both, both, 0.].map(f64::from),
        "of gamma",
    );
    close(&q("\"gamma of\"", true), &[0.; 10], "gamma of");
    close(&q("+in gamma", true), &[0.; 10], "+in gamma");
}

#[test]
fn old_versions_count_except_for_one_plain_word() {
    let a = an();
    let mut ix = MemIndex::default();
    for i in 0..10 {
        ix.add(&a, &[if i < 5 { "run" } else { "walk" }]);
    }
    // Two rows replaced: their old versions stay in the raw counts.
    ix.remove(0);
    ix.remove(1);
    ix.add(&a, &["walk"]);
    ix.add(&a, &["walk"]);
    let one = scores(&ix, &parse_natural("run", &a), 12);
    let two = scores(&ix, &parse_natural("run zzz", &a), 12);
    // N = 10 live rows; live df 3, raw df 5.
    close(&one[2..3], &[(10f64 / 3.0).log10().powi(2)], "one word");
    close(&two[2..3], &[(10f64 / 5.0).log10().powi(2)], "two words");
}

#[test]
fn a_prefix_counts_its_first_word_in_the_row() {
    let a = an();
    let mut ix = MemIndex::default();
    for t in [
        "security search security",
        "server security server",
        "search search server",
        "storage",
        "other",
    ] {
        ix.add(&a, &[t]);
    }
    let s = scores(
        &ix,
        &parse_boolean("s*", &a).unwrap_or_else(|e| panic!("{e}")),
        5,
    );
    // df = 2 (security) + 2 (search) + 2 (server) + 1 (storage) = 7 > N.
    let w = (5f64 / 7.0).log10().powi(2);
    close(&s, &[2.0 * w, 2.0 * w, 2.0 * w, w, 0.0], "s*");
}

#[test]
fn hit_order() {
    let mut h = vec![
        ("a", 0.5f32, 3u64),
        ("b", 0.7, 9),
        ("c", 0.5, 1),
        ("d", -0.2, 0),
    ];
    order_hits(&mut h);
    let ids: Vec<&str> = h.iter().map(|x| x.0).collect();
    assert_eq!(ids, ["b", "c", "a", "d"]);
}

/// Lower case and the accents of the differential's words dropped: what
/// the servers' `_ai_ci` collations do to them.
#[derive(Debug)]
struct AiFold;

impl Fold for AiFold {
    fn term(&self, word: &str) -> String {
        word.to_lowercase()
            .chars()
            .map(|c| match c {
                'é' | 'è' => 'e',
                'ï' | 'í' => 'i',
                'ü' | 'ú' => 'u',
                c => c,
            })
            .collect()
    }
}

/// The differential's corpus and queries answered by the model
/// (`tools/ftdiff.py --dump DIR`, then this with `FTDIFF_DIR=DIR`, then
/// `ftdiff.py --model DIR`). Does nothing without the variable.
#[test]
fn ftdiff_model() {
    use std::fmt::Write as _;
    let Ok(dir) = std::env::var("FTDIFF_DIR") else {
        return;
    };
    let a = Analyzer::new(Rules::default(), Arc::new(AiFold));
    let docs = std::fs::read_to_string(format!("{dir}/docs.tsv")).unwrap_or_default();
    let mut ix = MemIndex::default();
    let mut ids: Vec<i64> = Vec::new();
    for l in docs.lines() {
        let f: Vec<&str> = l.splitn(3, '\t').collect();
        let id = f[0].parse::<i64>().unwrap_or(0);
        ix.add(
            &a,
            &[
                f.get(1).copied().unwrap_or(""),
                f.get(2).copied().unwrap_or(""),
            ],
        );
        ids.push(id);
    }
    let mut sorted = ids.clone();
    sorted.sort_unstable();
    let qs = std::fs::read_to_string(format!("{dir}/queries.tsv")).unwrap_or_default();
    let mut out = String::new();
    for l in qs.lines() {
        let f: Vec<&str> = l.splitn(3, '\t').collect();
        let (i, mode, q) = (f[0], f[1], f.get(2).copied().unwrap_or(""));
        let node = if mode == "B" {
            parse_boolean(q, &a)
        } else {
            Ok(parse_natural(q, &a))
        };
        let node = match node {
            Ok(n) if mode == "E" => {
                let mut first: Vec<(i64, f32, u64)> =
                    rank(&n, &ix).into_iter().map(|(d, s)| (0, s, d)).collect();
                order_hits(&mut first);
                first.truncate(10_000);
                let words: Vec<String> = first.iter().flat_map(|h| ix.terms_of(h.2)).collect();
                expand(n, words)
            }
            Ok(n) => n,
            Err(e) => {
                for k in ["w", "a"] {
                    let _ = writeln!(out, "#Q{i}{k}\nERROR 1064 (42000) at line 1: {}", e.0);
                }
                continue;
            }
        };
        let mut hits: Vec<(i64, f32, u64)> = rank(&node, &ix)
            .into_iter()
            .map(|(d, s)| (ids[usize::try_from(d).unwrap_or(0)], s, d))
            .collect();
        order_hits(&mut hits);
        let _ = writeln!(out, "#Q{i}w");
        for (id, s, _) in &hits {
            let _ = writeln!(out, "{id}\t{}", f64::from(*s));
        }
        let _ = writeln!(out, "#Q{i}a");
        let by: HashMap<i64, f32> = hits.iter().map(|(id, s, _)| (*id, *s)).collect();
        for id in &sorted {
            let s = by.get(id).copied().unwrap_or(0.0);
            let _ = writeln!(out, "{id}\t{}", f64::from(s));
        }
    }
    std::fs::write(format!("{dir}/model.out"), out).unwrap_or_else(|e| panic!("{e}"));
}

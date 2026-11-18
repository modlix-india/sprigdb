use super::*;

#[test]
fn rows_are_planned_from_their_hashes() {
    let h = Some(&b"0123456789abcdef"[..]);
    let other = Some(&b"fedcba9876543210"[..]);
    // Current: nothing to do, whatever the source.
    assert_eq!(plan_row(Some(b"red shoes"), h, h), Action::Skip);
    // A NULL source whose hash (NULL) is stored: current too.
    assert_eq!(plan_row(None, None, None), Action::Skip);
    // A new or changed source is embedded.
    assert_eq!(
        plan_row(Some(b"red shoes"), h, None),
        Action::Embed("red shoes".into())
    );
    assert_eq!(
        plan_row(Some(b"red shoes"), h, other),
        Action::Embed("red shoes".into())
    );
    // A source gone NULL or blank clears the vector.
    assert_eq!(plan_row(None, None, h), Action::Clear);
    assert_eq!(plan_row(Some(b"  \n"), other, h), Action::Clear);
}

#[test]
fn inputs_are_cut_at_a_character_boundary() {
    assert_eq!(cut("abc", 10), "abc");
    assert_eq!(cut("abcdef", 3), "abc");
    // "é" is two bytes: a cut inside it backs off.
    assert_eq!(cut("aé", 2), "a");
    assert_eq!(cut("", 0), "");
}

#[test]
fn the_jobs_own_writes_are_passed_over() {
    let col = "embedding";
    assert!(wants_op(Op::Insert, None, col));
    assert!(wants_op(Op::Update, None, col));
    assert!(wants_op(Op::Update, Some(&["name".to_owned()]), col));
    assert!(wants_op(
        Op::Update,
        Some(&["embedding".to_owned(), "name".to_owned()]),
        col
    ));
    // Only the vector changed (with its hidden hash, which the feed
    // doesn't show): the job's own write.
    assert!(!wants_op(Op::Update, Some(&["Embedding".to_owned()]), col));
    // Only hidden columns changed (a shadow generation's write).
    assert!(!wants_op(Op::Update, Some(&[]), col));
    assert!(!wants_op(Op::Delete, None, col));
    assert!(!wants_op(Op::Truncate, None, col));
}

#[test]
fn the_throttle_keeps_its_rate() {
    let t0 = Instant::now();
    let mut t = Throttle::new(100);
    // A second's worth at once, then nothing.
    assert_eq!(t.take(500, t0), 100);
    assert_eq!(t.take(1, t0), 0);
    assert!(t.wait() > Duration::ZERO);
    // A tenth of a second later: ten more.
    assert_eq!(t.take(500, t0 + Duration::from_millis(100)), 10);
    // Never more than a second saved.
    assert_eq!(t.take(1000, t0 + Duration::from_secs(60)), 100);
    // Spending past zero makes the next ones wait longer.
    t.spend(200, t0 + Duration::from_secs(60));
    assert!(t.wait() >= Duration::from_secs(2));
    // A higher rate refills faster: a second at 1,000 a second covers
    // the 200 owed and leaves 800.
    t.set_rate(1000);
    assert_eq!(t.take(10_000, t0 + Duration::from_secs(61)), 800);
}

#[test]
fn vectors_are_written_as_text() {
    assert_eq!(vec_text(&[0.5, -1.0, 0.0]), "[0.5,-1,0]");
    assert_eq!(vec_text(&[]), "[]");
}

#[test]
fn the_stale_hash_check_reads_keys_and_flags() {
    let t = |vec: &str, src: &str, hash: &str| Target {
        vec: vec.into(),
        src: src.into(),
        hash: hash.into(),
        max_tokens: 512,
    };
    let targets = [t("emb", "_sprig_emb_emb_src", "_sprig_emb_emb_hash")];
    let pk = ["org".to_owned(), "id".to_owned()];
    assert_eq!(
        scan_sql("`s1`.`t`", &pk, &targets, None, 50),
        "SELECT `org`, `id`, (NOT (`_sprig_emb_emb_hash` <=> UNHEX(MD5(`_sprig_emb_emb_src`)))) \
         FROM `s1`.`t` WHERE TRUE ORDER BY `org`, `id` LIMIT 50"
    );
    // After a key; every generation filled is checked (a re-embedding's
    // shadow too).
    let two = [
        targets[0].clone(),
        t(
            "_sprig_emb_emb_g2",
            "_sprig_emb_emb_src",
            "_sprig_emb_emb_g2_hash",
        ),
    ];
    let next = scan_sql(
        "`s1`.`t`",
        &pk,
        &two,
        Some(&["'a'".to_owned(), "7".to_owned()]),
        5,
    );
    assert!(
        next.contains("WHERE (`org`, `id`) > ('a', 7) ORDER BY `org`, `id` LIMIT 5"),
        "{next}"
    );
    assert!(
        next.contains(
            "(NOT (`_sprig_emb_emb_hash` <=> UNHEX(MD5(`_sprig_emb_emb_src`))) OR \
             NOT (`_sprig_emb_emb_g2_hash` <=> UNHEX(MD5(`_sprig_emb_emb_src`))))"
        ),
        "{next}"
    );
}

#[test]
fn the_stale_hash_check_walks_rests_and_is_throttled() {
    let t0 = Instant::now();
    // A tenth of the backfill's rate, at least one a second.
    assert_eq!(scan_rate(500), 50);
    assert_eq!(scan_rate(5), 1);
    let mut s = Scan::new(500, t0);
    // Resting after the job starts.
    assert_eq!(s.take(500, t0), 0);
    assert_eq!(s.take(500, t0 + Duration::from_secs(59)), 0);
    let at = t0 + SCAN_REST;
    // Then a second's worth, then throttled.
    assert_eq!(s.take(500, at), 50);
    assert_eq!(s.take(500, at), 0);
    // A full window goes on after its last key; a short one ends the
    // shard.
    s.advance(Some(vec!["7".into()]), 50, 50);
    assert_eq!((s.shard, s.after.clone()), (0, Some(vec!["7".to_owned()])));
    s.advance(Some(vec!["9".into()]), 3, 50);
    assert_eq!((s.shard, s.after.clone()), (1, None));
    s.advance(None, 0, 50);
    assert_eq!((s.shard, s.after.clone()), (2, None));
    // The pass ends: back to the start after a rest, with what it repaired.
    s.repaired = 3;
    let end = at + Duration::from_secs(10);
    assert_eq!(s.pass_done(end), 3);
    assert_eq!((s.shard, s.after.clone(), s.repaired), (0, None, 0));
    assert_eq!(s.take(500, end + Duration::from_secs(5)), 0);
    assert!(s.take(500, end + SCAN_REST) > 0);
}

#[test]
fn flags_read_from_either_protocol() {
    use mysql_async::Value;
    assert!(flag(Some(&Value::Bytes(b"1".to_vec()))));
    assert!(!flag(Some(&Value::Bytes(b"0".to_vec()))));
    assert!(flag(Some(&Value::Int(1))));
    assert!(!flag(Some(&Value::Int(0))));
    assert!(!flag(Some(&Value::NULL)));
    assert!(!flag(None));
}

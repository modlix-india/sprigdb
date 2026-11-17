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

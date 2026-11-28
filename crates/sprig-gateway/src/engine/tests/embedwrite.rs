//! The embedded-column runner's writes (`crate::exec::embed_ddl::write`):
//! only the column and its hash may be set, `ON UPDATE CURRENT_TIMESTAMP`
//! columns stay as they are, and every constraint is checked as always.

use super::*;
use crate::exec::embed_ddl::write;

const EMB: &str = "CREATE TABLE `emb` (\n  `id` int(11) NOT NULL,\n  `body` text DEFAULT NULL,\n  `secret` varchar(20) DEFAULT NULL,\n  `at` datetime NOT NULL DEFAULT '2000-01-01 00:00:00' ON UPDATE current_timestamp(),\n  `v` varbinary(8) DEFAULT NULL,\n  `_sprig_emb_v_hash` binary(16) DEFAULT NULL INVISIBLE,\n  PRIMARY KEY (`id`),\n  CONSTRAINT `short` CHECK (octet_length(`v`) <= 8)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci";

fn with_emb() -> Fake {
    let mut f = Fake::new();
    let p = table::parse(ObjectId(40), 1, DB, EMB);
    assert!(p.is_ok(), "{p:?}");
    f.tables.insert("emb".into(), Arc::new(p));
    f
}

async fn at(e: &Engine, f: &Fake) -> Vec<Vec<Datum>> {
    run(e, f, "SELECT at FROM emb WHERE id = 1", &[])
        .await
        .unwrap()
        .rows
}

fn cols() -> Vec<String> {
    vec!["v".to_owned(), "_sprig_emb_v_hash".to_owned()]
}

#[tokio::test]
async fn the_runners_write_sets_only_its_column_and_keeps_on_update_columns() {
    let (e, _d) = mem();
    let f = with_emb();
    run(
        &e,
        &f,
        "INSERT INTO emb (id, body, secret) VALUES (1, 'a', 's')",
        &[],
    )
    .await
    .unwrap();
    let before = at(&e, &f).await;
    // The runner's write: the vector and its hash change, `at` doesn't.
    let o = write::scope(
        cols(),
        run(
            &e,
            &f,
            "UPDATE emb SET v = X'0000803F', _sprig_emb_v_hash = X'00112233445566778899AABBCCDDEEFF' WHERE id = 1",
            &[],
        ),
    )
    .await
    .unwrap();
    assert_eq!(o.affected, 1);
    assert_eq!(
        at(&e, &f).await,
        before,
        "ON UPDATE CURRENT_TIMESTAMP left alone"
    );
    // Any other column is refused, and nothing changes.
    let r = write::scope(
        cols(),
        run(
            &e,
            &f,
            "UPDATE emb SET v = NULL, secret = 'x' WHERE id = 1",
            &[],
        ),
    )
    .await;
    assert!(r.is_err(), "a user column through the runner's write");
    let got = run(&e, &f, "SELECT secret, v FROM emb WHERE id = 1", &[])
        .await
        .unwrap()
        .rows;
    assert_eq!(
        got,
        vec![vec![
            s("s"),
            Datum::Bytes(Bytes::from_static(&[0, 0, 0x80, 0x3F]))
        ]]
    );
    // Constraints still hold: a value the CHECK refuses.
    let r = write::scope(
        cols(),
        run(
            &e,
            &f,
            "UPDATE emb SET v = X'000000000000000000' WHERE id = 1",
            &[],
        ),
    )
    .await;
    assert!(r.is_err(), "CHECK still checked");
    // Only a single-table UPDATE runs under the switch: no insert, replace,
    // upsert or delete.
    for sql in [
        "INSERT INTO emb (id, body) VALUES (2, 'x')",
        "REPLACE INTO emb (id, body) VALUES (1, 'x')",
        "INSERT INTO emb (id, body) VALUES (1, 'x') ON DUPLICATE KEY UPDATE v = NULL",
        "DELETE FROM emb WHERE id = 1",
    ] {
        let r = write::scope(cols(), run(&e, &f, sql, &[])).await;
        assert!(r.is_err(), "{sql} under the runner's write");
    }
    let n = run(&e, &f, "SELECT id FROM emb", &[]).await.unwrap().rows;
    assert_eq!(n, vec![vec![Datum::Int(1)]]);
    // Outside the runner's write, an update bumps `at` as usual.
    run(&e, &f, "UPDATE emb SET body = 'b' WHERE id = 1", &[])
        .await
        .unwrap();
    assert_ne!(at(&e, &f).await, before);
}

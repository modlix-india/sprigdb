//! Row-level security applied by the store: a session's policies as a
//! row guard over reads, the targets of updates and deletes, and new
//! documents' checks.

#![allow(
    clippy::unwrap_used,
    clippy::too_many_lines,
    clippy::doc_markdown,
    clippy::many_single_char_names
)]

#[path = "docstore_common/mod.rs"]
mod common;

use std::collections::HashMap;
use std::sync::Arc;

use bson::{Document, doc};
use common::{Bed, TENANT, ids};
use sprig_doc::docstore::Name;
use sprig_doc::docstore::guard::{self, CollPolicies, Filter, RowGuard};
use sprig_doc::filter::parse;

fn f(permissive: &[Document], restrictive: &[Document]) -> Filter {
    Filter {
        permissive: permissive.iter().map(|d| parse(d).unwrap()).collect(),
        restrictive: restrictive.iter().map(|d| parse(d).unwrap()).collect(),
    }
}

/// Owner `a` sees, changes, deletes and writes its own documents of
/// `d.c`, all of them under 100.
fn guard_for(b: &Bed) -> Arc<RowGuard> {
    let c = b.store.coll(TENANT, &Name::new("d", "c")).unwrap().unwrap();
    let own = doc! {"owner": "a"};
    let small = doc! {"n": {"$lt": 100}};
    let p = CollPolicies {
        select: f(std::slice::from_ref(&own), std::slice::from_ref(&small)),
        update_using: f(std::slice::from_ref(&own), &[]),
        update_check: f(std::slice::from_ref(&own), &[small]),
        delete_using: f(std::slice::from_ref(&own), &[]),
        insert_check: f(&[own], &[]),
    };
    let mut m = HashMap::new();
    m.insert(c.space.object, Arc::new(p));
    Arc::new(RowGuard::new(m))
}

async fn as_a(b: &Bed, g: &Arc<RowGuard>, db: &str, body: Document) -> Document {
    guard::scope(Some(Arc::clone(g)), b.run(db, body)).await
}

fn first(r: &Document) -> Vec<Document> {
    r.get_document("cursor")
        .unwrap_or_else(|_| panic!("{r}"))
        .get_array("firstBatch")
        .unwrap()
        .iter()
        .map(|x| x.as_document().unwrap().clone())
        .collect()
}

async fn seed(b: &Bed) {
    let r = b
        .run(
            "d",
            doc! {"insert": "c", "documents": [
                {"_id": 1, "owner": "a", "n": 1},
                {"_id": 2, "owner": "b", "n": 2},
                {"_id": 3, "owner": "a", "n": 300},
                {"_id": 4, "owner": "A", "n": 4},
                {"_id": 5, "owner": "a", "n": 5},
                {"_id": 6, "owner": "b", "n": 6},
            ]},
        )
        .await;
    assert_eq!(r.get_i32("n"), Ok(6), "{r}");
    b.run(
        "d",
        doc! {"insert": "o", "documents": [{"_id": 10, "ref": 1}, {"_id": 11, "ref": 2}]},
    )
    .await;
}

#[tokio::test]
async fn reads_see_only_what_the_policies_let_through() {
    let b = Bed::mem();
    seed(&b).await;
    let g = guard_for(&b);
    // Unguarded: everything.
    assert_eq!(ids(&first(&b.run("d", doc! {"find": "c"}).await)).len(), 6);
    let r = as_a(&b, &g, "d", doc! {"find": "c", "sort": {"_id": 1}}).await;
    assert_eq!(ids(&first(&r)), [1, 5]);
    // A case-insensitive collation doesn't widen the policy.
    let r = as_a(
        &b,
        &g,
        "d",
        doc! {"find": "c", "filter": {"owner": "a"}, "collation": {"locale": "en", "strength": 2}},
    )
    .await;
    assert_eq!(ids(&first(&r)), [1, 5]);
    // Later batches too.
    let r = as_a(&b, &g, "d", doc! {"find": "c", "batchSize": 1}).await;
    assert_eq!(ids(&first(&r)), [1]);
    let id = r.get_document("cursor").unwrap().get_i64("id").unwrap();
    let r = as_a(&b, &g, "d", doc! {"getMore": id, "collection": "c"}).await;
    let next: Vec<i32> = r
        .get_document("cursor")
        .unwrap()
        .get_array("nextBatch")
        .unwrap()
        .iter()
        .map(|x| x.as_document().unwrap().get_i32("_id").unwrap())
        .collect();
    assert_eq!(next, [5]);
    let r = as_a(&b, &g, "d", doc! {"count": "c"}).await;
    assert_eq!(
        r.get_i64("n").or_else(|_| r.get_i32("n").map(i64::from)),
        Ok(2),
        "{r}"
    );
    let r = as_a(&b, &g, "d", doc! {"distinct": "c", "key": "owner"}).await;
    assert_eq!(r.get_array("values").unwrap().len(), 1, "{r}");
    let r = as_a(
        &b,
        &g,
        "d",
        doc! {"aggregate": "c", "pipeline": [{"$group": {"_id": null, "k": {"$sum": 1}}}], "cursor": {}},
    )
    .await;
    assert_eq!(first(&r)[0].get_i32("k"), Ok(2), "{r}");
    // A $lookup into the protected collection reads through the guard.
    let r = as_a(
        &b,
        &g,
        "d",
        doc! {"aggregate": "o", "pipeline": [
        {"$lookup": {"from": "c", "localField": "ref", "foreignField": "_id", "as": "j"}},
        {"$sort": {"_id": 1}}], "cursor": {}},
    )
    .await;
    let joined: Vec<usize> = first(&r)
        .iter()
        .map(|d| d.get_array("j").unwrap().len())
        .collect();
    assert_eq!(joined, [1, 0]);
    // A view over it too.
    b.run(
        "d",
        doc! {"create": "v", "viewOn": "c", "pipeline": [{"$match": {"n": {"$gte": 0}}}]},
    )
    .await;
    let r = as_a(&b, &g, "d", doc! {"find": "v", "sort": {"_id": 1}}).await;
    assert_eq!(ids(&first(&r)), [1, 5]);
}

#[tokio::test]
async fn writes_touch_only_what_they_may_and_check_new_documents() {
    let b = Bed::mem();
    seed(&b).await;
    let g = guard_for(&b);
    // An update reaches the visible owner-a documents only.
    let r = as_a(
        &b,
        &g,
        "d",
        doc! {"update": "c", "updates": [{"q": {}, "u": {"$inc": {"n": 10}}, "multi": true}]},
    )
    .await;
    assert_eq!(r.get_i32("n"), Ok(2), "{r}");
    let all = b
        .find_all_cmd("d", doc! {"find": "c", "sort": {"_id": 1}})
        .await;
    let ns: Vec<i32> = all.iter().map(|d| d.get_i32("n").unwrap()).collect();
    assert_eq!(ns, [11, 2, 300, 4, 15, 6]);
    // A new version the check refuses fails the statement: nothing changes.
    let r = as_a(
        &b,
        &g,
        "d",
        doc! {"update": "c", "updates": [{"q": {"_id": 1}, "u": {"$set": {"owner": "b"}}}]},
    )
    .await;
    let code = r
        .get_array("writeErrors")
        .ok()
        .and_then(|w| w[0].as_document()?.get_i32("code").ok())
        .or_else(|| r.get_i32("code").ok());
    assert_eq!(code, Some(guard::RLS_CHECK_FAILED), "{r}");
    let one = b
        .find_all_cmd("d", doc! {"find": "c", "filter": {"_id": 1}})
        .await;
    assert_eq!(one[0].get_str("owner"), Ok("a"));
    // findAndModify sees what an update may touch.
    let r = as_a(
        &b,
        &g,
        "d",
        doc! {"findAndModify": "c", "query": {"_id": 2}, "update": {"$set": {"x": 1}}},
    )
    .await;
    assert_eq!(
        r.get_document("lastErrorObject").unwrap().get_i32("n"),
        Ok(0),
        "{r}"
    );
    // Inserts and upserts pass INSERT's check.
    let r = as_a(
        &b,
        &g,
        "d",
        doc! {"insert": "c", "documents": [{"_id": 7, "owner": "b", "n": 7}]},
    )
    .await;
    assert!(
        r.get_array("writeErrors").is_ok() || r.get_f64("ok") == Ok(0.0),
        "{r}"
    );
    let r = as_a(
        &b,
        &g,
        "d",
        doc! {"insert": "c", "documents": [{"_id": 8, "owner": "a", "n": 8}]},
    )
    .await;
    assert_eq!(r.get_i32("n"), Ok(1), "{r}");
    let r = as_a(
        &b,
        &g,
        "d",
        doc! {"update": "c", "updates": [{"q": {"_id": 9}, "u": {"$set": {"owner": "b"}}, "upsert": true}]},
    )
    .await;
    assert!(
        r.get_array("writeErrors").is_ok() || r.get_f64("ok") == Ok(0.0),
        "{r}"
    );
    // A delete removes what it may only.
    let r = as_a(
        &b,
        &g,
        "d",
        doc! {"delete": "c", "deletes": [{"q": {}, "limit": 0}]},
    )
    .await;
    assert_eq!(r.get_i32("n"), Ok(3), "{r}");
    let left = b
        .find_all_cmd("d", doc! {"find": "c", "sort": {"_id": 1}})
        .await;
    assert_eq!(ids(&left), [2, 3, 4, 6]);
}

#[tokio::test]
async fn no_permissive_policy_shows_nothing() {
    let b = Bed::mem();
    seed(&b).await;
    let c = b.store.coll(TENANT, &Name::new("d", "c")).unwrap().unwrap();
    let mut m = HashMap::new();
    m.insert(c.space.object, Arc::new(CollPolicies::default()));
    let g = Arc::new(RowGuard::new(m));
    let r = as_a(&b, &g, "d", doc! {"find": "c"}).await;
    assert!(first(&r).is_empty());
    let r = as_a(
        &b,
        &g,
        "d",
        doc! {"delete": "c", "deletes": [{"q": {}, "limit": 0}]},
    )
    .await;
    assert_eq!(r.get_i32("n"), Ok(0), "{r}");
    // Other collections aren't filtered.
    let r = as_a(&b, &g, "d", doc! {"find": "o"}).await;
    assert_eq!(first(&r).len(), 2);
}

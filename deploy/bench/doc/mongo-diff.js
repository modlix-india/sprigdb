// The MongoDB differential corpora replayed against any MongoDB-compatible
// endpoint, compared with the answers MongoDB 8.0.34 recorded in
// crates/sprig-doc/tests/{expr_diff,stage_diff,validate_diff}/cases.json:
//   expr      aggregation expressions: values with exact BSON types (doubles
//             bit for bit), errors by code;
//   stage     pipelines over the case's documents (and its extra collections);
//   validate  $jsonSchema / query validators on insert (code and errInfo),
//             and filters (the _ids a find matches, or the error code).
// Each case runs over a real collection (the Rust tests use the engine or
// $documents; a collection is fairer to products without $documents).
// Prints one JSON summary line per corpus; mismatches go to stderr;
// MDIFF_CASES_OUT=<file> also writes every case's outcome.
//
// Usage: NODE_PATH=<node_modules with mongodb> node mongo-diff.js <uri> <db> <corpus dir> [expr,stage,validate]
const { MongoClient, BSON } = require('mongodb');
const { EJSON } = BSON;
const fs = require('fs');
const path = require('path');
const [uri, dbName, dir, which = 'expr,stage,validate'] = process.argv.slice(2);

const canon = (v) => EJSON.stringify(v, { relaxed: false });
const load = (n) => EJSON.parse(fs.readFileSync(path.join(dir, n, 'cases.json'), 'utf8'), { relaxed: false });
const errOf = (e) => ({ code: e.code !== undefined && !isNaN(Number(e.code)) ? Number(e.code) : -1, msg: String(e.errmsg || e.message || e) });

async function main() {
  const client = new MongoClient(uri, { serverSelectionTimeoutMS: 10000, promoteValues: false, promoteLongs: false, promoteBuffers: false });
  await client.connect();
  const db = client.db(dbName);
  const c = db.collection('bench_diff');
  const out = {};
  const perCase = {};
  for (const corpus of which.split(',')) {
    const cases = load(`${corpus}_diff`);
    let total = 0, matched = 0, codeOnly = 0;
    perCase[corpus] = {};
    for (const k of cases) {
      total++;
      let got, want = k.expect, ok = false;
      try {
        if (corpus === 'expr') {
          await c.deleteMany({});
          if (k.group) {
            if (k.docs.length) await c.insertMany(k.docs);
            got = { ok: await c.aggregate([{ $group: k.group }, { $sort: { _id: 1 } }]).toArray() };
          } else {
            await c.insertOne(k.doc);
            got = { ok: await c.aggregate([{ $replaceWith: { r: k.expr } }]).toArray() };
          }
        } else if (corpus === 'stage') {
          await c.deleteMany({});
          if (k.docs && k.docs.length) await c.insertMany(k.docs);
          for (const [name, docs] of Object.entries(k.colls || {})) {
            await db.collection(name).deleteMany({});
            if (docs.length) await db.collection(name).insertMany(docs);
          }
          got = { ok: k.db ? await db.aggregate(k.pipeline).toArray() : await c.aggregate(k.pipeline).toArray() };
        } else if (k.validator) {
          await db.collection('bench_val').drop().catch(() => {});
          try {
            await db.createCollection('bench_val', { validator: k.validator });
          } catch (e) { got = { create: errOf(e) }; }
          if (!got) {
            try { await db.collection('bench_val').insertOne(k.doc); got = { ok: true }; }
            catch (e) { got = { ...errOf(e), errInfo: e.errInfo }; }
          }
        } else {
          await c.deleteMany({});
          if (k.docs.length) await c.insertMany(k.docs);
          got = { ids: (await c.find(k.filter, { projection: { _id: 1 } }).sort({ _id: 1 }).toArray()).map((d) => d._id) };
        }
      } catch (e) { got = errOf(e); }
      // Compare.
      if (want.ok !== undefined && got.ok !== undefined) ok = canon(want.ok) === canon(got.ok);
      else if (want.ids !== undefined && got.ids !== undefined) ok = canon(want.ids) === canon(got.ids);
      else {
        const wcode = want.code !== undefined ? Number(want.code) : (want.create && Number(want.create.code));
        const gcode = got.code !== undefined ? got.code : (got.create && got.create.code);
        ok = wcode !== undefined && wcode === gcode;
        if (ok && want.errInfo !== undefined) ok = canon(want.errInfo) === canon(got.errInfo);
        if (ok && want.errmsg && got.msg && want.errmsg !== got.msg) codeOnly++;
      }
      perCase[corpus][k.name] = ok;
      if (ok) matched++;
      else process.stderr.write(`DIFF ${corpus} ${k.name}\n  want ${canon(want).slice(0, 300)}\n  got  ${canon(got).slice(0, 300)}\n`);
    }
    out[corpus] = { total, matched, same_code_other_message: codeOnly };
    console.log(JSON.stringify({ corpus, total, matched, same_code_other_message: codeOnly }));
  }
  if (process.env.MDIFF_CASES_OUT) fs.writeFileSync(process.env.MDIFF_CASES_OUT, JSON.stringify(perCase));
  await c.drop().catch(() => {});
  await db.collection('bench_val').drop().catch(() => {});
  await client.close();
}
main().catch((e) => { console.error('FATAL', e); process.exit(1); });

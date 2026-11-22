//! `SHOW` statements about tables, answered from the engine's definitions
//! in MariaDB 11.8's shape (physical schema names, its column definitions
//! and values), so the gateway's name mapping and the MySQL port's
//! rewrites apply to them exactly as they did to MariaDB's answers:
//! `SHOW CREATE TABLE`, `SHOW [FULL] COLUMNS`, `SHOW INDEX` and
//! `SHOW [FULL] TABLES`.

use bytes::{BufMut, Bytes, BytesMut};
use sprig_proto_mysql::resultset::encode_text_row;

use super::define::{self, Def, Default, KeyKind};

/// A result in the backend's shape: column definition payloads and text
/// rows.
pub(crate) struct Answer {
    pub defs: Vec<Bytes>,
    pub rows: Vec<Bytes>,
    /// The server marks the answer `SERVER_QUERY_NO_INDEX_USED`.
    pub no_index: bool,
}

fn lenenc_str(b: &mut BytesMut, s: &[u8]) {
    let n = s.len();
    if n < 251 {
        b.put_u8(u8::try_from(n).unwrap_or(0));
    } else if n < 1 << 16 {
        b.put_u8(0xfc);
        b.put_u16_le(u16::try_from(n).unwrap_or(0));
    } else {
        b.put_u8(0xfd);
        let v = u32::try_from(n).unwrap_or(0);
        b.put_u8((v & 0xff) as u8);
        b.put_u8(((v >> 8) & 0xff) as u8);
        b.put_u8(((v >> 16) & 0xff) as u8);
    }
    b.put_slice(s);
}

/// One column definition payload (protocol 4.1, without extended metadata).
#[allow(clippy::too_many_arguments)] // the fields of the packet, in order
pub(crate) fn coldef(
    schema: &str,
    table: &str,
    name: &str,
    org: &str,
    charset: u16,
    len: u32,
    ty: u8,
    flags: u16,
    decimals: u8,
) -> Bytes {
    let mut b = BytesMut::with_capacity(64);
    lenenc_str(&mut b, b"def");
    lenenc_str(&mut b, schema.as_bytes());
    lenenc_str(&mut b, table.as_bytes());
    lenenc_str(&mut b, table.as_bytes());
    lenenc_str(&mut b, name.as_bytes());
    lenenc_str(&mut b, org.as_bytes());
    b.put_u8(0x0c);
    b.put_u16_le(charset);
    b.put_u32_le(len);
    b.put_u8(ty);
    b.put_u16_le(flags);
    b.put_u8(decimals);
    b.put_u16_le(0);
    b.freeze()
}

const IS: &str = "information_schema";
const VARSTR: u8 = 0xfd;
const BLOB: u8 = 0xfc;
const LONGLONG: u8 = 0x08;
const LONG: u8 = 0x03;

/// `SHOW CREATE TABLE`: `text` is the definition to show.
pub(crate) fn create_table(name: &str, text: &str) -> Answer {
    Answer {
        defs: vec![
            coldef("", "", "Table", "", 224, 256, VARSTR, 0x0001, 0x27),
            coldef(
                "",
                "",
                "Create Table",
                "",
                224,
                4 * u32::try_from(text.chars().count().max(1024)).unwrap_or(u32::MAX / 4),
                VARSTR,
                0x0001,
                0x27,
            ),
        ],
        rows: vec![encode_text_row([
            Some(name.as_bytes()),
            Some(text.as_bytes()),
        ])],
        no_index: false,
    }
}

/// The definition's text with `AUTO_INCREMENT=next` as the server shows it
/// (the next value the table gives, when it is above 1).
pub(crate) fn with_counter(def: &Def, next: Option<u64>) -> String {
    let mut d = def.clone();
    d.auto_increment = next;
    // MariaDB prints neither `RESTRICT` (the default) nor `SET DEFAULT`
    // (which it doesn't keep); the stored definition keeps them as written.
    for f in &mut d.fks {
        for a in [&mut f.on_delete, &mut f.on_update] {
            if a.as_deref()
                .is_some_and(|x| x == "RESTRICT" || x == "SET DEFAULT")
            {
                *a = None;
            }
        }
    }
    define::render(&d)
}

/// `KEY` of `SHOW COLUMNS`: `PRI`, `UNI`, `MUL` or empty.
fn key_of(def: &Def, col: &str) -> &'static str {
    let first = |k: &define::Key| {
        k.parts
            .first()
            .is_some_and(|p| p.col.eq_ignore_ascii_case(col))
    };
    if def
        .primary()
        .is_some_and(|k| k.parts.iter().any(|p| p.col.eq_ignore_ascii_case(col)))
    {
        return "PRI";
    }
    if def
        .keys
        .iter()
        .any(|k| k.kind == KeyKind::Unique && k.parts.len() == 1 && first(k))
    {
        return "UNI";
    }
    if def.keys.iter().any(first) {
        return "MUL";
    }
    ""
}

/// A default as `SHOW COLUMNS` and `information_schema.COLUMNS` show it:
/// constants without their quotes.
fn default_text(c: &define::Col) -> Option<String> {
    match &c.default {
        None | Some(Default::Null) => None,
        Some(Default::Lit(l)) => Some(unquote(l)),
        Some(Default::Raw(e) | Default::Expr(e)) => Some(define::expr::print(e)),
        Some(Default::Now(p)) => Some(if *p == 0 {
            "current_timestamp()".into()
        } else {
            format!("current_timestamp({p})")
        }),
    }
}

pub(crate) fn unquote(l: &str) -> String {
    if l.len() >= 2 && l.starts_with('\'') && l.ends_with('\'') {
        let inner = &l[1..l.len() - 1];
        let mut o = String::with_capacity(inner.len());
        let mut it = inner.chars().peekable();
        while let Some(ch) = it.next() {
            if ch == '\'' && it.peek() == Some(&'\'') {
                it.next();
                o.push('\'');
            } else if ch == '\\' {
                match it.next() {
                    Some('n') => o.push('\n'),
                    Some('r') => o.push('\r'),
                    Some('0') => o.push('\0'),
                    Some('Z') => o.push('\u{1a}'),
                    Some(x) => o.push(x),
                    None => {}
                }
            } else {
                o.push(ch);
            }
        }
        o
    } else {
        l.to_owned()
    }
}

/// `EXTRA` of `SHOW COLUMNS`.
fn extra_of(c: &define::Col) -> String {
    let mut parts: Vec<String> = Vec::new();
    if c.auto_inc {
        parts.push("auto_increment".into());
    }
    if let Some(p) = c.on_update {
        parts.push(if p == 0 {
            "on update current_timestamp()".into()
        } else {
            format!("on update current_timestamp({p})")
        });
    }
    if let Some((_, stored)) = &c.generated {
        parts.push(if *stored {
            "STORED GENERATED".into()
        } else {
            "VIRTUAL GENERATED".into()
        });
    }
    if c.invisible {
        parts.push("INVISIBLE".into());
    }
    parts.join(" ")
}

/// `SHOW [FULL] COLUMNS FROM t [LIKE p]`.
#[allow(clippy::too_many_lines)] // the server's columns, in order
pub(crate) fn columns(def: &Def, full: bool, like: Option<&dyn Fn(&str) -> bool>) -> Answer {
    let t = "COLUMNS";
    let mut defs = vec![
        coldef(IS, t, "Field", "COLUMN_NAME", 224, 256, VARSTR, 0x1001, 0),
        coldef(
            IS,
            t,
            "Type",
            "COLUMN_TYPE",
            224,
            0x000b_fff4,
            BLOB,
            0x1011,
            0,
        ),
    ];
    if full {
        defs.push(coldef(
            IS,
            t,
            "Collation",
            "COLLATION_NAME",
            224,
            256,
            VARSTR,
            0x1000,
            0,
        ));
    }
    defs.extend([
        coldef(IS, t, "Null", "IS_NULLABLE", 224, 12, VARSTR, 0x1001, 0),
        coldef(IS, t, "Key", "COLUMN_KEY", 224, 12, VARSTR, 0x1001, 0),
        coldef(
            IS,
            t,
            "Default",
            "COLUMN_DEFAULT",
            224,
            0x000b_ffd0,
            BLOB,
            0x1010,
            0,
        ),
        coldef(IS, t, "Extra", "EXTRA", 224, 320, VARSTR, 0x1001, 0),
    ]);
    if full {
        defs.push(coldef(
            IS,
            t,
            "Privileges",
            "PRIVILEGES",
            224,
            320,
            VARSTR,
            0x1001,
            0,
        ));
        defs.push(coldef(
            IS,
            t,
            "Comment",
            "COLUMN_COMMENT",
            224,
            4096,
            VARSTR,
            0x1001,
            0,
        ));
    }
    let mut rows = Vec::new();
    for c in &def.cols {
        if like.is_some_and(|f| !f(&c.name)) {
            continue;
        }
        let ty = c.ty.print();
        let coll = c.ty.cs.as_ref().map(|(_, co)| co.clone());
        let null = if c.not_null { "NO" } else { "YES" };
        let key = key_of(def, &c.name);
        let dflt = default_text(c);
        let extra = extra_of(c);
        let comment = c.comment.clone().unwrap_or_default();
        let mut vals: Vec<Option<&[u8]>> = vec![Some(c.name.as_bytes()), Some(ty.as_bytes())];
        if full {
            vals.push(coll.as_deref().map(str::as_bytes));
        }
        vals.extend([
            Some(null.as_bytes()),
            Some(key.as_bytes()),
            dflt.as_deref().map(str::as_bytes),
            Some(extra.as_bytes()),
        ]);
        if full {
            vals.push(Some(b"select,insert,update,references".as_slice()));
            vals.push(Some(comment.as_bytes()));
        }
        rows.push(encode_text_row(vals));
    }
    Answer {
        defs,
        rows,
        no_index: true,
    }
}

/// `SHOW INDEX FROM t`; `rows` is the table's row count (the
/// cardinality the engine reports for every key part).
#[allow(clippy::too_many_lines)] // the server's columns, in order
pub(crate) fn index(def: &Def, rows_estimate: u64) -> Answer {
    let t = "STATISTICS";
    let defs = vec![
        coldef(IS, t, "Table", "TABLE_NAME", 224, 256, VARSTR, 0x1001, 0),
        coldef(
            IS,
            t,
            "Non_unique",
            "NON_UNIQUE",
            63,
            1,
            LONGLONG,
            0x1001,
            0,
        ),
        coldef(IS, t, "Key_name", "INDEX_NAME", 224, 256, VARSTR, 0x1001, 0),
        coldef(
            IS,
            t,
            "Seq_in_index",
            "SEQ_IN_INDEX",
            63,
            2,
            LONG,
            0x1021,
            0,
        ),
        coldef(
            IS,
            t,
            "Column_name",
            "COLUMN_NAME",
            224,
            256,
            VARSTR,
            0x1001,
            0,
        ),
        coldef(IS, t, "Collation", "COLLATION", 224, 4, VARSTR, 0x1000, 0),
        coldef(
            IS,
            t,
            "Cardinality",
            "CARDINALITY",
            63,
            21,
            LONGLONG,
            0x1000,
            0,
        ),
        coldef(IS, t, "Sub_part", "SUB_PART", 63, 3, LONGLONG, 0x1000, 0),
        coldef(IS, t, "Packed", "PACKED", 224, 40, VARSTR, 0x1000, 0),
        coldef(IS, t, "Null", "NULLABLE", 224, 12, VARSTR, 0x1001, 0),
        coldef(
            IS,
            t,
            "Index_type",
            "INDEX_TYPE",
            224,
            64,
            VARSTR,
            0x1001,
            0,
        ),
        coldef(IS, t, "Comment", "COMMENT", 224, 64, VARSTR, 0x1001, 0),
        coldef(
            IS,
            t,
            "Index_comment",
            "INDEX_COMMENT",
            224,
            4096,
            VARSTR,
            0x1001,
            0,
        ),
        coldef(IS, t, "Ignored", "IGNORED", 224, 12, VARSTR, 0x1001, 0),
    ];
    let mut out = Vec::new();
    for k in &def.keys {
        let non_unique = if matches!(k.kind, KeyKind::Primary | KeyKind::Unique) {
            "0"
        } else {
            "1"
        };
        for (i, p) in k.parts.iter().enumerate() {
            let seq = (i + 1).to_string();
            let coll = if p.desc { "D" } else { "A" };
            let card = rows_estimate.to_string();
            let sub = p.len.map(|l| l.to_string());
            let nullable = def.col(&p.col).is_some_and(|ci| !def.cols[ci].not_null);
            let itype = k.using.clone().unwrap_or_else(|| "BTREE".into());
            let icomment = k.comment.clone().unwrap_or_default();
            let ignored = if k.ignored { "YES" } else { "NO" };
            out.push(encode_text_row([
                Some(def.name.as_bytes()),
                Some(non_unique.as_bytes()),
                Some(k.name.as_bytes()),
                Some(seq.as_bytes()),
                Some(p.col.as_bytes()),
                Some(coll.as_bytes()),
                Some(card.as_bytes()),
                sub.as_deref().map(str::as_bytes),
                None,
                Some(if nullable {
                    b"YES".as_slice()
                } else {
                    b"".as_slice()
                }),
                Some(itype.as_bytes()),
                Some(b"".as_slice()),
                Some(icomment.as_bytes()),
                Some(ignored.as_bytes()),
            ]));
        }
    }
    Answer {
        defs,
        rows: out,
        no_index: true,
    }
}

/// `SHOW [FULL] TABLES [LIKE 'p']` of physical schema `schema`: `names`
/// sorted. The column is named after the pattern too, as written.
pub(crate) fn tables(schema: &str, names: &[String], full: bool, like: Option<&str>) -> Answer {
    let t = "TABLE_NAMES";
    let mut defs = vec![coldef(
        IS,
        t,
        &match like {
            Some(p) => format!("Tables_in_{schema} ({p})"),
            None => format!("Tables_in_{schema}"),
        },
        "TABLE_NAME",
        224,
        292,
        VARSTR,
        0x1001,
        0,
    )];
    if full {
        defs.push(coldef(
            IS,
            t,
            "Table_type",
            "TABLE_TYPE",
            224,
            256,
            VARSTR,
            0x1001,
            0,
        ));
    }
    let rows = names
        .iter()
        .map(|n| {
            if full {
                encode_text_row([Some(n.as_bytes()), Some(b"BASE TABLE".as_slice())])
            } else {
                encode_text_row([Some(n.as_bytes())])
            }
        })
        .collect();
    Answer {
        defs,
        rows,
        no_index: true,
    }
}

//! Finding personal and sensitive data: the deterministic scanner behind
//! the DBA assistant's `sensitive_columns` tool.
//!
//! ```text
//! each column → name tokens, type, comment, CHECK constraints
//!   rules by category (email, phone, birth date, national id, card,
//!   bank account, address, person name, IP address, location, secret,
//!   special category, financial, device id, free text) → a finding with a
//!   confidence and its reasons; flags, counts and type words
//!   (`email_verified`, `phone_type`, `tinyint(1)`) are left alone
//! with data sharing on, sampled values of text columns → pattern counts
//!   (email, phone, card number with the Luhn check, US SSN, IBAN with its
//!   check digits, IP address, token or key): they raise, lower or add a
//!   finding; only the counts are kept, never a value
//! ```
//!
//! The gateway reads the columns and the samples as the caller; this
//! module only decides, so it is tested without a database.

#![allow(
    clippy::many_single_char_names,
    clippy::similar_names,
    clippy::struct_excessive_bools,
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::cast_sign_loss,
    clippy::cast_precision_loss
)] // generators: short names for numbers, ranges clamped before casts

use std::collections::BTreeMap;
use std::net::IpAddr;

use serde::Serialize;
use serde_json::{Value, json};

/// Columns whose values are sampled, per table, at most.
pub const MAX_SAMPLED_COLUMNS: usize = 50;
/// Tables whose values are sampled in one call, at most.
pub const MAX_SAMPLED_TABLES: usize = 20;
/// Values sampled per column, at most.
pub const SAMPLE_VALUES: usize = 200;
/// Columns one call scans, at most.
pub const MAX_COLUMNS: usize = 5000;

/// What a finding is about.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Category {
    /// Email addresses.
    Email,
    /// Phone numbers.
    Phone,
    /// Dates of birth and ages.
    BirthDate,
    /// National ids, tax ids, passports, driving licences.
    NationalId,
    /// Payment card numbers and security codes.
    CardNumber,
    /// Bank account numbers, IBANs, routing codes.
    BankAccount,
    /// Postal addresses.
    Address,
    /// People's names.
    PersonName,
    /// IP addresses.
    IpAddress,
    /// Coordinates and locations.
    Location,
    /// Passwords, tokens, keys.
    Secret,
    /// Health, ethnicity, religion, sex and the like (special categories
    /// in data-protection law).
    SpecialCategory,
    /// Salaries, incomes, credit scores.
    Financial,
    /// Device and advertising ids, MAC addresses.
    DeviceId,
    /// Free text that may hold any of the above.
    FreeText,
}

impl Category {
    /// Its name in results.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Category::Email => "email",
            Category::Phone => "phone",
            Category::BirthDate => "birth_date",
            Category::NationalId => "national_id",
            Category::CardNumber => "card_number",
            Category::BankAccount => "bank_account",
            Category::Address => "address",
            Category::PersonName => "person_name",
            Category::IpAddress => "ip_address",
            Category::Location => "location",
            Category::Secret => "secret",
            Category::SpecialCategory => "special_category",
            Category::Financial => "financial",
            Category::DeviceId => "device_id",
            Category::FreeText => "free_text",
        }
    }

    /// What protecting it usually takes, in a sentence.
    #[must_use]
    pub const fn advice(self) -> &'static str {
        match self {
            Category::Secret => {
                "store a salted hash or keep it outside the database; restrict the column with column-level grants"
            }
            Category::CardNumber => {
                "don't store full card numbers or security codes (PCI DSS): keep a payment provider's token or the last four digits"
            }
            Category::NationalId | Category::BankAccount | Category::SpecialCategory => {
                "restrict the column with column-level grants or a view, limit rows with row-level security, and consider residency for the tenant"
            }
            Category::FreeText => {
                "check what people type into it; restrict it like the most sensitive data it may hold"
            }
            _ => {
                "limit whose rows users see with row-level security, and who sees the column with column-level grants or a view"
            }
        }
    }
}

/// How sure a finding is.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Confidence {
    /// Probably not, but worth a look.
    Low,
    /// Likely.
    Medium,
    /// Almost certainly.
    High,
}

/// One column as the scanner sees it.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ColumnInfo {
    /// Its table.
    pub table: String,
    /// Its name.
    pub column: String,
    /// `DATA_TYPE`, such as `varchar`.
    pub data_type: String,
    /// `COLUMN_TYPE`, such as `varchar(255)` or `tinyint(1)`.
    pub column_type: String,
    /// `COLUMN_COMMENT`.
    pub comment: String,
    /// CHECK clauses of its table that name it.
    pub checks: Vec<String>,
}

/// Pattern matches among a column's sampled values: counts only.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct ValueMatches {
    /// The pattern that matched most.
    pub pattern: Category,
    /// Values that matched it.
    pub matched: u32,
    /// Values sampled.
    pub sampled: u32,
}

/// One flagged column.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Finding {
    /// The table.
    pub table: String,
    /// The column.
    pub column: String,
    /// Its type.
    #[serde(rename = "type")]
    pub ty: String,
    /// What it holds.
    pub category: Category,
    /// How sure.
    pub confidence: Confidence,
    /// Why.
    pub reasons: Vec<String>,
    /// Pattern counts of sampled values (data sharing on).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub values: Option<ValueMatches>,
}

/// A name's words, lower case: `customerEmail_2` → `customer`, `email`.
fn tokens(name: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut cur = String::new();
    let mut prev_lower = false;
    for c in name.chars() {
        if c.is_ascii_alphabetic() {
            if c.is_ascii_uppercase() && prev_lower && !cur.is_empty() {
                out.push(std::mem::take(&mut cur));
            }
            prev_lower = c.is_ascii_lowercase();
            cur.push(c.to_ascii_lowercase());
        } else {
            prev_lower = false;
            if !cur.is_empty() {
                out.push(std::mem::take(&mut cur));
            }
        }
    }
    if !cur.is_empty() {
        out.push(cur);
    }
    out
}

/// Words that make a column a flag, a count or a kind rather than the
/// data itself (`email_verified`, `phone_type`, `address_count`).
const NOT_DATA: &[&str] = &[
    "verified",
    "verify",
    "verification",
    "enabled",
    "disabled",
    "flag",
    "type",
    "types",
    "kind",
    "status",
    "count",
    "counts",
    "template",
    "templates",
    "format",
    "optin",
    "opt",
    "consent",
    "sent",
    "bounced",
    "valid",
    "confirmed",
    "method",
    "pref",
    "prefs",
    "preference",
    "required",
    "visible",
    "changed",
    "updated",
    "expires",
    "expiry",
    "length",
    "policy",
    "label",
    "provider",
    "domain",
];

fn is_text(data_type: &str) -> bool {
    matches!(
        data_type,
        "char"
            | "varchar"
            | "tinytext"
            | "text"
            | "mediumtext"
            | "longtext"
            | "json"
            | "enum"
            | "set"
            | "binary"
            | "varbinary"
            | "blob"
            | "tinyblob"
            | "mediumblob"
            | "longblob"
    )
}

fn is_long_text(c: &ColumnInfo) -> bool {
    match c.data_type.as_str() {
        "text" | "mediumtext" | "longtext" | "json" => true,
        "varchar" => type_len(&c.column_type).is_some_and(|n| n >= 255),
        _ => false,
    }
}

fn is_date(data_type: &str) -> bool {
    matches!(data_type, "date" | "datetime" | "timestamp")
}

fn is_number(data_type: &str) -> bool {
    matches!(
        data_type,
        "tinyint"
            | "smallint"
            | "mediumint"
            | "int"
            | "integer"
            | "bigint"
            | "decimal"
            | "numeric"
            | "float"
            | "double"
            | "real"
    )
}

/// `varchar(255)` → 255.
fn type_len(column_type: &str) -> Option<u64> {
    let open = column_type.find('(')?;
    let close = column_type[open..].find(')')? + open;
    column_type[open + 1..close]
        .split(',')
        .next()?
        .trim()
        .parse()
        .ok()
}

fn is_boolean(c: &ColumnInfo) -> bool {
    let t = c.column_type.to_ascii_lowercase();
    matches!(c.data_type.as_str(), "bool" | "boolean")
        || t.starts_with("tinyint(1)")
        || t.starts_with("bit(1)")
}

/// Tables that hold people, for a bare `name` column.
const PEOPLE_TABLES: &[&str] = &[
    "customer",
    "customers",
    "user",
    "users",
    "person",
    "persons",
    "people",
    "contact",
    "contacts",
    "employee",
    "employees",
    "member",
    "members",
    "patient",
    "patients",
    "student",
    "students",
    "client",
    "clients",
    "staff",
    "author",
    "authors",
    "applicant",
    "applicants",
    "guest",
    "guests",
    "subscriber",
    "subscribers",
    "lead",
    "leads",
];

struct Names<'a> {
    t: &'a [String],
    joined: String,
}

impl Names<'_> {
    fn has(&self, w: &str) -> bool {
        self.t.iter().any(|x| x == w)
    }
    fn any(&self, ws: &[&str]) -> bool {
        ws.iter().any(|w| self.has(w))
    }
    fn pair(&self, a: &str, bs: &[&str]) -> bool {
        self.t
            .windows(2)
            .any(|p| p[0] == a && bs.contains(&p[1].as_str()))
    }
    fn joined_any(&self, ws: &[&str]) -> bool {
        ws.iter().any(|w| self.joined == *w)
    }
    fn ends(&self, w: &str) -> bool {
        self.t.last().is_some_and(|x| x == w)
    }
}

/// What the column's name alone says: a category, a confidence and the
/// reason.
#[allow(clippy::too_many_lines)] // one rule per category
fn by_name(c: &ColumnInfo, toks: &[String]) -> Option<(Category, Confidence, String)> {
    use Category as K;
    use Confidence::{High, Low, Medium};
    let n = Names {
        t: toks,
        joined: toks.concat(),
    };
    let text = is_text(&c.data_type);
    let num = is_number(&c.data_type);
    let says = |what: &str| format!("the name says {what}");
    // Secrets first: `password_hash`, `api_key`, `refresh_token`.
    if n.any(&[
        "password", "passwd", "pwd", "passcode", "secret", "apikey", "otp", "mfa", "totp",
    ]) || n.pair("api", &["key", "secret"])
        || n.pair("access", &["key", "token"])
        || n.pair("private", &["key"])
        || n.pair("refresh", &["token"])
        || n.pair("auth", &["token"])
        || n.pair("session", &["token", "key"])
        || n.joined_any(&["token", "accesstoken", "authtoken", "privatekey"])
        || (n.ends("token") && !n.any(&["csrf"]))
    {
        return Some((K::Secret, High, says("a password, key or token")));
    }
    if n.any(&["cvv", "cvc", "cvv2", "cvc2", "csc"]) {
        return Some((
            K::CardNumber,
            High,
            says("a card security code, which must never be stored"),
        ));
    }
    if n.joined_any(&[
        "cardnumber",
        "cardno",
        "ccnumber",
        "ccnum",
        "creditcard",
        "pan",
    ]) || n.pair("card", &["number", "no", "num"])
        || n.pair("credit", &["card"])
        || (n.has("cc") && n.any(&["number", "num", "no"]))
    {
        return Some((K::CardNumber, High, says("a payment card number")));
    }
    if n.any(&["iban"]) {
        return Some((K::BankAccount, High, says("an IBAN")));
    }
    if n.any(&["bic", "swift"])
        || n.pair("routing", &["number", "no"])
        || n.pair("sort", &["code"])
        || n.pair("account", &["number", "no", "num"])
        || n.pair("bank", &["account"])
    {
        return Some((K::BankAccount, Medium, says("a bank account or code")));
    }
    if n.any(&[
        "ssn", "nino", "aadhaar", "aadhar", "passport", "nric", "cpf", "curp", "bsn",
    ]) || n.pair("social", &["security"])
        || n.pair("national", &["id", "number", "no", "insurance"])
        || n.pair("tax", &["id", "number", "no", "code"])
        || n.pair("voter", &["id"])
        || n.pair("driver", &["license", "licence"])
        || n.pair("drivers", &["license", "licence"])
        || n.pair("license", &["number", "no"])
        || n.pair("licence", &["number", "no"])
        || n.joined_any(&["nationalid", "taxid", "tin", "sin", "nin"])
    {
        return Some((
            K::NationalId,
            High,
            says("a national, tax, passport or licence number"),
        ));
    }
    // Flags, counts and kinds of the data aren't the data.
    if is_boolean(c) || n.t.last().is_some_and(|w| NOT_DATA.contains(&w.as_str())) {
        return None;
    }
    if (n.any(&["email", "emailaddress", "mail"]) || n.pair("e", &["mail"])) && text {
        return Some((K::Email, High, says("an email address")));
    }
    if n.any(&["phone", "telephone", "msisdn", "phonenumber"]) && !n.has("model") && (text || num) {
        return Some((K::Phone, High, says("a phone number")));
    }
    if n.any(&["mobile", "cell", "cellphone", "tel", "fax", "whatsapp"]) && (text || num) {
        return Some((K::Phone, Medium, says("a phone number")));
    }
    if n.any(&["dob", "birthday", "birthdate", "dateofbirth"])
        || n.has("birth")
        || n.pair("date", &["birth"])
    {
        let conf = if is_date(&c.data_type) { High } else { Medium };
        return Some((K::BirthDate, conf, says("a date of birth")));
    }
    if n.joined_any(&["age"]) && num {
        return Some((
            K::BirthDate,
            Low,
            says("an age, which with other columns can identify a person"),
        ));
    }
    if n.any(&["ip", "ipv4", "ipv6", "ipaddr", "ipaddress"])
        || n.pair("remote", &["addr", "address", "ip"])
        || n.pair("client", &["ip"])
    {
        return Some((K::IpAddress, Medium, says("an IP address")));
    }
    if n.any(&["imei", "idfa", "gaid", "macaddr", "macaddress"])
        || n.pair("mac", &["address", "addr"])
        || n.pair("device", &["id", "token", "fingerprint"])
        || n.pair("advertising", &["id"])
    {
        return Some((K::DeviceId, Medium, says("a device identifier")));
    }
    if text
        && (n.any(&[
            "address", "addr", "street", "postcode", "zipcode", "zip", "address1", "address2",
        ]) || n.pair("postal", &["code"])
            || n.pair("post", &["code"])
            || n.pair("house", &["number", "no"]))
    {
        return Some((K::Address, Medium, says("a postal address")));
    }
    if n.any(&["latitude", "longitude", "geolocation", "coordinates", "geo"])
        || n.joined_any(&["lat", "lng", "lon", "latlng", "latlon", "location"])
    {
        return Some((K::Location, Medium, says("a location")));
    }
    if text
        && (n.any(&[
            "firstname",
            "lastname",
            "fullname",
            "surname",
            "forename",
            "maidenname",
            "middlename",
        ]) || [
            "first", "last", "full", "given", "middle", "maiden", "family", "legal",
        ]
        .iter()
        .any(|w| n.pair(w, &["name"])))
    {
        return Some((K::PersonName, High, says("a person's name")));
    }
    if text
        && (n.joined_any(&["name", "displayname", "contactname", "username"])
            || n.pair("display", &["name"])
            || n.pair("contact", &["name"]))
    {
        let people = PEOPLE_TABLES.contains(&c.table.to_ascii_lowercase().as_str());
        if people || n.joined != "name" {
            let conf = if people { Medium } else { Low };
            return Some((K::PersonName, conf, says("a person's name")));
        }
    }
    if n.any(&[
        "gender",
        "sex",
        "ethnicity",
        "ethnic",
        "race",
        "religion",
        "diagnosis",
        "medical",
        "health",
        "disability",
        "allergy",
        "allergies",
        "medication",
        "pregnancy",
        "orientation",
        "political",
        "biometric",
        "fingerprint",
    ]) || n.pair("blood", &["type", "group"])
    {
        return Some((
            K::SpecialCategory,
            Medium,
            says(
                "health, sex, ethnicity, religion or similar, a special category of personal data",
            ),
        ));
    }
    if n.any(&["salary", "income", "wage", "wages", "payroll"])
        || n.pair("credit", &["score", "rating"])
    {
        return Some((
            K::Financial,
            Medium,
            says("a salary, income or credit score"),
        ));
    }
    if n.joined_any(&["city", "town"]) && text {
        return Some((K::Address, Low, says("a city, part of an address")));
    }
    if is_long_text(c)
        && n.any(&[
            "note",
            "notes",
            "comment",
            "comments",
            "remark",
            "remarks",
            "message",
            "messages",
            "body",
            "bio",
            "about",
            "description",
            "memo",
            "feedback",
        ])
    {
        return Some((
            K::FreeText,
            Low,
            "free text, which may hold personal details people type in".to_owned(),
        ));
    }
    None
}

/// What the column's type alone says.
fn by_type(c: &ColumnInfo) -> Option<(Category, Confidence, String)> {
    match c.data_type.as_str() {
        "inet4" | "inet6" => Some((
            Category::IpAddress,
            Confidence::High,
            format!("its type is {}", c.data_type),
        )),
        "point" | "geometry" | "multipoint" => Some((
            Category::Location,
            Confidence::Medium,
            format!("its type is {}", c.data_type),
        )),
        _ => None,
    }
}

/// What the column's comment says.
fn by_comment(c: &ColumnInfo) -> Option<(Option<Category>, Confidence, String)> {
    let m = c.comment.to_lowercase();
    if m.trim().is_empty() {
        return None;
    }
    let cat = [
        ("email", Category::Email),
        ("phone", Category::Phone),
        ("date of birth", Category::BirthDate),
        ("birth", Category::BirthDate),
        ("social security", Category::NationalId),
        ("passport", Category::NationalId),
        ("national id", Category::NationalId),
        ("card number", Category::CardNumber),
        ("iban", Category::BankAccount),
        ("address", Category::Address),
        ("password", Category::Secret),
        ("token", Category::Secret),
        ("ip address", Category::IpAddress),
    ]
    .into_iter()
    .find(|(w, _)| m.contains(w))
    .map(|(_, k)| k);
    let marked = [
        "pii",
        "personal data",
        "personally identifiable",
        "gdpr",
        "hipaa",
        "pci",
        "sensitive",
        "confidential",
        "personal",
    ]
    .into_iter()
    .find(|w| {
        m.split(|ch: char| !ch.is_alphanumeric() && ch != ' ')
            .any(|part| part.contains(w))
    });
    match (cat, marked) {
        (_, Some(w)) => Some((cat, Confidence::High, format!("its comment says {w:?}"))),
        (Some(k), None) => Some((
            Some(k),
            Confidence::Medium,
            format!("its comment mentions {}", k.as_str().replace('_', " ")),
        )),
        (None, None) => None,
    }
}

/// What a CHECK constraint on the column says.
fn by_checks(c: &ColumnInfo) -> Option<(Category, Confidence, String)> {
    let col = c.column.to_lowercase();
    for chk in &c.checks {
        let l = chk.to_lowercase();
        if !l.contains(&col) {
            continue;
        }
        if (l.contains(" like ") || l.contains("regexp") || l.contains("rlike")) && l.contains('@')
        {
            return Some((
                Category::Email,
                Confidence::High,
                "a CHECK constraint makes it look like an email address".to_owned(),
            ));
        }
    }
    None
}

/// The schema's verdict on one column.
#[must_use]
pub fn by_schema(c: &ColumnInfo) -> Option<Finding> {
    let toks = tokens(&c.column);
    let mut found: Option<(Category, Confidence, Vec<String>)> = None;
    let mut take = |k: Category, conf: Confidence, why: String| match &mut found {
        None => found = Some((k, conf, vec![why])),
        Some((k0, c0, rs)) if *k0 == k => {
            *c0 = (*c0).max(conf);
            rs.push(why);
        }
        Some((_, c0, _)) if conf > *c0 => found = Some((k, conf, vec![why])),
        Some(_) => {}
    };
    if let Some((k, conf, why)) = by_name(c, &toks) {
        take(k, conf, why);
    }
    if let Some((k, conf, why)) = by_type(c) {
        take(k, conf, why);
    }
    if let Some((k, conf, why)) = by_checks(c) {
        take(k, conf, why);
    }
    let comment = by_comment(c);
    let (k, conf, mut reasons) = match (found, comment) {
        (Some((k, conf, mut rs)), Some((ck, cconf, why))) => {
            if ck.is_none_or(|x| x == k) {
                rs.push(why);
                (k, conf.max(cconf), rs)
            } else {
                (k, conf, rs)
            }
        }
        (Some(f), None) => f,
        (None, Some((ck, cconf, why))) => (
            ck.unwrap_or(if is_long_text(c) {
                Category::FreeText
            } else {
                Category::SpecialCategory
            }),
            cconf,
            vec![why],
        ),
        (None, None) => return None,
    };
    reasons.dedup();
    Some(Finding {
        table: c.table.clone(),
        column: c.column.clone(),
        ty: c.column_type.clone(),
        category: k,
        confidence: conf,
        reasons,
        values: None,
    })
}

// ---- value patterns ----

fn looks_email(v: &str) -> bool {
    let v = v.trim();
    if v.len() > 254 || v.contains(char::is_whitespace) {
        return false;
    }
    let Some((local, domain)) = v.split_once('@') else {
        return false;
    };
    !local.is_empty()
        && !domain.contains('@')
        && domain.contains('.')
        && !domain.starts_with('.')
        && !domain.ends_with('.')
        && domain
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '-'))
}

fn digits(v: &str) -> String {
    v.chars().filter(char::is_ascii_digit).collect()
}

fn looks_phone(v: &str) -> bool {
    let v = v.trim();
    if v.is_empty()
        || !v
            .chars()
            .all(|c| c.is_ascii_digit() || matches!(c, '+' | ' ' | '-' | '(' | ')' | '.'))
    {
        return false;
    }
    let d = digits(v);
    if !(7..=15).contains(&d.len()) {
        return false;
    }
    // A date (2026-11-15, 2026.11.15) isn't a phone number.
    let b = v.as_bytes();
    if b.len() == 10 && b[4] == b[7] && matches!(b[4], b'-' | b'.') {
        return false;
    }
    // A bare number is an id or an amount far more often than a phone.
    v.starts_with('+')
        || v.contains([' ', '-', '(', ')', '.'])
        || (v.starts_with('0') && d.len() >= 9)
}

fn luhn(d: &str) -> bool {
    let mut sum = 0u32;
    for (i, c) in d.chars().rev().enumerate() {
        let mut n = c.to_digit(10).unwrap_or(0);
        if i % 2 == 1 {
            n *= 2;
            if n > 9 {
                n -= 9;
            }
        }
        sum += n;
    }
    sum.is_multiple_of(10)
}

fn looks_card(v: &str) -> bool {
    let v = v.trim();
    if !v
        .chars()
        .all(|c| c.is_ascii_digit() || matches!(c, ' ' | '-'))
    {
        return false;
    }
    let d = digits(v);
    (13..=19).contains(&d.len()) && matches!(d.as_bytes()[0], b'3'..=b'6') && luhn(&d)
}

fn looks_ssn(v: &str) -> bool {
    let b = v.trim().as_bytes();
    b.len() == 11
        && b[3] == b'-'
        && b[6] == b'-'
        && b.iter()
            .enumerate()
            .all(|(i, c)| i == 3 || i == 6 || c.is_ascii_digit())
}

fn looks_iban(v: &str) -> bool {
    let s: String = v.chars().filter(|c| !c.is_whitespace()).collect();
    let b = s.as_bytes();
    if !(15..=34).contains(&b.len())
        || !b[..2].iter().all(u8::is_ascii_uppercase)
        || !b[2..4].iter().all(u8::is_ascii_digit)
        || !b.iter().all(u8::is_ascii_alphanumeric)
    {
        return false;
    }
    // The check digits: the rearranged number mod 97 is 1.
    let mut rem = 0u32;
    for c in s[4..].chars().chain(s[..4].chars()) {
        let Some(n) = c.to_digit(36) else {
            return false;
        };
        rem = if n >= 10 {
            (rem * 100 + n) % 97
        } else {
            (rem * 10 + n) % 97
        };
    }
    rem == 1
}

fn looks_ip(v: &str) -> bool {
    let v = v.trim();
    v.contains(['.', ':']) && v.parse::<IpAddr>().is_ok()
}

fn looks_secret(v: &str) -> bool {
    let v = v.trim();
    let jwt = v.starts_with("eyJ")
        && v.split('.').count() == 3
        && v.chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '-' | '_'));
    jwt || v.starts_with("-----BEGIN")
        || [
            "sk-", "sk_live_", "pk_live_", "AKIA", "ghp_", "xoxb-", "xoxp-", "glpat-",
        ]
        .iter()
        .any(|p| v.starts_with(p) && v.len() >= 16 && !v.contains(' '))
}

/// The pattern one value matches, if any.
#[must_use]
pub fn value_kind(v: &str) -> Option<Category> {
    if looks_email(v) {
        Some(Category::Email)
    } else if looks_ssn(v) {
        Some(Category::NationalId)
    } else if looks_card(v) {
        Some(Category::CardNumber)
    } else if looks_iban(v) {
        Some(Category::BankAccount)
    } else if looks_ip(v) {
        Some(Category::IpAddress)
    } else if looks_secret(v) {
        Some(Category::Secret)
    } else if looks_phone(v) {
        Some(Category::Phone)
    } else {
        None
    }
}

/// The commonest pattern among `samples` (counts only), or `None` when
/// none was sampled. A sample without any match gives a zero count for
/// the pattern the schema expected, if any.
#[must_use]
pub fn by_values(samples: &[String], expected: Option<Category>) -> Option<ValueMatches> {
    if samples.is_empty() {
        return None;
    }
    let mut counts: BTreeMap<Category, u32> = BTreeMap::new();
    for s in samples {
        if let Some(k) = value_kind(s) {
            *counts.entry(k).or_default() += 1;
        }
    }
    let sampled = u32::try_from(samples.len()).unwrap_or(u32::MAX);
    let best = counts
        .iter()
        .max_by_key(|(k, n)| (**n, Some(**k) == expected));
    match (best, expected) {
        (Some((k, n)), _) => Some(ValueMatches {
            pattern: *k,
            matched: *n,
            sampled,
        }),
        (None, Some(k)) => Some(ValueMatches {
            pattern: k,
            matched: 0,
            sampled,
        }),
        (None, None) => None,
    }
}

/// Categories whose values a pattern recognises.
fn pattern_checked(k: Category) -> bool {
    matches!(
        k,
        Category::Email
            | Category::Phone
            | Category::CardNumber
            | Category::IpAddress
            | Category::BankAccount
    )
}

/// Merges a column's value counts into its schema finding (or makes
/// one): matches raise the confidence, a large sample with none lowers it.
#[must_use]
pub fn with_values(c: &ColumnInfo, f: Option<Finding>, m: Option<ValueMatches>) -> Option<Finding> {
    let Some(m) = m else { return f };
    let share = f64::from(m.matched) / f64::from(m.sampled.max(1));
    let what = m.pattern.as_str().replace('_', " ");
    match f {
        Some(mut f) if f.category == m.pattern => {
            if m.matched == 0 {
                if m.sampled >= 20 && pattern_checked(f.category) {
                    f.confidence = Confidence::Low;
                    f.reasons.push(format!(
                        "none of {} sampled values looks like a {what}",
                        m.sampled
                    ));
                }
            } else {
                if share >= 0.5 {
                    f.confidence = Confidence::High;
                }
                f.reasons.push(format!(
                    "{} of {} sampled values look like a {what}",
                    m.matched, m.sampled
                ));
            }
            f.values = Some(m);
            Some(f)
        }
        Some(mut f) => {
            if m.matched > 0 {
                f.reasons.push(format!(
                    "{} of {} sampled values look like a {what}",
                    m.matched, m.sampled
                ));
                if f.category == Category::FreeText {
                    f.confidence = f.confidence.max(Confidence::Medium);
                }
            }
            f.values = Some(m);
            Some(f)
        }
        None if m.matched > 0 && (share >= 0.3 || m.matched >= 5) => Some(Finding {
            table: c.table.clone(),
            column: c.column.clone(),
            ty: c.column_type.clone(),
            category: m.pattern,
            confidence: if share >= 0.8 {
                Confidence::High
            } else {
                Confidence::Medium
            },
            reasons: vec![format!(
                "{} of {} sampled values look like a {what}",
                m.matched, m.sampled
            )],
            values: Some(m),
        }),
        None => None,
    }
}

/// Whether a column's values are worth sampling: text that isn't huge.
#[must_use]
pub fn sample_worthy(c: &ColumnInfo) -> bool {
    matches!(
        c.data_type.as_str(),
        "char" | "varchar" | "tinytext" | "text" | "mediumtext" | "longtext"
    )
}

/// A table's row-level security, for the report.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TableSecurity {
    /// Row-level security is enabled.
    pub rls_enabled: bool,
    /// Its policies.
    pub policies: u32,
}

/// The tool's result: findings (strongest first) and tables, with no
/// value in it.
#[must_use]
pub fn report(
    db: &str,
    columns: usize,
    findings: &[Finding],
    security: &BTreeMap<String, TableSecurity>,
    sampled: Option<usize>,
) -> Value {
    let mut fs: Vec<&Finding> = findings.iter().collect();
    fs.sort_by(|a, b| {
        b.confidence
            .cmp(&a.confidence)
            .then_with(|| a.table.cmp(&b.table))
            .then_with(|| a.column.cmp(&b.column))
    });
    let mut tables: BTreeMap<&str, (u32, u32)> = BTreeMap::new();
    for f in &fs {
        let e = tables.entry(f.table.as_str()).or_default();
        if f.confidence >= Confidence::Medium {
            e.0 += 1;
        } else {
            e.1 += 1;
        }
    }
    let tables: Vec<Value> = tables
        .into_iter()
        .map(|(t, (strong, weak))| {
            let sec = security.get(t).cloned().unwrap_or_default();
            json!({
                "table": t, "flagged": strong, "low_confidence": weak,
                "row_level_security": if sec.rls_enabled { "on" } else { "off" },
                "policies": sec.policies,
            })
        })
        .collect();
    let mut cats: Vec<Category> = fs.iter().map(|f| f.category).collect();
    cats.sort();
    cats.dedup();
    let advice: Map = cats
        .into_iter()
        .map(|k| (k.as_str().to_owned(), json!(k.advice())))
        .collect();
    json!({
        "db": db,
        "columns_scanned": columns,
        "values": match sampled {
            Some(n) => json!(format!("data sharing is on: up to {SAMPLE_VALUES} values of each of {n} text columns were matched against patterns; only the counts are reported")),
            None => json!("data sharing is off: no value was read; findings come from names, types, comments and constraints"),
        },
        "findings": fs,
        "tables": tables,
        "advice": advice,
        "note": "SprigDB has no column masking: protect columns with column-level grants or a view, rows with row-level security; residency (where a tenant's data lives) is set by operators for the whole tenant.",
    })
}

type Map = serde_json::Map<String, Value>;

#[cfg(test)]
mod tests;

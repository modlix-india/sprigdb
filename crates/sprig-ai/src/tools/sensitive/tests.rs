use std::collections::BTreeMap;

use proptest::prelude::*;

use super::*;

fn col(table: &str, column: &str, column_type: &str) -> ColumnInfo {
    let data_type = column_type
        .split(['(', ' '])
        .next()
        .unwrap_or_default()
        .to_owned();
    ColumnInfo {
        table: table.into(),
        column: column.into(),
        data_type,
        column_type: column_type.into(),
        ..ColumnInfo::default()
    }
}

fn cat(c: &ColumnInfo) -> Option<(Category, Confidence)> {
    by_schema(c).map(|f| (f.category, f.confidence))
}

#[test]
fn personal_columns_are_flagged_by_name_and_type() {
    use Category as K;
    for (table, column, ty, want) in [
        ("customers", "email", "varchar(255)", K::Email),
        ("customers", "contactEmail", "varchar(255)", K::Email),
        ("customers", "e_mail", "varchar(120)", K::Email),
        ("customers", "phone", "varchar(32)", K::Phone),
        ("customers", "mobile_number", "varchar(20)", K::Phone),
        ("customers", "dob", "date", K::BirthDate),
        ("customers", "date_of_birth", "date", K::BirthDate),
        ("customers", "birthDate", "datetime", K::BirthDate),
        ("customers", "ssn", "char(11)", K::NationalId),
        ("customers", "national_id", "varchar(20)", K::NationalId),
        ("customers", "passport_no", "varchar(20)", K::NationalId),
        ("payments", "card_number", "varchar(19)", K::CardNumber),
        ("payments", "cvv", "char(3)", K::CardNumber),
        ("payments", "iban", "varchar(34)", K::BankAccount),
        ("customers", "street_address", "varchar(200)", K::Address),
        ("customers", "postal_code", "varchar(10)", K::Address),
        ("customers", "first_name", "varchar(80)", K::PersonName),
        ("customers", "surname", "varchar(80)", K::PersonName),
        ("customers", "name", "varchar(120)", K::PersonName),
        ("sessions", "client_ip", "varchar(45)", K::IpAddress),
        ("sessions", "addr", "inet6", K::IpAddress),
        ("users", "password_hash", "char(60)", K::Secret),
        ("users", "api_key", "varchar(64)", K::Secret),
        ("users", "refresh_token", "text", K::Secret),
        ("patients", "diagnosis", "text", K::SpecialCategory),
        ("employees", "salary", "decimal(10,2)", K::Financial),
        ("stores", "latitude", "double", K::Location),
        ("contacts", "notes", "text", K::FreeText),
    ] {
        let c = col(table, column, ty);
        assert_eq!(cat(&c).map(|x| x.0), Some(want), "{table}.{column} {ty}");
    }
    // Strong signals are high confidence.
    assert_eq!(
        cat(&col("t", "email", "varchar(255)")).unwrap().1,
        Confidence::High
    );
    assert_eq!(cat(&col("t", "dob", "date")).unwrap().1, Confidence::High);
    assert_eq!(
        cat(&col("t", "ssn", "char(11)")).unwrap().1,
        Confidence::High
    );
}

#[test]
fn ordinary_columns_are_not_flagged() {
    for (table, column, ty) in [
        ("orders", "id", "bigint"),
        ("orders", "status", "varchar(16)"),
        ("orders", "total", "decimal(10,2)"),
        ("orders", "created_at", "datetime"),
        ("orders", "quantity", "int"),
        ("orders", "customer_id", "bigint"),
        ("orders", "shipped_on", "date"),
        ("products", "name", "varchar(120)"),
        ("products", "sku", "varchar(32)"),
        ("products", "price", "decimal(10,2)"),
        ("users", "email_verified", "tinyint(1)"),
        ("users", "email_verified_at", "datetime"),
        ("users", "phone_type", "varchar(10)"),
        ("users", "address_count", "int"),
        ("users", "is_active", "tinyint(1)"),
        ("events", "kind", "varchar(32)"),
        ("events", "payload_version", "int"),
        ("invoices", "currency", "char(3)"),
        ("invoices", "country", "char(2)"),
        ("orders", "description", "varchar(80)"),
    ] {
        let c = col(table, column, ty);
        assert_eq!(cat(&c), None, "{table}.{column} {ty} was flagged");
    }
}

#[test]
fn comments_and_checks_flag_columns() {
    let mut c = col("t", "ref_a", "varchar(64)");
    c.comment = "Customer's personal data (GDPR)".into();
    assert_eq!(cat(&c).unwrap().1, Confidence::High);
    let mut c = col("t", "contact", "varchar(255)");
    c.checks = vec!["`contact` like '%@%.%'".into()];
    assert_eq!(cat(&c), Some((Category::Email, Confidence::High)));
    let mut c = col("t", "x1", "varchar(40)");
    c.comment = "the holder's phone".into();
    assert_eq!(cat(&c), Some((Category::Phone, Confidence::Medium)));
}

#[test]
fn value_patterns() {
    use Category as K;
    for (v, want) in [
        ("alice@example.com", Some(K::Email)),
        ("not an email@", None),
        ("+44 7700 900123", Some(K::Phone)),
        ("(202) 555-0100", Some(K::Phone)),
        ("12345678", None),
        ("4111 1111 1111 1111", Some(K::CardNumber)),
        ("4111 1111 1111 1112", None),
        ("123-45-6789", Some(K::NationalId)),
        ("GB82 WEST 1234 5698 7654 32", Some(K::BankAccount)),
        ("GB82 WEST 1234 5698 7654 33", None),
        ("192.168.0.1", Some(K::IpAddress)),
        ("2001:db8::1", Some(K::IpAddress)),
        ("1.5", None),
        (
            "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxIn0.c2lnbmF0dXJl",
            Some(K::Secret),
        ),
        ("sk-abcdefghijklmnop1234", Some(K::Secret)),
        ("shipped", None),
        ("2026-11-15", None),
    ] {
        assert_eq!(value_kind(v), want, "{v}");
    }
}

#[test]
fn values_raise_lower_or_add_findings() {
    let c = col("contacts", "phone", "varchar(32)");
    let none: Vec<String> = (0..40).map(|i| format!("n/a {i}")).collect();
    let f = with_values(&c, by_schema(&c), by_values(&none, Some(Category::Phone))).unwrap();
    assert_eq!(f.confidence, Confidence::Low);
    let some: Vec<String> = (0..40).map(|i| format!("+1 202 555 01{i:02}")).collect();
    let f = with_values(&c, by_schema(&c), by_values(&some, Some(Category::Phone))).unwrap();
    assert_eq!(f.confidence, Confidence::High);
    assert_eq!(f.values.unwrap().matched, 40);
    // A column the name doesn't flag, full of emails.
    let c = col("t", "ref", "varchar(200)");
    let mails: Vec<String> = (0..10).map(|i| format!("u{i}@example.org")).collect();
    let f = with_values(&c, by_schema(&c), by_values(&mails, None)).unwrap();
    assert_eq!(
        (f.category, f.confidence),
        (Category::Email, Confidence::High)
    );
}

fn report_of(columns: &[ColumnInfo], samples: &BTreeMap<String, Vec<String>>) -> String {
    let fs: Vec<Finding> = columns
        .iter()
        .filter_map(|c| {
            let f = by_schema(c);
            let m = samples
                .get(&c.column)
                .and_then(|s| by_values(s, f.as_ref().map(|f| f.category)));
            with_values(c, f, m)
        })
        .collect();
    report(
        "crm",
        columns.len(),
        &fs,
        &BTreeMap::new(),
        Some(samples.len()),
    )
    .to_string()
}

proptest! {
    // Whatever the sampled values are, the report holds counts and never a
    // value: it is all that reaches the provider.
    #[test]
    fn the_report_never_holds_a_sampled_value(
        values in proptest::collection::vec(
            prop_oneof![
                "[a-z]{4,10}@[a-z]{4,8}\\.(com|org)",
                "\\+[1-9][0-9]{2} [0-9]{3} [0-9]{4}",
                "zq[A-Za-z]{6,14}",
                "[0-9]{3}-[0-9]{2}-[0-9]{4}",
                "192\\.168\\.[0-9]{1,3}\\.[0-9]{1,3}",
            ],
            1..40,
        )
    ) {
        let columns = vec![
            col("contacts", "email", "varchar(255)"),
            col("contacts", "notes", "text"),
            col("contacts", "ref", "varchar(64)"),
        ];
        let mut samples = BTreeMap::new();
        for c in &columns {
            samples.insert(c.column.clone(), values.clone());
        }
        let r = report_of(&columns, &samples);
        for v in &values {
            prop_assert!(!r.contains(v.as_str()), "{v} reached the report");
        }
    }
}

#![allow(clippy::unwrap_used)]

use sprig_catalog::Migrator;

use super::*;

#[test]
fn enums_round_trip_their_sql_text() {
    for k in UserKind::ALL {
        assert_eq!(UserKind::parse(k.as_str()).unwrap(), *k);
    }
    for k in VerifierKind::ALL {
        assert_eq!(VerifierKind::parse(k.as_str()).unwrap(), *k);
    }
    for s in Slot::ALL {
        assert_eq!(Slot::parse(s.as_str()).unwrap(), *s);
    }
    for s in UserSource::ALL {
        assert_eq!(UserSource::parse(s.as_str()).unwrap(), *s);
    }
    assert!(RequireTls::parse("X509").is_err());
    // Slots order as the cache tries them: primary, secondary, history.
    assert!(Slot::Primary < Slot::Secondary && Slot::Secondary < Slot::History);
}

#[test]
fn auth_methods_are_a_set() {
    let m = AuthMethods::parse("password,api_key").unwrap();
    assert!(m.contains(AuthMethods::PASSWORD) && m.contains(AuthMethods::API_KEY));
    assert!(!m.contains(AuthMethods::TOKEN));
    assert_eq!(m.to_sql(), "password,api_key");
    assert_eq!(AuthMethods::parse("").unwrap(), AuthMethods::empty());
    assert!(AuthMethods::parse("password,bogus").is_err());
    assert_eq!(AuthMethods::default(), AuthMethods::PASSWORD);
}

#[test]
fn times_round_trip_as_micros() {
    let t = UNIX_EPOCH + Duration::from_micros(1_762_560_000_123_456);
    assert_eq!(from_micros(micros(t)), t);
    assert_eq!(micros(UNIX_EPOCH - Duration::from_secs(5)), 0);
    // 9999-12-31 00:00:00 UTC.
    assert_eq!(UNBOUNDED_LOCK_MICROS / 1_000_000, 253_402_214_400);
}

#[test]
fn the_module_has_no_migrations() {
    let m = Migrator::new(&[AUTH_MODULE]).unwrap();
    // Not a format readers check: its rows are engine records.
    assert_eq!(m.supports().get(MODULE), None);
    assert_eq!(AUTH_MODULE.newest(), 0);
}

#[test]
fn verifier_debug_hides_secrets() {
    let v = VerifierRow::mysql(
        UserId(1),
        VerifierKind::Native,
        Slot::Primary,
        b"*0123456789ABCDEF0123456789ABCDEF01234567".to_vec(),
        1,
        UNIX_EPOCH,
    );
    let d = format!("{v:?}");
    assert!(!d.contains("0123456789"), "{d}");
}

#[test]
fn rows_round_trip_through_their_records() {
    let mut u = UserRow::new(
        UserId(7),
        "zoë",
        "10.%",
        UserSource::Sql,
        UNIX_EPOCH + Duration::from_nanos(1_762_560_000_123_456_789),
        3,
    );
    u.auth_methods = AuthMethods::PASSWORD.union(AuthMethods::API_KEY);
    u.attributes = Some(serde_json::json!({"team": "a"}));
    u.require_subject = Some("/CN=x".into());
    let back: UserRow = serde_json::from_slice(&serde_json::to_vec(&u).unwrap()).unwrap();
    // Times keep microseconds, as DATETIME(6) did.
    assert_eq!(
        back.created_at,
        UNIX_EPOCH + Duration::from_micros(1_762_560_000_123_456)
    );
    let mut want = u.clone();
    want.created_at = back.created_at;
    assert_eq!(back, want);
    assert!(serde_json::from_str::<UserKind>("\"robot\"").is_err());
}

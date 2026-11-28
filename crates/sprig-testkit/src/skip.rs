//! Tests whose environment isn't configured.
//!
//! Live tests need real services: a running SprigDB stack, a reference
//! server, etcd, MariaDB in some topology, Docker. Each one names the
//! variable that points at its service and is marked
//! `#[ignore = "live: needs <VAR>; tools/live/README.md"]`, so a plain
//! `cargo test` or `cargo nextest run` lists it as ignored (skipped) instead
//! of passing it, and `tools/live/run.sh` (or `--run-ignored`) runs it. A
//! live test run without its variable fails: [`require_env!`] and
//! [`report_missing`] panic. The variables hold connection URLs; a variable
//! naming several servers holds a comma-separated list, writer first (see
//! [`urls`]).
//!
//! | Variable | Holds | Example |
//! |---|---|---|
//! | [`ETCD`] | etcd client URLs | `http://127.0.0.1:2379` |
//! | [`MARIADB_SINGLE`] | one MariaDB URL | `mysql://root:sprig-dev-root@127.0.0.1:13301` |
//! | [`MARIADB_REPLICATION`] | primary, then replicas | `mysql://…@127.0.0.1:13311,mysql://…@127.0.0.1:13312` |
//! | [`MARIADB_GALERA`] | the Galera nodes | `mysql://…@127.0.0.1:13321,…` |
//! | [`DOCKER`] | `1` when tests may run `docker compose` | `1` |
//! | [`ENV_FILE`] | a TOML [`Env`](crate::Env) file | `deploy/compose/env.3dc.toml` |
//! | [`TOXIPROXY`] | toxiproxy's API (profile `faults`) | `http://127.0.0.1:18474` |
//! | [`TOXIPROXY_UPSTREAM`] | the single node as toxiproxy reaches it | `single:3306` (default) |
//! | [`FAULTD`] | the faultd container (profile `faults`) | `sprigdb-dev-faultd-1` |
//! | [`ELLE`] | how to run elle-cli: `1` (image), an image, or a `.jar` | `1` |
//!
//! Rust has no runtime "skipped" status: a test that returned early would
//! report as passed, so a green run would say nothing about the suites that
//! didn't run. That is why the skip is the `#[ignore]` attribute, which the
//! test runners count, and why a missing variable is a failure. [`STRICT`]
//! is still read by a few older tests and fixtures; these helpers fail
//! whether it is set or not.

/// etcd client URLs (single-etcd mode).
pub const ETCD: &str = "SPRIG_TEST_ETCD";
/// One single-node MariaDB.
pub const MARIADB_SINGLE: &str = "SPRIG_TEST_MARIADB_SINGLE";
/// A primary with replicas: the primary's URL first.
pub const MARIADB_REPLICATION: &str = "SPRIG_TEST_MARIADB_REPLICATION";
/// A Galera cluster: one URL per node.
pub const MARIADB_GALERA: &str = "SPRIG_TEST_MARIADB_GALERA";
/// Set to `1` to allow tests that drive `docker compose` themselves.
pub const DOCKER: &str = "SPRIG_TEST_DOCKER";
/// Path of a TOML file describing the whole environment ([`crate::Env::load`]).
pub const ENV_FILE: &str = "SPRIG_TEST_ENV_FILE";
/// toxiproxy's API URL.
pub const TOXIPROXY: &str = "SPRIG_TEST_TOXIPROXY";
/// The single MariaDB node's address as toxiproxy sees it, inside its
/// Docker network; `single:3306` when unset.
pub const TOXIPROXY_UPSTREAM: &str = "SPRIG_TEST_TOXIPROXY_UPSTREAM";
/// The faultd sidecar's container name.
pub const FAULTD: &str = "SPRIG_TEST_FAULTD";
/// How to run elle-cli ([`crate::faults::elle::ElleCli::from_env`]).
pub const ELLE: &str = "SPRIG_TEST_ELLE";
/// `1` in CI's live stages. The helpers here fail on a missing variable
/// whatever its value; some older tests and fixtures still read it.
pub const STRICT: &str = "SPRIG_TEST_STRICT";
/// Set to `1` to keep Compose projects running after a test, for debugging.
pub const KEEP: &str = "SPRIG_TEST_KEEP";
/// Directory where Compose fixtures write logs of failed tests.
pub const LOG_DIR: &str = "SPRIG_TEST_LOG_DIR";

/// The value of `name`, trimmed, or `None` when it is unset or empty.
#[must_use]
pub fn var(name: &str) -> Option<String> {
    std::env::var(name)
        .ok()
        .map(|v| v.trim().to_owned())
        .filter(|v| !v.is_empty())
}

/// Whether `name` is set to a true value (`1`, `true`, `yes` or `on`).
#[must_use]
pub fn flag(name: &str) -> bool {
    var(name).is_some_and(|v| is_true(&v))
}

/// Whether strict mode is on ([`STRICT`]).
#[must_use]
pub fn strict() -> bool {
    flag(STRICT)
}

/// Splits a variable's value into URLs: comma-separated, whitespace
/// trimmed, empty items dropped.
#[must_use]
pub fn urls(value: &str) -> Vec<String> {
    value
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_owned)
        .collect()
}

/// Fails a live test that runs without `name`; called by
/// [`require_env!`](crate::require_env).
///
/// A live test is `#[ignore]`d, so it runs only when asked for
/// (`--ignored`, `--run-ignored`, `tools/live/run.sh`); asked for without
/// its environment, it fails instead of passing having done nothing.
///
/// # Panics
/// Always.
pub fn report_missing(name: &str, test: &str) {
    panic!(
        "{test}: {name} is not set; a live test needs its environment (see tools/live/README.md)"
    );
}

fn is_true(v: &str) -> bool {
    matches!(v.to_ascii_lowercase().as_str(), "1" | "true" | "yes" | "on")
}

/// Evaluates to the value of an environment variable, or fails the test.
///
/// When the variable is unset or empty it panics ([`report_missing`]). The
/// second argument, the value an early return would have used, is still
/// accepted so older call sites compile; it is never returned. Mark the test
/// `#[ignore = "live: needs <VAR>; tools/live/README.md"]`.
///
/// ```no_run
/// use sprig_testkit::{require_env, skip};
///
/// #[test]
/// #[ignore = "live: needs SPRIG_TEST_MARIADB_GALERA; tools/live/README.md"]
/// fn talks_to_galera() -> Result<(), String> {
///     let urls = skip::urls(&require_env!(skip::MARIADB_GALERA, Ok(())));
///     assert_eq!(urls.len(), 3);
///     Ok(())
/// }
/// ```
#[macro_export]
macro_rules! require_env {
    ($name:expr) => {
        $crate::require_env!($name, ())
    };
    ($name:expr, $ret:expr) => {
        match $crate::skip::var($name) {
            ::std::option::Option::Some(v) => v,
            ::std::option::Option::None => {
                $crate::skip::report_missing($name, ::std::module_path!());
                return $ret;
            }
        }
    };
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn urls_split_and_trim() {
        assert_eq!(
            urls(" mysql://a:1 , ,mysql://b:2,"),
            vec!["mysql://a:1".to_owned(), "mysql://b:2".to_owned()]
        );
        assert!(urls("").is_empty());
    }

    #[test]
    fn truthy_values() {
        for v in ["1", "true", "YES", "on"] {
            assert!(is_true(v), "{v}");
        }
        for v in ["0", "false", "no", "off", "2"] {
            assert!(!is_true(v), "{v}");
        }
    }

    fn gated() -> Option<String> {
        let v = require_env!("SPRIG_TEST_SURELY_UNSET_VARIABLE_FOR_TESTKIT", None);
        Some(v)
    }

    #[test]
    #[should_panic(expected = "SPRIG_TEST_SURELY_UNSET_VARIABLE_FOR_TESTKIT is not set")]
    fn require_env_fails_when_unset() {
        let _ = gated();
    }
}

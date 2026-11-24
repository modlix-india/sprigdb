//! Where backups go: a [`BackupTarget`] stores whole objects by name.
//!
//! - [`LocalDir`]: a directory (the default for development). A put
//!   writes a temporary file, syncs it and renames it into place, so an
//!   object is either whole or absent.
//! - [`crate::s3::S3Target`]: S3-compatible object storage (AWS S3, OCI
//!   Object Storage's S3 compatibility API, `MinIO`).
//!
//! [`open_location`] reads `backup.location`: a path or `file:///path`,
//! or `s3://bucket/prefix?endpoint=https://host&region=r`.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use sprig_common::{Result, SprigError};

/// Object storage for backups. Names use `/` as a separator.
pub trait BackupTarget: Send + Sync {
    /// Stores `bytes` as `name`, replacing what was there. After it
    /// returns, the object is durable and whole.
    ///
    /// # Errors
    /// The storage failed.
    fn put(&self, name: &str, bytes: &[u8]) -> Result<()>;

    /// The object `name`, or `None`.
    ///
    /// # Errors
    /// The storage failed.
    fn get(&self, name: &str) -> Result<Option<Vec<u8>>>;

    /// Every name starting with `prefix`, sorted.
    ///
    /// # Errors
    /// The storage failed.
    fn list(&self, prefix: &str) -> Result<Vec<String>>;

    /// Removes `name` (nothing if it's absent).
    ///
    /// # Errors
    /// The storage failed.
    fn delete(&self, name: &str) -> Result<()>;

    /// Where it is, for messages.
    fn describe(&self) -> String;
}

pub(crate) fn err(what: &str, e: impl std::fmt::Display) -> SprigError {
    SprigError::internal(format!("backup storage: {what}: {e}"))
}

/// Objects as files under a directory.
#[derive(Debug, Clone)]
pub struct LocalDir {
    root: PathBuf,
}

impl LocalDir {
    /// A target at `root` (created on the first put).
    #[must_use]
    pub fn new(root: impl Into<PathBuf>) -> LocalDir {
        LocalDir { root: root.into() }
    }

    fn path(&self, name: &str) -> Result<PathBuf> {
        if name.is_empty()
            || name.starts_with('/')
            || name
                .split('/')
                .any(|s| s.is_empty() || s == "." || s == "..")
        {
            return Err(err("object name", format!("'{name}' is not allowed")));
        }
        Ok(self.root.join(name))
    }
}

fn sync_dir(d: &Path) {
    if let Ok(f) = std::fs::File::open(d) {
        let _ = f.sync_all();
    }
}

impl BackupTarget for LocalDir {
    fn put(&self, name: &str, bytes: &[u8]) -> Result<()> {
        use std::io::Write as _;
        static SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let p = self.path(name)?;
        let dir = p.parent().unwrap_or(&self.root).to_path_buf();
        std::fs::create_dir_all(&dir).map_err(|e| err(&format!("create {}", dir.display()), e))?;
        // A name of its own per put, so two puts of one object never
        // share a temporary file.
        let tmp = dir.join(format!(
            ".{}.{}.{}.tmp",
            p.file_name().and_then(|n| n.to_str()).unwrap_or("o"),
            std::process::id(),
            SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        ));
        let mut f = std::fs::File::create(&tmp)
            .map_err(|e| err(&format!("create {}", tmp.display()), e))?;
        f.write_all(bytes)
            .and_then(|()| f.sync_all())
            .map_err(|e| err(&format!("write {}", tmp.display()), e))?;
        std::fs::rename(&tmp, &p).map_err(|e| err(&format!("rename {}", p.display()), e))?;
        sync_dir(&dir);
        Ok(())
    }

    fn get(&self, name: &str) -> Result<Option<Vec<u8>>> {
        match std::fs::read(self.path(name)?) {
            Ok(b) => Ok(Some(b)),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(err(&format!("read {name}"), e)),
        }
    }

    fn list(&self, prefix: &str) -> Result<Vec<String>> {
        // Walk the directory the prefix's last '/' names.
        let (dir, _) = prefix.rsplit_once('/').unwrap_or(("", prefix));
        let start = if dir.is_empty() {
            self.root.clone()
        } else {
            self.root.join(dir)
        };
        let mut out = Vec::new();
        let mut stack = vec![(start, dir.to_owned())];
        while let Some((d, rel)) = stack.pop() {
            let rd = match std::fs::read_dir(&d) {
                Ok(r) => r,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
                Err(e) => return Err(err(&format!("list {}", d.display()), e)),
            };
            for ent in rd {
                let ent = ent.map_err(|e| err("list", e))?;
                let n = ent.file_name().to_string_lossy().into_owned();
                if n.starts_with('.') {
                    continue;
                }
                let full = if rel.is_empty() {
                    n.clone()
                } else {
                    format!("{rel}/{n}")
                };
                let ft = ent.file_type().map_err(|e| err("list", e))?;
                if ft.is_dir() {
                    if full.starts_with(prefix) || prefix.starts_with(&format!("{full}/")) {
                        stack.push((ent.path(), full));
                    }
                } else if full.starts_with(prefix) {
                    out.push(full);
                }
            }
        }
        out.sort();
        Ok(out)
    }

    fn delete(&self, name: &str) -> Result<()> {
        match std::fs::remove_file(self.path(name)?) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(err(&format!("delete {name}"), e)),
        }
    }

    fn describe(&self) -> String {
        format!("directory {}", self.root.display())
    }
}

/// The target `backup.location` names: a path (or `file:///path`), or
/// `s3://bucket[/prefix]?endpoint=URL[&region=R]` with the credentials in
/// `SPRIGDB_BACKUP_S3_ACCESS_KEY` and `SPRIGDB_BACKUP_S3_SECRET_KEY` (or a
/// `credentials=/path` file of `access_key = ...` and `secret_key = ...`
/// lines).
///
/// # Errors
/// The location can't be parsed, or credentials are missing.
pub fn open_location(location: &str) -> Result<Arc<dyn BackupTarget>> {
    let loc = location.trim();
    if loc.is_empty() {
        return Err(err("backup.location", "is empty"));
    }
    if let Some(rest) = loc.strip_prefix("s3://") {
        return Ok(Arc::new(crate::s3::S3Target::from_url(rest)?));
    }
    let path = loc.strip_prefix("file://").unwrap_or(loc);
    if !path.starts_with('/') {
        return Err(err(
            "backup.location",
            format!("'{loc}' must be an absolute path, file:///path or s3://bucket"),
        ));
    }
    Ok(Arc::new(LocalDir::new(path)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn local_dir_round_trip() {
        let d = std::env::temp_dir().join(format!("sprig-bk-local-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&d);
        let t = LocalDir::new(&d);
        t.put("t1/backups/a.json", b"1").unwrap();
        t.put("t1/chunks/x", b"2").unwrap();
        t.put("t2/chunks/y", b"3").unwrap();
        assert_eq!(t.get("t1/chunks/x").unwrap().unwrap(), b"2");
        assert!(t.get("t1/chunks/z").unwrap().is_none());
        assert_eq!(
            t.list("t1/").unwrap(),
            vec!["t1/backups/a.json", "t1/chunks/x"]
        );
        assert_eq!(t.list("t1/chunks/").unwrap(), vec!["t1/chunks/x"]);
        assert_eq!(t.list("t").unwrap().len(), 3);
        t.delete("t1/chunks/x").unwrap();
        t.delete("t1/chunks/x").unwrap();
        assert!(t.get("../etc").is_err());
        assert_eq!(t.list("t1/").unwrap(), vec!["t1/backups/a.json"]);
        let _ = std::fs::remove_dir_all(&d);
    }
}

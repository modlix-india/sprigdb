//! The process's one copy of the local model, shared by everything that
//! embeds with it (embedded columns, `AI_EMBED`, docs search).
//!
//! [`shared`] loads the model from a directory the first time it is asked
//! for, and hands out the same [`Embedder`] afterwards. A directory whose
//! model failed to load is tried again only after [`RETRY`], so a missing
//! model costs one failed read a minute, not one per call. Loading reads
//! files and takes about a second: call it from a blocking context
//! (`tokio::task::spawn_blocking`), as the embedding itself.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock, PoisonError};
use std::time::{Duration, Instant};

use crate::Embedder;

/// The alias of the bundled model in `ai.embedding_models` and in
/// `USING MODEL '...'`.
pub const ALIAS: &str = "local:bge-small";

/// How long a directory whose model didn't load is left alone.
pub const RETRY: Duration = Duration::from_secs(60);

enum Slot {
    Loaded(Arc<Embedder>),
    Failed { at: Instant, why: String },
}

fn slots() -> &'static Mutex<HashMap<PathBuf, Slot>> {
    static SLOTS: OnceLock<Mutex<HashMap<PathBuf, Slot>>> = OnceLock::new();
    SLOTS.get_or_init(|| Mutex::new(HashMap::new()))
}

/// The model in `dir`, loaded once per process (blocking).
///
/// # Errors
///
/// Why the model isn't available: the load's error, remembered for
/// [`RETRY`].
pub fn shared(dir: &Path) -> Result<Arc<Embedder>, String> {
    {
        let m = slots().lock().unwrap_or_else(PoisonError::into_inner);
        match m.get(dir) {
            Some(Slot::Loaded(e)) => return Ok(Arc::clone(e)),
            Some(Slot::Failed { at, why }) if at.elapsed() < RETRY => return Err(why.clone()),
            _ => {}
        }
    }
    // Loaded outside the lock: two first callers may both load, and the
    // second copy is dropped.
    let r = Embedder::load(dir).map_err(|e| format!("{}: {e}", dir.display()));
    let mut m = slots().lock().unwrap_or_else(PoisonError::into_inner);
    match r {
        Ok(e) => {
            if let Some(Slot::Loaded(prev)) = m.get(dir) {
                return Ok(Arc::clone(prev));
            }
            let e = Arc::new(e);
            m.insert(dir.to_path_buf(), Slot::Loaded(Arc::clone(&e)));
            Ok(e)
        }
        Err(why) => {
            m.insert(
                dir.to_path_buf(),
                Slot::Failed {
                    at: Instant::now(),
                    why: why.clone(),
                },
            );
            Err(why)
        }
    }
}

/// Whether the model in `dir` is loaded in this process.
#[must_use]
pub fn is_loaded(dir: &Path) -> bool {
    matches!(
        slots()
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(dir),
        Some(Slot::Loaded(_))
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_missing_model_fails_and_is_remembered() {
        let dir = std::env::temp_dir().join("sprig-embed-no-model-here");
        let e = shared(&dir).unwrap_err();
        assert!(e.contains("config.json"), "{e}");
        assert!(!is_loaded(&dir));
        // The second call answers from the remembered failure.
        assert_eq!(shared(&dir).unwrap_err(), e);
    }
}

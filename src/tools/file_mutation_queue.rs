use super::ToolError;
use crate::cancel::CancelToken;
use crate::env::{ExecutionEnv, FileError, FileErrorCode};
use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard, OnceLock, PoisonError};

type Slot = Arc<Mutex<()>>;

fn slots() -> MutexGuard<'static, HashMap<String, Slot>> {
    static SLOTS: OnceLock<Mutex<HashMap<String, Slot>>> = OnceLock::new();
    SLOTS
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
}

fn canonical(
    env: &dyn ExecutionEnv,
    absolute: &str,
    cancel: &CancelToken,
) -> Result<String, FileError> {
    match env.canonical_path(absolute, cancel) {
        Ok(path) => Ok(path),
        Err(error) if error.code == FileErrorCode::NotSupported => Ok(absolute.to_owned()),
        Err(error) if error.code != FileErrorCode::NotFound => Err(error),
        Err(_) => {
            let parent = env.join_path(&[absolute, ".."], cancel)?;
            if parent == absolute || !absolute.starts_with(&parent) {
                return Ok(absolute.to_owned());
            }
            let separator = usize::from(!(parent.ends_with('/') || parent.ends_with('\\')));
            let name = &absolute[parent.len() + separator..];
            let canonical_parent = canonical(env, &parent, cancel)?;
            env.join_path(&[&canonical_parent, name], cancel)
        }
    }
}

fn mutation_key(
    env: &dyn ExecutionEnv,
    path: &str,
    cancel: &CancelToken,
) -> Result<String, FileError> {
    let absolute = env.absolute_path(path, cancel)?;
    Ok(format!(
        "{}\0{}",
        env.id(),
        canonical(env, &absolute, cancel)?
    ))
}

pub fn with_file_mutation_queue<T>(
    env: &dyn ExecutionEnv,
    path: &str,
    cancel: &CancelToken,
    run: impl FnOnce() -> Result<T, ToolError>,
) -> Result<T, ToolError> {
    let key = mutation_key(env, path, cancel)?;
    let slot = slots()
        .entry(key.clone())
        .or_insert_with(|| Arc::new(Mutex::new(())))
        .clone();
    let result = {
        let _turn = slot.lock().unwrap_or_else(PoisonError::into_inner);
        run()
    };
    let mut map = slots();
    if map
        .get(&key)
        .is_some_and(|current| Arc::strong_count(current) == 2)
    {
        map.remove(&key);
    }
    drop(slot);
    result
}

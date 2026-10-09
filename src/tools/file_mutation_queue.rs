use super::ToolError;
use crate::cancel::CancelToken;
use crate::env::{ExecutionEnv, FileError, FileErrorCode};
use std::collections::HashMap;
use std::sync::{Arc, Condvar, Mutex, MutexGuard, OnceLock, PoisonError};

#[derive(Default)]
struct Turns {
    next: u64,
    serving: u64,
}

#[derive(Default)]
struct Queue {
    turns: Mutex<Turns>,
    turn: Condvar,
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

fn queues() -> MutexGuard<'static, HashMap<String, Arc<Queue>>> {
    static QUEUES: OnceLock<Mutex<HashMap<String, Arc<Queue>>>> = OnceLock::new();
    lock(QUEUES.get_or_init(|| Mutex::new(HashMap::new())))
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
    let (queue, ticket) = {
        let mut map = queues();
        let queue = map.entry(key.clone()).or_default().clone();
        let ticket = {
            let mut turns = lock(&queue.turns);
            let ticket = turns.next;
            turns.next += 1;
            ticket
        };
        (queue, ticket)
    };
    {
        let mut turns = lock(&queue.turns);
        while turns.serving != ticket {
            turns = queue
                .turn
                .wait(turns)
                .unwrap_or_else(PoisonError::into_inner);
        }
    }
    let result = run();
    {
        let mut turns = lock(&queue.turns);
        turns.serving += 1;
        queue.turn.notify_all();
    }
    let mut map = queues();
    let turns = lock(&queue.turns);
    if turns.serving == turns.next
        && map
            .get(&key)
            .is_some_and(|current| Arc::ptr_eq(current, &queue))
    {
        map.remove(&key);
    }
    result
}

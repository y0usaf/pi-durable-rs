use super::path;
use super::{FileError, FileErrorCode, FileKind, FileWatcher, WatchChange, WatchTarget};
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io;
use std::sync::mpsc::{self, RecvTimeoutError};
use std::thread;
use std::time::{Duration, SystemTime};

#[derive(Debug, Clone, Copy)]
pub struct WatchOptions {
    pub poll_interval: Duration,
    pub max_directories: usize,
}

impl Default for WatchOptions {
    fn default() -> Self {
        Self {
            poll_interval: Duration::from_millis(2000),
            max_directories: 10_000,
        }
    }
}

#[derive(PartialEq)]
struct Stamp {
    kind: FileKind,
    size: u64,
    modified: Option<SystemTime>,
}

type Snapshot = BTreeMap<String, Stamp>;

pub struct PollingWatcher {
    stop: Option<mpsc::Sender<()>>,
    handle: Option<thread::JoinHandle<()>>,
}

impl FileWatcher for PollingWatcher {
    fn mode(&self) -> &'static str {
        "polling"
    }

    fn close(mut self: Box<Self>) {
        self.stop.take();
        if let Some(handle) = self.handle.take() {
            handle.join().ok();
        }
    }
}

pub fn open(
    targets: &[WatchTarget],
    options: WatchOptions,
    mut on_change: Box<dyn FnMut(WatchChange) + Send>,
) -> Result<Box<dyn FileWatcher>, FileError> {
    let targets = targets.to_vec();
    let mut previous = snapshot(&targets, options.max_directories)?;
    let (stop, stopped) = mpsc::channel::<()>();
    let handle = thread::spawn(move || {
        loop {
            match stopped.recv_timeout(options.poll_interval) {
                Err(RecvTimeoutError::Timeout) => {}
                _ => return,
            }
            match snapshot(&targets, options.max_directories) {
                Err(error) => {
                    on_change(WatchChange::Error(error));
                    return;
                }
                Ok(current) => {
                    let changed = changed_paths(&previous, &current);
                    previous = current;
                    if !changed.is_empty() {
                        on_change(WatchChange::Paths(changed));
                    }
                }
            }
        }
    });
    Ok(Box::new(PollingWatcher {
        stop: Some(stop),
        handle: Some(handle),
    }))
}

fn fail(path: &str, error: &io::Error) -> FileError {
    let code = match error.kind() {
        io::ErrorKind::PermissionDenied => FileErrorCode::PermissionDenied,
        _ => FileErrorCode::Invalid,
    };
    FileError::new(code, error.to_string(), Some(path.to_owned()))
}

fn snapshot(targets: &[WatchTarget], max_directories: usize) -> Result<Snapshot, FileError> {
    let mut snap = BTreeMap::new();
    let mut directories = 0;
    for target in targets {
        match fs::symlink_metadata(&target.path) {
            Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
            Err(error) => return Err(fail(&target.path, &error)),
            Ok(meta) => {
                record(&mut snap, &target.path, &meta);
                if meta.is_dir() {
                    walk(
                        &mut snap,
                        &mut directories,
                        &target.path,
                        target,
                        max_directories,
                    )?;
                }
            }
        }
    }
    Ok(snap)
}

fn walk(
    snap: &mut Snapshot,
    directories: &mut usize,
    dir: &str,
    target: &WatchTarget,
    max_directories: usize,
) -> Result<(), FileError> {
    *directories += 1;
    if *directories > max_directories {
        return Err(FileError::new(
            FileErrorCode::Invalid,
            format!("Watched paths exceed {max_directories} directories"),
            None,
        ));
    }
    let entries = match fs::read_dir(dir) {
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(fail(dir, &error)),
        Ok(entries) => entries,
    };
    for entry in entries {
        let entry = entry.map_err(|error| fail(dir, &error))?;
        let name = entry.file_name().to_string_lossy().into_owned();
        if excluded(target, &name) {
            continue;
        }
        let entry_path = path::child(dir, &name);
        let meta = match fs::symlink_metadata(&entry_path) {
            Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
            Err(error) => return Err(fail(&entry_path, &error)),
            Ok(meta) => meta,
        };
        record(snap, &entry_path, &meta);
        if target.recursive && meta.is_dir() {
            walk(snap, directories, &entry_path, target, max_directories)?;
        }
    }
    Ok(())
}

fn excluded(target: &WatchTarget, name: &str) -> bool {
    (target.hidden && name.starts_with('.')) || target.names.iter().any(|excluded| excluded == name)
}

fn record(snap: &mut Snapshot, path: &str, meta: &fs::Metadata) {
    let kind = if meta.is_file() {
        FileKind::File
    } else if meta.is_dir() {
        FileKind::Directory
    } else if meta.file_type().is_symlink() {
        FileKind::Symlink
    } else {
        return;
    };
    let stamp = if kind == FileKind::Directory {
        Stamp {
            kind,
            size: 0,
            modified: None,
        }
    } else {
        Stamp {
            kind,
            size: meta.len(),
            modified: meta.modified().ok(),
        }
    };
    snap.insert(path.to_owned(), stamp);
}

fn changed_paths(previous: &Snapshot, current: &Snapshot) -> Vec<String> {
    let mut changed = BTreeSet::new();
    for (path, stamp) in current {
        if previous.get(path) != Some(stamp) {
            changed.insert(path.clone());
        }
    }
    for path in previous.keys() {
        if !current.contains_key(path) {
            changed.insert(path.clone());
        }
    }
    changed.into_iter().collect()
}

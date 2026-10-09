use std::sync::{Condvar, Mutex, MutexGuard, PoisonError, mpsc};
use std::time::{Duration, Instant};

use crate::errors::{Error, Result};

pub const PROGRESS_BYTES_PER_SECOND: u64 = 100 * 1024;

pub type Waiter = mpsc::Sender<std::result::Result<(), String>>;
pub type Settled = mpsc::Receiver<std::result::Result<(), String>>;

#[derive(Default)]
struct State {
    dirty: bool,
    stopped: bool,
    in_flight: bool,
    next_at: Option<Instant>,
    waiters: Vec<Waiter>,
}

pub struct Progress<'a> {
    write: &'a (dyn Fn() -> Result<usize> + Sync),
    on_error: &'a (dyn Fn(Error) + Sync),
    min_interval: Duration,
    state: Mutex<State>,
    wake: Condvar,
}

struct Stop<'b, 'a>(&'b Progress<'a>);

impl Drop for Stop<'_, '_> {
    fn drop(&mut self) {
        self.0.lock().stopped = true;
        self.0.wake.notify_all();
    }
}

impl<'a> Progress<'a> {
    pub fn new(
        write: &'a (dyn Fn() -> Result<usize> + Sync),
        on_error: &'a (dyn Fn(Error) + Sync),
        min_interval: Duration,
    ) -> Self {
        Self {
            write,
            on_error,
            min_interval,
            state: Mutex::new(State::default()),
            wake: Condvar::new(),
        }
    }

    pub fn mark(&self) {
        self.lock().dirty = true;
        self.wake.notify_all();
    }

    pub fn mark_and_wait(&self) -> Settled {
        let (sender, receiver) = mpsc::channel();
        {
            let mut state = self.lock();
            state.waiters.push(sender);
            state.dirty = true;
        }
        self.wake.notify_all();
        receiver
    }

    pub fn run<T>(&self, body: impl FnOnce() -> T) -> (T, Vec<Waiter>) {
        let out = std::thread::scope(|scope| {
            scope.spawn(|| self.commit_loop());
            let _stop = Stop(self);
            body()
        });
        let waiters = std::mem::take(&mut self.lock().waiters);
        (out, waiters)
    }

    fn commit_loop(&self) {
        let mut state = self.lock();
        loop {
            if state.stopped {
                return;
            }
            let now = Instant::now();
            if !state.dirty || state.in_flight {
                state = self
                    .wake
                    .wait(state)
                    .unwrap_or_else(PoisonError::into_inner);
                continue;
            }
            if let Some(at) = state.next_at
                && at > now
            {
                state = self
                    .wake
                    .wait_timeout(state, at - now)
                    .unwrap_or_else(PoisonError::into_inner)
                    .0;
                continue;
            }
            state.dirty = false;
            state.in_flight = true;
            let waiters = std::mem::take(&mut state.waiters);
            drop(state);
            let outcome = (self.write)();
            state = self.lock();
            state.in_flight = false;
            let failure = match outcome {
                Ok(bytes) => {
                    let pace =
                        Duration::from_secs_f64(bytes as f64 / PROGRESS_BYTES_PER_SECOND as f64);
                    state.next_at = Some(now + self.min_interval.max(pace));
                    for waiter in waiters {
                        let _ = waiter.send(Ok(()));
                    }
                    None
                }
                Err(error) => {
                    state.next_at = Some(now + self.min_interval);
                    for waiter in waiters {
                        let _ = waiter.send(Err(error.to_string()));
                    }
                    Some(error)
                }
            };
            self.wake.notify_all();
            drop(state);
            if let Some(error) = failure {
                (self.on_error)(error);
            }
            state = self.lock();
        }
    }

    fn lock(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

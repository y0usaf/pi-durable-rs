use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};

type Callback = Box<dyn FnOnce() + Send>;

struct Inner {
    cancelled: AtomicBool,
    next_id: AtomicU64,
    callbacks: Mutex<Vec<(u64, Callback)>>,
    signal: Condvar,
}

#[derive(Clone)]
pub struct CancelToken(Arc<Inner>);

pub struct Subscription {
    token: CancelToken,
    id: u64,
}

impl Drop for Subscription {
    fn drop(&mut self) {
        let mut callbacks = self.token.0.callbacks.lock().unwrap();
        callbacks.retain(|(id, _)| *id != self.id);
    }
}

impl Default for CancelToken {
    fn default() -> Self {
        Self::new()
    }
}

impl CancelToken {
    pub fn new() -> Self {
        Self(Arc::new(Inner {
            cancelled: AtomicBool::new(false),
            next_id: AtomicU64::new(0),
            callbacks: Mutex::new(Vec::new()),
            signal: Condvar::new(),
        }))
    }

    pub fn is_cancelled(&self) -> bool {
        self.0.cancelled.load(Ordering::SeqCst)
    }

    pub fn cancel(&self) {
        let mut callbacks = self.0.callbacks.lock().unwrap();
        if self.0.cancelled.swap(true, Ordering::SeqCst) {
            return;
        }
        let pending = std::mem::take(&mut *callbacks);
        drop(callbacks);
        self.0.signal.notify_all();
        for (_, callback) in pending {
            callback();
        }
    }

    pub fn on_cancel(&self, callback: impl FnOnce() + Send + 'static) {
        let mut callbacks = self.0.callbacks.lock().unwrap();
        if self.is_cancelled() {
            drop(callbacks);
            callback();
        } else {
            let id = self.0.next_id.fetch_add(1, Ordering::Relaxed);
            callbacks.push((id, Box::new(callback)));
        }
    }

    pub fn subscribe(&self, callback: impl FnOnce() + Send + 'static) -> Subscription {
        let mut callbacks = self.0.callbacks.lock().unwrap();
        let id = self.0.next_id.fetch_add(1, Ordering::Relaxed);
        if self.is_cancelled() {
            drop(callbacks);
            callback();
        } else {
            callbacks.push((id, Box::new(callback)));
        }
        Subscription {
            token: self.clone(),
            id,
        }
    }

    pub fn child(&self) -> CancelToken {
        let child = CancelToken::new();
        let link = child.clone();
        self.on_cancel(move || link.cancel());
        child
    }

    pub fn wait_timeout(&self, timeout: std::time::Duration) -> bool {
        let guard = self.0.callbacks.lock().unwrap();
        let (_guard, _) = self
            .0
            .signal
            .wait_timeout_while(guard, timeout, |_| !self.is_cancelled())
            .unwrap();
        self.is_cancelled()
    }
}

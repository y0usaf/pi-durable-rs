//! FIFO turn lock: one scheduler handler runs at a time. A handler gives the turn up only at a blocking
//! wait (see `pause`) and queues again behind every thread already waiting, so handlers that never block run in
//! the order they were reserved, like JS tasks that run to completion.

use std::cell::RefCell;
use std::collections::VecDeque;
use std::sync::{Arc, Condvar, Mutex, MutexGuard, PoisonError};

#[derive(Clone, Default)]
pub struct Turn(Arc<Shared>);

#[derive(Default)]
struct Shared {
    queue: Mutex<Queue>,
    moved: Condvar,
}

#[derive(Default)]
struct Queue {
    next: u64,
    waiting: VecDeque<u64>,
    held: bool,
}

/// A place in the FIFO queue, reserved by the thread that will later run under it.
pub struct Ticket {
    turn: Turn,
    id: u64,
}

thread_local! {
    static HELD: RefCell<Option<Turn>> = const { RefCell::new(None) };
}

impl Turn {
    pub fn reserve(&self) -> Ticket {
        Ticket {
            turn: self.clone(),
            id: self.enqueue(),
        }
    }

    fn enqueue(&self) -> u64 {
        let mut queue = self.lock();
        let id = queue.next;
        queue.next += 1;
        queue.waiting.push_back(id);
        id
    }

    fn take(&self, id: u64) {
        let mut queue = self.lock();
        while queue.held || queue.waiting.front() != Some(&id) {
            queue = self
                .0
                .moved
                .wait(queue)
                .unwrap_or_else(PoisonError::into_inner);
        }
        queue.waiting.pop_front();
        queue.held = true;
    }

    fn release(&self) {
        self.lock().held = false;
        self.0.moved.notify_all();
    }

    fn forget(&self, id: u64) {
        let mut queue = self.lock();
        queue.waiting.retain(|waiting| *waiting != id);
        self.0.moved.notify_all();
    }

    fn lock(&self) -> MutexGuard<'_, Queue> {
        self.0.queue.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

impl Ticket {
    pub fn run<R>(self, body: impl FnOnce() -> R) -> R {
        self.turn.take(self.id);
        let _held = Held::enter(self.turn.clone());
        body()
    }
}

impl Drop for Ticket {
    fn drop(&mut self) {
        self.turn.forget(self.id);
    }
}

struct Held(Turn);

impl Held {
    fn enter(turn: Turn) -> Self {
        HELD.with(|held| *held.borrow_mut() = Some(turn.clone()));
        Self(turn)
    }
}

impl Drop for Held {
    fn drop(&mut self) {
        HELD.with(|held| *held.borrow_mut() = None);
        self.0.release();
    }
}

struct Again(Turn);

impl Drop for Again {
    fn drop(&mut self) {
        let id = self.0.enqueue();
        self.0.take(id);
        HELD.with(|held| *held.borrow_mut() = Some(self.0.clone()));
    }
}

/// The turn the calling thread holds, if it runs under one.
pub fn current() -> Option<Turn> {
    HELD.with(|held| held.borrow().clone())
}

/// Runs `body` with the turn given up, then queues again behind every waiting thread.
///
/// Call it only around a wait for another thread, and only while holding no lock that a turn holder may take:
/// `body` must release such guards before it returns, because the re-acquire blocks.
pub fn pause<R>(body: impl FnOnce() -> R) -> R {
    let Some(turn) = current() else {
        return body();
    };
    turn.release();
    HELD.with(|held| *held.borrow_mut() = None);
    let _again = Again(turn);
    body()
}

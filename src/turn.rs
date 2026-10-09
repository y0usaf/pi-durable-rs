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

pub fn current() -> Option<Turn> {
    HELD.with(|held| held.borrow().clone())
}

pub fn pause<R>(body: impl FnOnce() -> R) -> R {
    let Some(turn) = current() else {
        return body();
    };
    turn.release();
    HELD.with(|held| *held.borrow_mut() = None);
    let _again = Again(turn);
    body()
}

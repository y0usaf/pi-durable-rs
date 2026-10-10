use std::sync::mpsc::{Receiver, Sender, channel};
use std::sync::{Mutex, MutexGuard, PoisonError};
use std::thread;
use std::time::Duration;

type Job = Box<dyn FnOnce() + Send>;

const KEEP: Duration = Duration::from_secs(2);

struct Idle {
    id: u64,
    jobs: Sender<Job>,
}

struct Pool {
    next: u64,
    idle: Vec<Idle>,
}

static POOL: Mutex<Pool> = Mutex::new(Pool {
    next: 0,
    idle: Vec::new(),
});

fn pool() -> MutexGuard<'static, Pool> {
    POOL.lock().unwrap_or_else(PoisonError::into_inner)
}

pub(crate) fn spawn(job: impl FnOnce() + Send + 'static) {
    let mut job: Job = Box::new(job);
    loop {
        let Some(idle) = pool().idle.pop() else {
            break;
        };
        match idle.jobs.send(job) {
            Ok(()) => return,
            Err(returned) => job = returned.0,
        }
    }
    let id = {
        let mut pool = pool();
        pool.next += 1;
        pool.next
    };
    thread::spawn(move || work(id, job));
}

fn work(id: u64, mut job: Job) {
    let (jobs, inbox) = channel::<Job>();
    loop {
        job();
        pool().idle.push(Idle {
            id,
            jobs: jobs.clone(),
        });
        match next(id, &inbox) {
            Some(following) => job = following,
            None => return,
        }
    }
}

fn next(id: u64, inbox: &Receiver<Job>) -> Option<Job> {
    if let Ok(job) = inbox.recv_timeout(KEEP) {
        return Some(job);
    }
    pool().idle.retain(|idle| idle.id != id);
    inbox.try_recv().ok()
}

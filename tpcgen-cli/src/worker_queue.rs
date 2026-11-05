//! [`WorkerQueue`]: run generation tasks in parallel within a thread budget.

use log::debug;
use std::future::Future;
use std::io;
use std::sync::Arc;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::task::{JoinError, JoinSet};

/// How many threads of a [`WorkerQueue`] there are per running task (output
/// file), at least [`MIN_TASKS`]. Each file has a single writer, which cannot
/// keep up with many threads, so the threads are spread over several files.
const THREADS_PER_TASK: usize = 8;

/// The fewest tasks a [`WorkerQueue`] runs at once, so that the end of one
/// file overlaps the start of the next
const MIN_TASKS: usize = 2;

/// How many chunks the tasks of a [`WorkerQueue`] may have in flight (from
/// starting one until it is written), per thread. More than one, so a thread
/// that finishes a chunk can start another while the writers catch up, but
/// few enough to bound the memory the finished, unwritten chunks take.
const CHUNKS_PER_THREAD: usize = 2;

/// The permits of a [`WorkerQueue`], shared by all its tasks
#[derive(Debug, Clone)]
pub(crate) struct Permits {
    /// One per thread: held while generating a chunk
    threads: Arc<Semaphore>,
    /// [`CHUNKS_PER_THREAD`] per thread: held from starting a chunk until it is
    /// written
    chunks: Arc<Semaphore>,
}

tokio::task_local! {
    /// The permits of the [`WorkerQueue`] that runs the current task
    static PERMITS: Permits;
}

impl Permits {
    /// The permits of the [`WorkerQueue`] that runs the current task, if any.
    /// Take them before spawning the work for a chunk: a spawned task does not
    /// inherit them.
    pub(crate) fn current() -> Option<Self> {
        PERMITS.try_with(Self::clone).ok()
    }

    /// Waits for a chunk permit: start a chunk only with one, and hold it
    /// until the chunk is written. Without permits (outside a
    /// [`WorkerQueue`]) returns at once.
    pub(crate) async fn chunk(permits: &Option<Self>) -> Option<OwnedSemaphorePermit> {
        Self::acquire(permits.as_ref().map(|p| &p.chunks)).await
    }

    /// Waits for a thread permit: generate a chunk only while holding one.
    /// Without permits (outside a [`WorkerQueue`]) returns at once.
    pub(crate) async fn thread(permits: &Option<Self>) -> Option<OwnedSemaphorePermit> {
        Self::acquire(permits.as_ref().map(|p| &p.threads)).await
    }

    async fn acquire(semaphore: Option<&Arc<Semaphore>>) -> Option<OwnedSemaphorePermit> {
        // the semaphores are never closed
        Arc::clone(semaphore?).acquire_owned().await.ok()
    }
}

/// Manages worker tasks, limiting the number of total outstanding threads
/// to some fixed number
///
/// Each task writes one output file from its chunks (e.g. parts or row
/// groups). It may have as many chunks in flight as the chunks it will
/// produce, but no more than the total number of threads specified when
/// creating the queue. The queue runs one task per [`THREADS_PER_TASK`]
/// threads at once (at least [`MIN_TASKS`]), the largest first.
///
/// All tasks share the queue's permits: a chunk is generated only while
/// holding a thread permit, and at most [`CHUNKS_PER_THREAD`] chunks per
/// thread are in flight. So no more chunks are generated at once than there
/// are threads, and a thread one file does not need (at its end, or while its
/// writer is behind) generates a chunk of another file instead of idling.
///
/// This is important to keep all cores busy, both for smaller tables that may
/// not have sufficient chunks to keep all threads busy and for the many files
/// of a large table, without generating more chunks at once than we have
/// threads for.
///
/// Scheduling too many tasks requires more memory and leads to context
/// switching overhead, which can slow down the generation process.
pub(crate) struct WorkerQueue {
    join_set: JoinSet<io::Result<()>>,
    /// The most threads one task can use
    max_threads: usize,
    /// Current number of tasks that may still be started
    available_tasks: usize,
    /// Shared by all tasks
    permits: Permits,
}

impl WorkerQueue {
    pub(crate) fn new(max_threads: usize) -> Self {
        assert!(max_threads > 0);
        Self {
            join_set: JoinSet::new(),
            max_threads,
            available_tasks: (max_threads / THREADS_PER_TASK).max(MIN_TASKS),
            permits: Permits {
                threads: Arc::new(Semaphore::new(max_threads)),
                chunks: Arc::new(Semaphore::new(max_threads * CHUNKS_PER_THREAD)),
            },
        }
    }

    /// Spawns a task that can use up to `chunk_count` threads, no more than
    /// the maximum number of threads. The task is created by calling `task`
    /// with the number of threads allocated to it: the most chunks it should
    /// have in flight. Its chunks take the queue's [`Permits`] (see
    /// [`Permits::current`]).
    ///
    /// If the queue already runs as many tasks as it may, waits for a running
    /// task to finish before spawning the new one.
    ///
    /// Note this algorithm does not guarantee that all threads are always busy,
    /// but it should be good enough for most cases. For best thread utilization
    /// spawn the largest tasks first.
    pub(crate) async fn schedule<F, Fut>(&mut self, chunk_count: usize, task: F) -> io::Result<()>
    where
        F: FnOnce(usize) -> Fut,
        Fut: Future<Output = io::Result<()>> + Send + 'static,
    {
        if self.available_tasks == 0 {
            debug!("no tasks left, wait for one to finish");
            let Some(result) = self.join_set.join_next().await else {
                return Err(io::Error::other(
                    "Internal Error No more tasks to wait for, but had no tasks left",
                ));
            };
            task_result(result)?;
            self.available_tasks += 1;
        }
        // Check for any other jobs done so their errors surface early
        while let Some(result) = self.join_set.try_join_next() {
            task_result(result)?;
            self.available_tasks += 1;
        }

        let num_task_threads = chunk_count.clamp(1, self.max_threads);
        debug!("Spawning task with {num_task_threads} threads");
        let permits = self.permits.clone();
        self.join_set
            .spawn(PERMITS.scope(permits, task(num_task_threads)));
        self.available_tasks -= 1;
        Ok(())
    }

    /// Wait for all tasks to finish
    pub(crate) async fn join_all(mut self) -> io::Result<()> {
        debug!("Waiting for tasks to finish...");
        while let Some(result) = self.join_set.join_next().await {
            task_result(result)?;
        }
        debug!("Tasks finished.");
        Ok(())
    }
}

/// unwraps the result of a task and converts it to an `io::Result<T>`.
fn task_result<T>(result: Result<io::Result<T>, JoinError>) -> io::Result<T> {
    result.map_err(|e| io::Error::other(format!("Task Panic: {e}")))?
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    #[tokio::test]
    async fn schedules_more_tasks_than_threads() {
        let completed = Arc::new(AtomicUsize::new(0));
        let mut queue = WorkerQueue::new(2);

        for chunk_count in [3, 1, 1, 2, 1] {
            let completed = Arc::clone(&completed);
            queue
                .schedule(chunk_count, move |num_threads| async move {
                    // each task gets at least one thread and no more than
                    // its chunk count or the maximum
                    assert!(num_threads >= 1);
                    assert!(num_threads <= chunk_count.min(2));
                    completed.fetch_add(1, Ordering::Relaxed);
                    Ok(())
                })
                .await
                .unwrap();
        }
        queue.join_all().await.unwrap();

        assert_eq!(completed.load(Ordering::Relaxed), 5);
    }

    #[tokio::test]
    async fn task_error_is_propagated() {
        let mut queue = WorkerQueue::new(1);
        queue
            .schedule(1, |num_threads| async move {
                let _ = num_threads;
                Err(io::Error::other("boom"))
            })
            .await
            .unwrap();

        let err = queue.join_all().await.unwrap_err();
        assert!(err.to_string().contains("boom"));
    }
}

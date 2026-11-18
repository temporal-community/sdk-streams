//! The `LazyStore` of `temporalio-streams-log`, on a memory Store. The log crate has no Store of
//! its own, so this crate tests its `LazyStore`.

use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use temporalio_streams::{Reason, StreamError, proto::StreamRef};
use temporalio_streams_log::store::{Capabilities, Create, DEFAULT_RETENTION, LazyStore, Store};
use temporalio_streams_memory::MemoryStore;

fn stream() -> StreamRef {
    StreamRef {
        id: "s".to_string(),
        ..Default::default()
    }
}

fn memory() -> MemoryStore {
    MemoryStore::new(DEFAULT_RETENTION, None)
}

#[tokio::test]
async fn a_failed_connect_is_tried_again_on_the_next_call() {
    let connects = Arc::new(AtomicUsize::new(0));
    let counted = connects.clone();
    let lazy = LazyStore::new(Capabilities::ALL, move || {
        let first = counted.fetch_add(1, Ordering::SeqCst) == 0;
        async move {
            if first {
                return Err(StreamError::storage_unavailable("the Store is down"));
            }
            Ok(memory())
        }
    });
    assert_eq!(lazy.capabilities(), Capabilities::ALL);
    assert_eq!(connects.load(Ordering::SeqCst), 0);
    let error = lazy
        .create(&stream(), &Create::default())
        .await
        .unwrap_err();
    assert_eq!(error.reason, Reason::StorageUnavailable);
    lazy.create(&stream(), &Create::default()).await.unwrap();
    let (state, _) = lazy.read(&stream(), 0, 0).await.unwrap();
    assert!(state.exists);
    assert_eq!(connects.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn a_connect_refuses_a_store_that_lacks_a_declared_capability_once() {
    let connects = Arc::new(AtomicUsize::new(0));
    let counted = connects.clone();
    let lazy = LazyStore::new(Capabilities::ALL, move || {
        counted.fetch_add(1, Ordering::SeqCst);
        async {
            let fewer = LazyStore::new(Capabilities::CLOSE, || async {
                Ok::<_, StreamError>(memory())
            });
            Ok::<_, StreamError>(fewer)
        }
    });
    for _ in 0..2 {
        let error = lazy.read(&stream(), 0, 0).await.unwrap_err();
        assert_eq!(error.reason, Reason::Unsupported);
        assert!(
            error.message.contains("producer_fence"),
            "{}",
            error.message
        );
    }
    assert_eq!(connects.load(Ordering::SeqCst), 1);
}

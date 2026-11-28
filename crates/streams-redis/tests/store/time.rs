//! When a Stream expires, and what an expired Stream answers.

use crate::{
    reads::until_blocked,
    support::{
        NoRequestId, Redis, append, create, describe, keys, made, read, reason, runtime, standalone,
    },
};
use redis::aio::MultiplexedConnection;
use std::time::Duration;
use temporalio_streams::proto::{
    ReadRecordsResponse, StreamErrorReason, StreamIdConflictPolicy, sdk::CreateStreamRequest,
};

/// How long the meta key stays after its Stream expired, in ms.
const TOMBSTONE: i64 = 30 * 24 * 60 * 60 * 1000;

/// Maximum TTL that a script gets, in ms.
const MAX_TTL: u128 = 1 << 52;

async fn time_ms(admin: &mut MultiplexedConnection) -> i64 {
    let (seconds, micros): (i64, i64) = redis::cmd("TIME").query_async(admin).await.unwrap();
    seconds * 1000 + micros / 1000
}

/// Returns `exp` of a Stream and the expiry time of each of its keys. Redis gives -2 for a key
/// that doesn't exist.
async fn expiry(admin: &mut MultiplexedConnection, keys: &[String; 3]) -> (i64, [i64; 3]) {
    let mut pipe = redis::pipe();
    pipe.cmd("HGET").arg(&keys[0]).arg("exp");
    for key in keys {
        pipe.cmd("PEXPIRETIME").arg(key);
    }
    let (exp, meta, log, producers) = pipe.query_async(admin).await.unwrap();
    (exp, [meta, log, producers])
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs Redis"]
async fn the_retention_starts_again_at_each_append() {
    #[derive(Debug)]
    enum Call {
        Create,
        Append,
        Resend,
        RetryCreate,
        UseExisting,
        Read,
        Describe,
    }

    let redis = Redis::start(&[]);
    let mut admin = redis.admin().await;
    let ttls = [
        Duration::from_secs(1),
        Duration::from_secs(90),
        Duration::from_secs(7 * 24 * 60 * 60),
        // A million years, which is longer than a script can add to the time.
        Duration::from_secs(1_000_000 * 365 * 24 * 60 * 60),
    ];
    for (case, ttl) in ttls.into_iter().enumerate() {
        let prefix = format!("time-{case}");
        // A later writer's configuration must not change the Stream's retention.
        let stores = [ttl, Duration::from_secs(3600)].map(|retention| {
            let mut options = standalone(redis.url());
            options.key_prefix = prefix.parse().unwrap();
            options.retention = retention;
            runtime(options)
        });
        let keys = keys(&prefix, "s");
        let retention = ttl.as_millis().min(MAX_TTL) as i64;
        let mut wrote = (0, 0);
        let mut appends = 0;
        for (call, via) in [
            (Call::Create, 0),
            (Call::Read, 1),
            (Call::Describe, 1),
            (Call::RetryCreate, 1),
            (Call::UseExisting, 1),
            (Call::Append, 1),
            (Call::Resend, 0),
            (Call::RetryCreate, 1),
            (Call::UseExisting, 1),
            (Call::Read, 1),
            (Call::Describe, 1),
            (Call::Append, 0),
        ] {
            let store = &stores[via];
            let context = format!("case {case}, {call:?}");
            // Expiry changes must be visible at Redis's millisecond precision.
            tokio::time::sleep(Duration::from_millis(2)).await;
            let before = time_ms(&mut admin).await;
            let writes = match call {
                Call::Create => {
                    let created = store.create_stream(create("s"), "c".to_string()).await;
                    assert_eq!(created, Ok(made(&create("s"))), "{context}");
                    true
                }
                Call::Append => {
                    appends += 1;
                    let request_id = format!("a{appends}");
                    store
                        .append_records(append("s", &[b"x", b"y"]), request_id)
                        .await
                        .unwrap();
                    true
                }
                Call::Resend => {
                    let request_id = format!("a{appends}");
                    let resend = append("s", &[b"x"]);
                    store.append_records(resend, request_id).await.unwrap();
                    false
                }
                Call::RetryCreate => {
                    let retry = store.create_stream(create("s"), "c".to_string()).await;
                    assert_eq!(retry, Ok(made(&create("s"))), "{context}");
                    false
                }
                Call::UseExisting => {
                    let again = CreateStreamRequest {
                        id_conflict_policy: StreamIdConflictPolicy::UseExisting as i32,
                        ..create("s")
                    };
                    let found = store.create_no_id(again).await.unwrap();
                    assert!(!found.created, "{context}");
                    false
                }
                Call::Read => {
                    store.read_records(read("s", &[])).await.unwrap();
                    false
                }
                Call::Describe => {
                    store.describe_stream(describe("s")).await.unwrap();
                    false
                }
            };
            let after = time_ms(&mut admin).await;
            if writes {
                wrote = (before, after);
            }
            let (exp, expiries) = expiry(&mut admin, &keys).await;
            let (before, after) = wrote;
            assert!(
                before + retention <= exp && exp <= after + retention,
                "{exp}, {context}"
            );
            assert_eq!(expiries[0], exp + TOMBSTONE, "{context}");
            // A key that the Stream doesn't have yet gets its expiry with its first write.
            let mut others = expiries[1..].iter().filter(|at| **at != -2);
            assert!(others.all(|at| *at == exp), "{expiries:?}, {context}");
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs Redis"]
async fn an_expired_stream_answers_expired_until_the_store_forgets_it() {
    let redis = Redis::start(&[]);
    let mut admin = redis.admin().await;
    // Long enough for the two reads to block before the Stream expires, also on a slow host.
    let retention = Duration::from_secs(2);
    let mut options = standalone(redis.url());
    options.retention = retention;
    let store = runtime(options);
    store.create_no_id(create("s")).await.unwrap();
    let appended = store.append_no_id(append("s", &[b"x"])).await.unwrap();
    let expired = StreamErrorReason::Expired;
    let [meta, ..] = keys("temporal-streams", "s");
    let exp: i64 = redis::cmd("HGET")
        .arg(&meta)
        .arg("exp")
        .query_async(&mut admin)
        .await
        .unwrap();
    // Even the longest valid protobuf wait must end when the Stream expires.
    let waiting = |seconds| {
        let mut request = read("s", &appended.last_offset);
        request.wait = Some(Default::default());
        request.wait.as_mut().unwrap().seconds = seconds;
        store.read_records(request)
    };
    let url = redis.url();
    let reads = async {
        tokio::join!(
            waiting(60),
            waiting(315_576_000_000),
            until_blocked(&url, 2)
        )
    };
    let (bounded, unbounded, ()) = tokio::time::timeout(Duration::from_secs(30), reads)
        .await
        .expect("a read that waits didn't end at the expiry");
    assert_eq!((reason(bounded), reason(unbounded)), (expired, expired));
    assert!(time_ms(&mut admin).await >= exp);

    assert_eq!(
        reason(store.append_no_id(append("s", &[b"y"])).await),
        expired
    );
    assert_eq!(reason(store.describe_stream(describe("s")).await), expired);
    let mut waits = read("s", &[]);
    waits.wait = Some(Default::default());
    waits.wait.as_mut().unwrap().seconds = 30;
    assert_eq!(reason(store.read_records(waits).await), expired);
    // The id is never used for a second Stream while the Store knows it.
    for policy in [
        StreamIdConflictPolicy::Fail,
        StreamIdConflictPolicy::UseExisting,
    ] {
        let again = CreateStreamRequest {
            id_conflict_policy: policy as i32,
            ..create("s")
        };
        assert_eq!(reason(store.create_no_id(again).await), expired);
    }

    // Redis removes the meta key 30 days after the expiry. The test removes it itself.
    let _: i64 = redis::cmd("DEL")
        .arg(&meta)
        .query_async(&mut admin)
        .await
        .unwrap();
    assert_eq!(
        reason(store.describe_stream(describe("s")).await),
        StreamErrorReason::NotFound
    );
    assert_eq!(
        store.create_no_id(create("s")).await,
        Ok(made(&create("s")))
    );
    // The new Stream has none of the Records of the expired one.
    let page = store.read_records(read("s", &[])).await;
    assert_eq!(page, Ok(ReadRecordsResponse::default()));
}

//! When a Stream expires, and what an expired Stream answers.

use crate::{
    reads::until_blocked,
    support::{
        NoRequestId, Redis, Rng, append, create, describe, keys, made, read, reason, runtime,
        seeds, standalone,
    },
};
use redis::aio::MultiplexedConnection;
use std::time::Duration;
use temporalio_streams::{
    StreamRuntime,
    proto::{
        ReadRecordsResponse, StreamErrorReason, StreamIdConflictPolicy, sdk::CreateStreamRequest,
    },
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
    let redis = Redis::start(&[]);
    let mut admin = redis.admin().await;
    let ttls = [
        Duration::from_secs(1),
        Duration::from_secs(90),
        Duration::from_secs(7 * 24 * 60 * 60),
        // A million years, which is longer than a script can add to the time.
        Duration::from_secs(1_000_000 * 365 * 24 * 60 * 60),
    ];
    for seed in seeds(40) {
        let mut rng = Rng::new(seed);
        let prefix = format!("time-{seed}");
        // Two Stores of one Redis with their own Retention, as two Workers can have.
        let retentions = [(); 2].map(|()| rng.pick(&ttls));
        let stores: Vec<StreamRuntime> = (retentions.iter())
            .map(|retention| {
                let mut options = standalone(redis.url());
                options.key_prefix = prefix.parse().unwrap();
                options.retention = *retention;
                runtime(options)
            })
            .collect();
        let keys = keys(&prefix, "s");
        // The Stream keeps the Retention of the Store that creates it.
        let creator = rng.below(2);
        let retention = retentions[creator].as_millis().min(MAX_TTL) as i64;
        // The times of Redis around the last create or append, which the Retention counts from.
        let mut wrote = (0, 0);
        let mut appends = 0;
        for step in 0..2 + rng.below(8) {
            let store = match step {
                0 => &stores[creator],
                _ => &stores[rng.below(2)],
            };
            let context = format!("seed {seed}, step {step}");
            // A call that wrongly starts the Retention again then moves `exp` by a millisecond.
            tokio::time::sleep(Duration::from_millis(2)).await;
            let before = time_ms(&mut admin).await;
            let writes = match (step, rng.below(10)) {
                (0, _) => {
                    let created = store.create_stream(create("s"), "c".to_string()).await;
                    assert_eq!(created, Ok(made(&create("s"))), "{context}");
                    true
                }
                (_, 0..5) => {
                    appends += 1;
                    let records: Vec<&[u8]> = vec![b"x"; 1 + rng.below(2)];
                    let request_id = format!("a{appends}");
                    store
                        .append_records(append("s", &records), request_id)
                        .await
                        .unwrap();
                    true
                }
                // A resend and a retry of the create write nothing, so the Retention goes on.
                (_, 5) if appends > 0 => {
                    let request_id = format!("a{appends}");
                    let resend = append("s", &[b"x"]);
                    store.append_records(resend, request_id).await.unwrap();
                    false
                }
                (_, 6) => {
                    let retry = store.create_stream(create("s"), "c".to_string()).await;
                    assert_eq!(retry, Ok(made(&create("s"))), "{context}");
                    false
                }
                (_, 7) => {
                    let again = CreateStreamRequest {
                        id_conflict_policy: StreamIdConflictPolicy::UseExisting as i32,
                        ..create("s")
                    };
                    let found = store.create_no_id(again).await.unwrap();
                    assert!(!found.created, "{context}");
                    false
                }
                _ => {
                    store.read_records(read("s", &[])).await.unwrap();
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
    // A read that waits ends at the expiry, also one whose wait is too long to have a deadline.
    let waiting = |seconds| {
        let mut request = read("s", &appended.last_offset);
        request.wait = Some(Default::default());
        request.wait.as_mut().unwrap().seconds = seconds;
        store.read_records(request)
    };
    let url = redis.url();
    let reads = async { tokio::join!(waiting(60), waiting(i64::MAX), until_blocked(&url, 2)) };
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

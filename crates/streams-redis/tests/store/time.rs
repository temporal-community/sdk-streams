//! When a Stream expires, and what an expired Stream answers.

use crate::support::{
    Redis, Rng, append, close, create, describe, keys, read, reason, runtime, seeds,
};
use redis::aio::MultiplexedConnection;
use std::time::Duration;
use temporalio_streams::{
    StreamRuntime,
    proto::{CreateStreamRequest, StreamErrorReason, StreamIdConflictPolicy},
};
use temporalio_streams_redis::RedisStoreOptions;

/// How long the meta key stays after its Stream expired, in ms.
const TOMBSTONE: i64 = 30 * 24 * 60 * 60 * 1000;

/// The longest TTL that a script gets, in ms.
const MAX_TTL: u128 = 1 << 52;

async fn time_ms(admin: &mut MultiplexedConnection) -> i64 {
    let (seconds, micros): (i64, i64) = redis::cmd("TIME").query_async(admin).await.unwrap();
    seconds * 1000 + micros / 1000
}

/// Returns `exp` of a Stream and the expiry time of each of its keys. Redis gives -1 for a key
/// with no expiry and -2 for a key that doesn't exist.
async fn expiry(admin: &mut MultiplexedConnection, keys: &[String; 4]) -> (Option<i64>, [i64; 4]) {
    let mut pipe = redis::pipe();
    pipe.cmd("HGET").arg(&keys[0]).arg("exp");
    for key in keys {
        pipe.cmd("PEXPIRETIME").arg(key);
    }
    let (exp, meta, log, producers, closed) = pipe.query_async(admin).await.unwrap();
    (exp, [meta, log, producers, closed])
}

#[tokio::test(flavor = "multi_thread")]
async fn each_key_expires_when_its_rule_says() {
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
        // Two Stores of one Redis with their own settings, as two Workers can have.
        let settings = [(); 2].map(|()| (rng.pick(&ttls), rng.chance(60).then(|| rng.pick(&ttls))));
        let stores: Vec<StreamRuntime> = (settings.iter())
            .map(|(retention, producer_idle_ttl)| {
                let mut options = RedisStoreOptions::new(redis.url());
                options.key_prefix = prefix.clone();
                options.retention = *retention;
                options.producer_idle_ttl = *producer_idle_ttl;
                runtime(options)
            })
            .collect();
        let keys = keys(&prefix, "s");
        let ms = |ttl: Duration| ttl.as_millis().min(MAX_TTL) as i64;
        // The Stream keeps the settings of the Store that creates it.
        let creator = rng.below(2);
        let (retention, idle) = (ms(settings[creator].0), settings[creator].1.map(ms));
        // The TTL of the Stream's state, and the time of the write that it counts from.
        let mut rule = None;
        let mut closed = false;
        for step in 0..2 + rng.below(8) {
            let store = match step {
                0 => &stores[creator],
                _ => &stores[rng.below(2)],
            };
            let context = format!("seed {seed}, step {step}");
            let before = time_ms(&mut admin).await;
            let wrote = match (step, rng.below(10)) {
                (0, _) => {
                    store.create_stream(create("s")).await.unwrap();
                    true
                }
                (_, 0..5) if !closed => {
                    let records: Vec<&[u8]> = vec![b"x"; 1 + rng.below(2)];
                    store.append_records(append("s", &records)).await.unwrap();
                    true
                }
                (_, 5..7) => {
                    store.close_stream(close("s")).await.unwrap();
                    !std::mem::replace(&mut closed, true)
                }
                (_, 7) => {
                    let again = CreateStreamRequest {
                        id_conflict_policy: StreamIdConflictPolicy::UseExisting as i32,
                        ..create("s")
                    };
                    // A closed Stream refuses the create, and neither answer is a write.
                    let _ = store.create_stream(again).await;
                    false
                }
                _ => {
                    store.read_records(read("s", &[])).await.unwrap();
                    store.describe_stream(describe("s")).await.unwrap();
                    false
                }
            };
            let after = time_ms(&mut admin).await;
            if wrote {
                let ttl = if closed { Some(retention) } else { idle };
                rule = ttl.map(|ttl| (ttl, before, after));
            }
            let (exp, expiries) = expiry(&mut admin, &keys).await;
            let Some((ttl, before, after)) = rule else {
                assert_eq!(exp, None, "{context}");
                assert!(expiries.iter().all(|at| *at < 0), "{expiries:?}, {context}");
                continue;
            };
            let exp = exp.unwrap();
            assert!(
                before + ttl <= exp && exp <= after + ttl,
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
async fn an_expired_stream_answers_expired() {
    let redis = Redis::start(&[]);
    let mut admin = redis.admin().await;
    let store = |retention, producer_idle_ttl| {
        let mut options = RedisStoreOptions::new(redis.url());
        options.retention = retention;
        options.producer_idle_ttl = producer_idle_ttl;
        runtime(options)
    };
    let short = Duration::from_millis(1);
    let long = Duration::from_secs(3600);
    // A closed Stream past its Retention, and an open one past its Producer idle TTL.
    let closed = store(short, None);
    closed.create_stream(create("closed")).await.unwrap();
    closed
        .append_records(append("closed", &[b"x"]))
        .await
        .unwrap();
    closed.close_stream(close("closed")).await.unwrap();
    let idle = store(long, Some(short));
    idle.create_stream(create("idle")).await.unwrap();
    while time_ms(&mut admin).await <= latest_expiry(&mut admin).await {
        tokio::time::sleep(short).await;
    }
    for (stream, store) in [("closed", &closed), ("idle", &idle)] {
        let expired = StreamErrorReason::Expired;
        assert_eq!(
            reason(store.append_records(append(stream, &[b"y"])).await),
            expired
        );
        assert_eq!(reason(store.close_stream(close(stream)).await), expired);
        assert_eq!(
            reason(store.describe_stream(describe(stream)).await),
            expired
        );
        let mut waits = read(stream, &[]);
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
                ..create(stream)
            };
            assert_eq!(reason(store.create_stream(again).await), expired);
        }
    }
}

/// Returns the later `exp` of the two Streams of the test, whose keys have the default prefix.
async fn latest_expiry(admin: &mut MultiplexedConnection) -> i64 {
    let mut latest = 0;
    for stream in ["closed", "idle"] {
        let [meta, ..] = keys("temporal-streams", stream);
        let exp: i64 = redis::cmd("HGET")
            .arg(meta)
            .arg("exp")
            .query_async(admin)
            .await
            .unwrap();
        latest = latest.max(exp);
    }
    latest
}

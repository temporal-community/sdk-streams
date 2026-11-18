//! The native module of the Python package of the Redis Stream Store. Python calls the five Stream
//! service operations by name, with the encoded request, and gets the encoded response.

// A panic on a Tokio worker would take down a call that Python waits for.
#![warn(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::cast_possible_truncation
)]

use pyo3::{
    exceptions::{PyException, PyRuntimeError, PyValueError},
    prelude::*,
};
use std::{
    sync::{Arc, OnceLock},
    time::Duration,
};
use temporalio_streams::StreamRuntime;
use temporalio_streams_redis::{RedisStreamRuntime, RuntimeOptions, proto::RedisStreamConfig};
use tokio::runtime::{Builder, Runtime};

// The args are the reason, as the integer value of the `StreamErrorReason` proto enum, and the
// message.
pyo3::create_exception!(
    temporalio_streams_redis._native,
    StreamCallError,
    PyException
);

/// The Tokio runtime of every call. It stays up until the process ends.
static TOKIO: OnceLock<Runtime> = OnceLock::new();

/// The id of the process that started the Tokio runtime, or why it did not start.
static STARTED_BY: OnceLock<Result<u32, String>> = OnceLock::new();

/// Starts the Tokio runtime at the first call, and checks that the same process runs the call.
fn start_tokio() -> PyResult<()> {
    let started_by = STARTED_BY.get_or_init(|| {
        let runtime = Builder::new_multi_thread()
            .thread_name("temporal-streams-redis")
            .enable_all()
            .build()
            .map_err(|err| format!("The Redis Stream Store could not start its threads: {err}"))?;
        let runtime = TOKIO.get_or_init(|| runtime);
        // `future_into_py` runs on the runtime that is set here, and builds its own otherwise.
        pyo3_async_runtimes::tokio::init_with_runtime(runtime)
            .map_err(|()| "The Redis Stream Store started its threads twice".to_string())?;
        Ok(std::process::id())
    });
    match started_by {
        Ok(pid) if *pid == std::process::id() => Ok(()),
        // The threads of a Tokio runtime don't exist in a child of a fork.
        Ok(pid) => Err(PyRuntimeError::new_err(format!(
            "The Redis Stream Store can't be used in a child of a fork (its threads started in \
             process {pid}, and this is process {})",
            std::process::id()
        ))),
        Err(message) => Err(PyRuntimeError::new_err(message.clone())),
    }
}

/// Makes sure that a rustls provider is installed before the first connection. The Redis crate
/// reads the provider of the process. The SDK's bridge gets it from Core, and nothing does that
/// here.
fn install_tls_provider() {
    // Installs `ring` for a `tls-ring` build. It does nothing for a `tls-aws-lc` build.
    temporalio_common::telemetry::ensure_default_crypto_provider();
    #[cfg(feature = "tls-aws-lc")]
    {
        // A provider that another component installed first stays, so the error is no fault.
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    }
}

struct Inner {
    options: RuntimeOptions,
    /// Built at the first call, inside the Tokio runtime, since it spawns tasks.
    streams: OnceLock<Arc<StreamRuntime>>,
}

/// The Stream runtime on Redis. It connects to Redis at its first call that needs it.
#[pyclass(frozen)]
struct RedisRuntime {
    inner: Arc<Inner>,
}

#[pymethods]
impl RedisRuntime {
    /// Checks the options, and builds nothing else. A bad option raises `ValueError` with the
    /// message of the Redis crate.
    #[new]
    #[pyo3(signature = (
        urls,
        *,
        cluster,
        key_prefix,
        retention,
        producer_idle_ttl,
        response_timeout,
        store_name,
        wait_replicas,
        wait_timeout,
    ))]
    #[allow(clippy::too_many_arguments)]
    fn new(
        urls: Vec<String>,
        cluster: bool,
        key_prefix: Option<String>,
        retention: Option<Duration>,
        producer_idle_ttl: Option<Duration>,
        response_timeout: Option<Duration>,
        store_name: String,
        wait_replicas: i32,
        wait_timeout: Option<Duration>,
    ) -> PyResult<Self> {
        // The crate reads an empty or zero field as its default, so `None` maps to that.
        let config = RedisStreamConfig {
            urls,
            cluster,
            key_prefix: key_prefix.unwrap_or_default(),
            retention: proto_duration("retention", retention)?,
            producer_idle_ttl: proto_duration("producer_idle_ttl", producer_idle_ttl)?,
            response_timeout: proto_duration("response_timeout", response_timeout)?,
            store_name,
            wait_replicas,
            wait_timeout: proto_duration("wait_timeout", wait_timeout)?,
            ..Default::default()
        };
        let options = RuntimeOptions::try_from(config)
            .map_err(|err| PyValueError::new_err(err.to_string()))?;
        Ok(Self {
            inner: Arc::new(Inner {
                options,
                streams: OnceLock::new(),
            }),
        })
    }

    /// Runs one Stream operation on an encoded request. The awaitable gives the encoded response,
    /// and a cancel of it stops the call.
    fn call<'py>(
        &self,
        py: Python<'py>,
        operation: String,
        request: &[u8],
    ) -> PyResult<Bound<'py, PyAny>> {
        start_tokio()?;
        let inner = self.inner.clone();
        let request = request.to_vec();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let streams = inner
                .streams
                .get_or_init(|| {
                    install_tls_provider();
                    RedisStreamRuntime::create(inner.options.clone(), None)
                })
                .clone();
            streams
                .call(&operation, &request)
                .await
                .map_err(|err| StreamCallError::new_err((err.reason.proto() as i32, err.message)))
        })
    }
}

// The config's durations are the `prost-wkt-types` type that the Redis crate's protos use. This
// crate doesn't depend on it, so the field's type picks the conversion.
fn proto_duration<T>(field: &str, value: Option<Duration>) -> PyResult<Option<T>>
where
    T: TryFrom<Duration>,
    T::Error: std::fmt::Display,
{
    value
        .map(T::try_from)
        .transpose()
        .map_err(|err| PyValueError::new_err(format!("The {field} is too long: {err}")))
}

#[pymodule]
fn _native(py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add("StreamCallError", py.get_type::<StreamCallError>())?;
    module.add_class::<RedisRuntime>()?;
    Ok(())
}

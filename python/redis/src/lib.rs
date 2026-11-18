//! The native module of the Python package of the Redis Stream Store. Python calls the five Stream
//! service operations by name, with the encoded request, and gets the encoded response.

use pyo3::{exceptions::PyValueError, prelude::*};
use std::time::Duration;
use temporalio_streams_python_native::{Identity, StreamBridge, duration_option};
use temporalio_streams_redis::{RedisStreamRuntime, RuntimeOptions, proto::RedisStreamConfig};

const IDENTITY: Identity = Identity {
    threads: "temporal-streams-redis",
    store: "Redis Stream Store",
};

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

/// The Stream runtime on Redis. It connects to Redis at its first call that needs it.
#[pyclass(frozen)]
struct RedisRuntime {
    bridge: StreamBridge,
}

#[pymethods]
impl RedisRuntime {
    /// Checks the options, and builds nothing else. A negative duration, or a bad option, raises
    /// `ValueError`. A bad option has the message of the Redis crate.
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
        retention: Option<Bound<'_, PyAny>>,
        producer_idle_ttl: Option<Bound<'_, PyAny>>,
        response_timeout: Option<Bound<'_, PyAny>>,
        store_name: String,
        wait_replicas: i32,
        wait_timeout: Option<Bound<'_, PyAny>>,
    ) -> PyResult<Self> {
        let retention = duration_option("retention", retention.as_ref())?;
        let producer_idle_ttl = duration_option("producer_idle_ttl", producer_idle_ttl.as_ref())?;
        let response_timeout = duration_option("response_timeout", response_timeout.as_ref())?;
        let wait_timeout = duration_option("wait_timeout", wait_timeout.as_ref())?;
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
        // The runtime is built at the first call, inside the Tokio runtime, since it spawns tasks.
        let bridge = StreamBridge::new(IDENTITY, move || {
            install_tls_provider();
            RedisStreamRuntime::create(options.clone(), None)
        });
        Ok(Self { bridge })
    }

    /// Runs one Stream operation on an encoded request. The awaitable gives the response as a
    /// message of `response_type`, and a cancel of it stops the call.
    fn call<'py>(
        &self,
        py: Python<'py>,
        operation: String,
        request: &[u8],
        response_type: Py<PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        self.bridge.call(py, operation, request, response_type)
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

// The Python module is `_native`, and the library has a name of its own so that the two native
// modules of this workspace build to different files.
#[pymodule]
#[pyo3(name = "_native")]
fn native(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<RedisRuntime>()?;
    Ok(())
}

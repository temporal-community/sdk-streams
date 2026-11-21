// A panic on a Tokio thread would leave the Python call that waits for it with no answer.
#![warn(clippy::unwrap_used, clippy::expect_used)]

//! The native module of the Python package of the Redis Store. It runs the Store of the
//! `temporalio-streams-redis` crate behind a [`StreamRuntime`], on Tokio threads of its own, and
//! Python calls each operation with the encoded request.

use prost::Message;
use pyo3::{
    exceptions::{PyRuntimeError, PyValueError},
    prelude::*,
    types::{PyBytes, PyDict},
};
use std::{
    future::Future,
    sync::{Arc, OnceLock},
    time::Duration,
};
use temporalio_streams::{
    AnyError, StreamError, StreamRuntime, proto::StreamErrorReason, record::StoreLimits,
};
use temporalio_streams_redis::{Fence, RedisServers, RedisStoreOptions, WaitForReplicas};
use tokio::runtime::{Builder, Runtime};

/// The Tokio runtime of every call of the process. It stays up until the process ends.
static TOKIO: OnceLock<Runtime> = OnceLock::new();

/// The id of the process that started [`TOKIO`], or why it didn't start.
static STARTED_BY: OnceLock<Result<u32, String>> = OnceLock::new();

/// Starts the Tokio runtime at the first call, and checks that this process started it. A child
/// of a fork has the runtime with none of its threads, so a call there would never answer.
fn start_tokio() -> PyResult<()> {
    let started_by = STARTED_BY.get_or_init(|| {
        // The calls wait for Redis and do little work, so two threads serve many of them.
        let runtime = Builder::new_multi_thread()
            .worker_threads(2)
            .thread_name("temporal-streams-redis")
            .enable_all()
            .build()
            .map_err(|error| error.to_string())?;
        let runtime = TOKIO.get_or_init(|| runtime);
        pyo3_async_runtimes::tokio::init_with_runtime(runtime)
            .map_err(|()| "another runtime was set first".to_string())?;
        Ok(std::process::id())
    });
    match started_by {
        Ok(pid) if *pid == std::process::id() => Ok(()),
        Ok(pid) => Err(PyRuntimeError::new_err(format!(
            "The Redis Stream Store can't be used in a child of a fork. Its threads run in \
             process {pid}, and this is process {}",
            std::process::id()
        ))),
        Err(error) => Err(PyRuntimeError::new_err(format!(
            "The Redis Stream Store could not start its threads: {error}"
        ))),
    }
}

/// Returns the `temporalio.exceptions.StreamError` of a failed call.
fn stream_error<R: Into<StreamErrorReason>>(error: StreamError<R>) -> PyErr {
    let reason: StreamErrorReason = error.reason.into();
    Python::attach(|py| {
        let raised = || -> PyResult<PyErr> {
            let class = py.import("temporalio.exceptions")?.getattr("StreamError")?;
            let kwargs = PyDict::new(py);
            kwargs.set_item("reason", reason as i32)?;
            Ok(PyErr::from_value(
                class.call((error.message,), Some(&kwargs))?,
            ))
        };
        raised().unwrap_or_else(|import_error| import_error)
    })
}

fn invalid(error: AnyError) -> PyErr {
    PyValueError::new_err(error.message)
}

/// The Redis Store behind the runtime of `temporalio-streams`, which checks every request
/// before the Store gets it and every response on the way back.
#[pyclass(frozen)]
struct RedisStore {
    runtime: Arc<StreamRuntime>,
}

#[pymethods]
impl RedisStore {
    /// Checks the options and builds the Store, which connects at its first call. An option that
    /// is `None` keeps the default of the crate, and a bad one raises `ValueError`.
    #[new]
    #[pyo3(signature = (
        urls,
        *,
        cluster,
        key_prefix,
        retention,
        producer_idle_ttl,
        fence,
        resend_window,
        response_timeout,
        wait_replicas,
        wait_timeout,
        max_blocking_connections,
        max_record_bytes,
        max_batch_bytes,
        max_batch_records,
    ))]
    #[allow(clippy::too_many_arguments)]
    fn new(
        mut urls: Vec<String>,
        cluster: bool,
        key_prefix: Option<String>,
        retention: Option<Duration>,
        producer_idle_ttl: Option<Duration>,
        fence: bool,
        resend_window: Option<usize>,
        response_timeout: Option<Duration>,
        wait_replicas: Option<usize>,
        wait_timeout: Option<Duration>,
        max_blocking_connections: Option<usize>,
        max_record_bytes: Option<usize>,
        max_batch_bytes: Option<usize>,
        max_batch_records: Option<usize>,
    ) -> PyResult<Self> {
        let servers = match (cluster, urls.len()) {
            (true, _) => RedisServers::Cluster(urls),
            (false, 1) => RedisServers::Standalone(urls.remove(0)),
            (false, _) => {
                return Err(PyValueError::new_err(
                    "url must be one URL, or the URLs of a Redis Cluster with cluster=True",
                ));
            }
        };
        let mut options = RedisStoreOptions::new(servers);
        options.producer_idle_ttl = producer_idle_ttl;
        if fence {
            options.fence = Fence::On;
        }
        if let Some(key_prefix) = key_prefix {
            options.key_prefix = key_prefix;
        }
        if let Some(retention) = retention {
            options.retention = retention;
        }
        if let Some(resend_window) = resend_window {
            options.resend_window = resend_window;
        }
        if let Some(response_timeout) = response_timeout {
            options.response_timeout = response_timeout;
        }
        if let Some(max_blocking_connections) = max_blocking_connections {
            options.max_blocking_connections = max_blocking_connections;
        }
        options.wait = match (wait_replicas, wait_timeout) {
            (None, None) => None,
            (None, Some(_)) => {
                return Err(PyValueError::new_err("wait_timeout needs wait_replicas"));
            }
            (Some(replicas), None) => Some(WaitForReplicas::new(replicas)),
            (Some(replicas), Some(timeout)) => Some(WaitForReplicas { replicas, timeout }),
        };
        let mut limits = StoreLimits::default();
        if let Some(bytes) = max_record_bytes {
            limits = limits.with_max_record_bytes(bytes).map_err(invalid)?;
        }
        if let Some(bytes) = max_batch_bytes {
            limits = limits.with_max_batch_bytes(bytes).map_err(invalid)?;
        }
        if let Some(records) = max_batch_records {
            limits = limits.with_max_batch_records(records).map_err(invalid)?;
        }
        let store = temporalio_streams_redis::RedisStore::new(options).map_err(invalid)?;
        Ok(Self {
            runtime: Arc::new(StreamRuntime::new(Arc::new(store), limits)),
        })
    }

    fn create_stream<'py>(&self, py: Python<'py>, request: &[u8]) -> PyResult<Bound<'py, PyAny>> {
        self.call(py, request, |runtime, request| async move {
            runtime.create_stream(request).await
        })
    }

    fn append_records<'py>(&self, py: Python<'py>, request: &[u8]) -> PyResult<Bound<'py, PyAny>> {
        self.call(py, request, |runtime, request| async move {
            runtime.append_records(request).await
        })
    }

    fn read_records<'py>(&self, py: Python<'py>, request: &[u8]) -> PyResult<Bound<'py, PyAny>> {
        self.call(py, request, |runtime, request| async move {
            runtime.read_records(request).await
        })
    }

    fn close_stream<'py>(&self, py: Python<'py>, request: &[u8]) -> PyResult<Bound<'py, PyAny>> {
        self.call(py, request, |runtime, request| async move {
            runtime.close_stream(request).await
        })
    }

    fn describe_stream<'py>(&self, py: Python<'py>, request: &[u8]) -> PyResult<Bound<'py, PyAny>> {
        self.call(py, request, |runtime, request| async move {
            runtime.describe_stream(request).await
        })
    }
}

impl RedisStore {
    /// Runs one operation on an encoded request, and returns an awaitable of the encoded
    /// response. A failed call raises `temporalio.exceptions.StreamError`. A cancel of the
    /// awaitable drops the call, which closes the connection of a read that waits.
    fn call<'py, Request, Response, R, Operation, Call>(
        &self,
        py: Python<'py>,
        request: &[u8],
        operation: Operation,
    ) -> PyResult<Bound<'py, PyAny>>
    where
        Request: Message + Default,
        Response: Message,
        R: Into<StreamErrorReason>,
        Operation: FnOnce(Arc<StreamRuntime>, Request) -> Call,
        Call: Future<Output = Result<Response, StreamError<R>>> + Send + 'static,
    {
        start_tokio()?;
        let request = Request::decode(request).map_err(|error| {
            PyValueError::new_err(format!("The request is not its proto message: {error}"))
        })?;
        let call = operation(self.runtime.clone(), request);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let response = call.await.map_err(stream_error)?;
            Ok(Python::attach(|py| {
                PyBytes::new(py, &response.encode_to_vec()).unbind()
            }))
        })
    }
}

#[pymodule]
#[pyo3(name = "_native")]
fn native(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<RedisStore>()
}

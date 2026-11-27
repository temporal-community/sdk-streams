// A panic of a call reaches Python as a `RustPanic`, which no caller of a Store expects, so the
// module returns errors.
#![warn(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::cast_possible_truncation
)]

//! Python calls use the shared Stream runtime so request checks match Workflow calls.

use prost::Message;
use pyo3::{
    exceptions::{PyRuntimeError, PyValueError},
    prelude::*,
    types::{PyBytes, PyDict},
};
use std::{
    future::Future,
    num::NonZeroUsize,
    sync::{Arc, OnceLock},
    time::Duration,
};
use temporalio_streams::{
    CreateError, StreamError, StreamRuntime,
    proto::{
        AppendRecordsRequest, CreateStreamRequest, CreateStreamResponse, DescribeStreamRequest,
        ReadRecordsRequest, StreamErrorReason, sdk,
    },
    record::StoreLimits,
};
use temporalio_streams_redis::{
    ClusterNodeUrl, Fence, KeyPrefix, RedisServers, RedisStoreError, RedisStoreOptions, RedisUrl,
    WaitForReplicas,
};
use tokio::runtime::{Builder, Runtime};

static TOKIO: OnceLock<Runtime> = OnceLock::new();

static STARTED_BY: OnceLock<Result<u32, String>> = OnceLock::new();

// A forked child has no runtime threads, so using the inherited runtime would hang.
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

fn stream_error<R: Into<StreamErrorReason>>(error: StreamError<R>) -> PyErr {
    let reason: StreamErrorReason = error.reason.into();
    Python::attach(|py| {
        let raised = || -> PyResult<PyErr> {
            let class = py.import("temporalio.exceptions")?.getattr("StreamError")?;
            let kwargs = PyDict::new(py);
            kwargs.set_item("reason", i32::from(reason))?;
            Ok(PyErr::from_value(
                class.call((error.message,), Some(&kwargs))?,
            ))
        };
        raised().unwrap_or_else(|import_error| import_error)
    })
}

fn invalid(error: RedisStoreError) -> PyErr {
    PyValueError::new_err(error.to_string())
}

// `None` preserves the crate default so both APIs use the same values.
#[derive(FromPyObject)]
#[pyo3(from_item_all)]
struct Options {
    urls: Vec<String>,
    cluster: bool,
    key_prefix: Option<String>,
    retention: Option<Duration>,
    fence: bool,
    resend_window: Option<usize>,
    response_timeout: Option<Duration>,
    wait_replicas: Option<usize>,
    wait_timeout: Option<Duration>,
    max_blocking_connections: Option<usize>,
}

impl TryFrom<Options> for RedisStoreOptions {
    type Error = PyErr;

    fn try_from(options: Options) -> PyResult<Self> {
        let mut urls = options.urls.iter().map(|url| url.parse::<RedisUrl>());
        let first = urls.next().transpose().map_err(invalid)?;
        let first = first.ok_or_else(|| PyValueError::new_err("url must have one URL or more"))?;
        let rest = urls.collect::<Result<Vec<_>, _>>().map_err(invalid)?;
        let servers = match (options.cluster, rest.is_empty()) {
            (true, _) => {
                let node = |url| ClusterNodeUrl::try_from(url).map_err(invalid);
                RedisServers::Cluster {
                    first: node(first)?,
                    rest: rest.into_iter().map(node).collect::<PyResult<_>>()?,
                }
            }
            (false, true) => RedisServers::Standalone(first),
            (false, false) => {
                return Err(PyValueError::new_err(
                    "url must be one URL, or the URLs of a Redis Cluster with cluster=True",
                ));
            }
        };
        // A zero is refused, so that it never reads as "use the default".
        let positive = |option: &str, count: Option<usize>| match count.map(NonZeroUsize::new) {
            Some(None) => Err(PyValueError::new_err(format!("{option} must be 1 or more"))),
            Some(count) => Ok(count),
            None => Ok(None),
        };
        let wait = match (
            positive("wait_replicas", options.wait_replicas)?,
            options.wait_timeout,
        ) {
            (None, None) => None,
            (None, Some(_)) => {
                return Err(PyValueError::new_err("wait_timeout needs wait_replicas"));
            }
            (Some(replicas), timeout) => {
                let mut wait = WaitForReplicas::new(replicas);
                wait.timeout = timeout.unwrap_or(wait.timeout);
                Some(wait)
            }
        };
        let key_prefix = options.key_prefix.map(KeyPrefix::try_from).transpose();
        let window = positive("resend_window", options.resend_window)?;
        let connections = positive("max_blocking_connections", options.max_blocking_connections)?;
        let mut built = RedisStoreOptions::new(servers);
        built.key_prefix = key_prefix.map_err(invalid)?.unwrap_or(built.key_prefix);
        built.retention = options.retention.unwrap_or(built.retention);
        built.fence = match options.fence {
            true => Fence::On,
            false => Fence::Off,
        };
        built.resend_window = window.unwrap_or(built.resend_window);
        built.response_timeout = options.response_timeout.unwrap_or(built.response_timeout);
        built.wait = wait;
        built.max_blocking_connections = connections.unwrap_or(built.max_blocking_connections);
        Ok(built)
    }
}

#[pyclass(frozen)]
struct RedisStore {
    runtime: Arc<StreamRuntime>,
}

#[pymethods]
impl RedisStore {
    #[new]
    fn new(options: Options) -> PyResult<Self> {
        let store = temporalio_streams_redis::RedisStore::new(options.try_into()?);
        // The SDK checks its configured batch limits before calling this bridge.
        let limits = StoreLimits::UNBOUNDED;
        Ok(Self {
            runtime: Arc::new(StreamRuntime::new(
                Arc::new(store.map_err(invalid)?),
                limits,
            )),
        })
    }

    fn create_stream<'py>(
        &self,
        py: Python<'py>,
        request: &[u8],
        request_id: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        self.call(py, request, |runtime, request| async move {
            let CreateStreamRequest {
                stream_id,
                id_conflict_policy,
            } = request;
            let request = sdk::CreateStreamRequest {
                stream_ref: stream_ref(stream_id),
                id_conflict_policy,
            };
            let created = runtime.create_stream(request, request_id).await?.created;
            Ok::<_, CreateError>(CreateStreamResponse { created })
        })
    }

    fn append_records<'py>(
        &self,
        py: Python<'py>,
        request: &[u8],
        request_id: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        self.call(py, request, |runtime, request| async move {
            let AppendRecordsRequest {
                stream_id,
                producer,
                records,
            } = request;
            let request = sdk::AppendRecordsRequest {
                r#ref: stream_ref(stream_id),
                producer,
                records,
            };
            runtime.append_records(request, request_id).await
        })
    }

    fn read_records<'py>(&self, py: Python<'py>, request: &[u8]) -> PyResult<Bound<'py, PyAny>> {
        self.call(py, request, |runtime, request| async move {
            let ReadRecordsRequest {
                stream_id,
                from_offset,
                max_records,
                max_bytes,
                wait,
            } = request;
            let request = sdk::ReadRecordsRequest {
                r#ref: stream_ref(stream_id),
                from_offset,
                max_records,
                max_bytes,
                wait,
            };
            runtime.read_records(request).await
        })
    }

    fn describe_stream<'py>(&self, py: Python<'py>, request: &[u8]) -> PyResult<Bound<'py, PyAny>> {
        self.call(py, request, |runtime, request| async move {
            let DescribeStreamRequest { stream_id } = request;
            let request = sdk::DescribeStreamRequest {
                r#ref: stream_ref(stream_id),
            };
            runtime.describe_stream(request).await
        })
    }
}

// Routing is complete, so the runtime reference needs no Store name.
fn stream_ref(stream_id: String) -> Option<sdk::StreamRef> {
    Some(sdk::StreamRef {
        id: stream_id,
        store: String::new(),
    })
}

impl RedisStore {
    // Cancelling this future drops the Store call and releases its blocked connection.
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
            // Encode outside the GIL so Python can continue while Rust copies the response.
            let response = call.await.map_err(stream_error)?.encode_to_vec();
            Ok(Python::attach(|py| PyBytes::new(py, &response).unbind()))
        })
    }
}

#[pymodule]
#[pyo3(name = "_native")]
fn native(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<RedisStore>()
}

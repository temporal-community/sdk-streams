//! What the native modules of the Python Stream Store packages share. A package gives the names of
//! its threads and how to build its Stream runtime. This crate runs the Tokio runtime, guards
//! against a fork, and turns one Stream operation into a Python awaitable that gives the decoded
//! response or raises `temporalio.exceptions.StreamError`.

use pyo3::{
    exceptions::{PyRuntimeError, PyValueError},
    prelude::*,
    sync::PyOnceLock,
    types::{PyBytes, PyDict},
};
use std::{
    sync::{Arc, OnceLock},
    time::Duration,
};
use temporalio_streams::StreamRuntime;
use tokio::runtime::{Builder, Runtime};

/// The Tokio runtime of every call. It stays up until the process ends.
static TOKIO: OnceLock<Runtime> = OnceLock::new();

/// The id of the process that started the Tokio runtime, or why it did not start.
static STARTED_BY: OnceLock<Result<u32, String>> = OnceLock::new();

/// What a failed call needs from `temporalio`. The first failure looks it up, and the lookup stays.
struct ErrorTypes {
    /// The class `temporalio.exceptions.StreamError`.
    stream_error: Py<PyAny>,
    /// The values of `temporalio.api.enums.v1.StreamErrorReason`.
    reasons: Vec<i32>,
    /// `STREAM_ERROR_REASON_UNSPECIFIED`.
    unspecified: i32,
}

static ERROR_TYPES: PyOnceLock<ErrorTypes> = PyOnceLock::new();

/// How a package says who it is, in the names of its threads and in its messages.
#[derive(Clone, Copy, Debug)]
pub struct Identity {
    /// The name of the threads of the package's Tokio runtime.
    pub threads: &'static str,
    /// The Store as a message says it, for example `Redis Stream Store`.
    pub store: &'static str,
}

/// Starts the Tokio runtime at the first call, and checks that the same process runs the call.
fn start_tokio(identity: &Identity) -> PyResult<()> {
    let store = identity.store;
    let started_by = STARTED_BY.get_or_init(|| {
        let runtime = Builder::new_multi_thread()
            .thread_name(identity.threads)
            .enable_all()
            .build()
            .map_err(|err| format!("The {store} could not start its threads: {err}"))?;
        let runtime = TOKIO.get_or_init(|| runtime);
        // `future_into_py` runs on the runtime that is set here, and builds its own otherwise.
        pyo3_async_runtimes::tokio::init_with_runtime(runtime)
            .map_err(|()| format!("The {store} started its threads twice"))?;
        Ok(std::process::id())
    });
    match started_by {
        Ok(pid) if *pid == std::process::id() => Ok(()),
        // The threads of a Tokio runtime don't exist in a child of a fork.
        Ok(pid) => Err(PyRuntimeError::new_err(format!(
            "The {store} can't be used in a child of a fork (its threads started in process \
             {pid}, and this is process {})",
            std::process::id()
        ))),
        Err(message) => Err(PyRuntimeError::new_err(message.clone())),
    }
}

/// The `StreamError` of a failed call. A reason that this SDK doesn't know gives the unspecified
/// reason.
fn stream_error(py: Python<'_>, reason: i32, message: String) -> PyErr {
    let build = || -> PyResult<PyErr> {
        let types = ERROR_TYPES.get_or_try_init(py, || -> PyResult<ErrorTypes> {
            let reasons = py
                .import("temporalio.api.enums.v1")?
                .getattr("StreamErrorReason")?;
            Ok(ErrorTypes {
                stream_error: py
                    .import("temporalio.exceptions")?
                    .getattr("StreamError")?
                    .unbind(),
                reasons: reasons.call_method0("values")?.extract()?,
                unspecified: reasons
                    .getattr("STREAM_ERROR_REASON_UNSPECIFIED")?
                    .extract()?,
            })
        })?;
        let reason = if types.reasons.contains(&reason) {
            reason
        } else {
            types.unspecified
        };
        let kwargs = PyDict::new(py);
        kwargs.set_item("reason", reason)?;
        let error = types
            .stream_error
            .bind(py)
            .call((message,), Some(&kwargs))?;
        Ok(PyErr::from_value(error))
    };
    build().unwrap_or_else(|err| err)
}

/// Reads a `timedelta` option. A negative one raises `ValueError`, since the conversion of pyo3
/// has a message of its own for it.
pub fn duration_option(
    field: &str,
    value: Option<&Bound<'_, PyAny>>,
) -> PyResult<Option<Duration>> {
    let Some(value) = value else {
        return Ok(None);
    };
    // A `timedelta` keeps its sign in `days`. Its `seconds` and `microseconds` are never negative.
    if value.getattr("days")?.extract::<i64>()? < 0 {
        return Err(PyValueError::new_err(format!(
            "{field} must not be negative"
        )));
    }
    value.extract().map(Some)
}

struct Inner {
    identity: Identity,
    build: Box<dyn Fn() -> Arc<StreamRuntime> + Send + Sync>,
    /// Built at the first call, inside the Tokio runtime, since it spawns tasks.
    streams: OnceLock<Arc<StreamRuntime>>,
}

/// The Stream runtime of one Store object, and the calls into it from Python.
pub struct StreamBridge {
    inner: Arc<Inner>,
}

impl StreamBridge {
    /// Takes the function that builds the Stream runtime. The bridge runs it once, at the first
    /// call.
    pub fn new(
        identity: Identity,
        build: impl Fn() -> Arc<StreamRuntime> + Send + Sync + 'static,
    ) -> Self {
        Self {
            inner: Arc::new(Inner {
                identity,
                build: Box::new(build),
                streams: OnceLock::new(),
            }),
        }
    }

    /// Runs one Stream operation on an encoded request. The awaitable gives the response as a
    /// message of `response_type`, and a cancel of it stops the call. A failed call raises
    /// `temporalio.exceptions.StreamError`.
    pub fn call<'py>(
        &self,
        py: Python<'py>,
        operation: String,
        request: &[u8],
        response_type: Py<PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        start_tokio(&self.inner.identity)?;
        let inner = self.inner.clone();
        let request = request.to_vec();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let streams = inner.streams.get_or_init(|| (inner.build)()).clone();
            let result = streams.call(&operation, &request).await;
            Python::attach(|py| match result {
                Ok(response) => response_type
                    .bind(py)
                    .call_method1("FromString", (PyBytes::new(py, &response),))
                    .map(Bound::unbind),
                Err(err) => Err(stream_error(py, err.reason.proto() as i32, err.message)),
            })
        })
    }
}

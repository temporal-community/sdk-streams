//! The native module of the Python package of the memory Stream Store. Python calls the five Stream
//! service operations by name, with the encoded request, and gets the encoded response.

use pyo3::{exceptions::PyValueError, prelude::*};
use temporalio_streams_memory::{Fence, MemoryStreamOptions, MemoryStreamRuntime};
use temporalio_streams_python_native::{Identity, StreamBridge, duration_option};

const IDENTITY: Identity = Identity {
    threads: "temporal-streams-memory",
    store: "memory Stream Store",
};

/// The Stream runtime on a memory Store. Each object has its own Streams.
#[pyclass(frozen)]
struct MemoryRuntime {
    bridge: StreamBridge,
}

#[pymethods]
impl MemoryRuntime {
    /// Builds nothing but the bridge. A `None` duration takes the default of the crate, and a
    /// negative one raises `ValueError`. So does a batch limit that the crate refuses.
    #[new]
    #[pyo3(signature = (
        *,
        retention,
        producer_idle_ttl,
        store_name,
        max_record_bytes,
        max_batch_bytes,
        max_batch_records,
        fence,
    ))]
    fn new(
        retention: Option<Bound<'_, PyAny>>,
        producer_idle_ttl: Option<Bound<'_, PyAny>>,
        store_name: String,
        max_record_bytes: usize,
        max_batch_bytes: usize,
        max_batch_records: usize,
        fence: bool,
    ) -> PyResult<Self> {
        // The crate re-exports no `BatchLimits`, so the default options give the value to set.
        let limits = MemoryStreamOptions::default()
            .limits
            .with_max_record_bytes(max_record_bytes)
            .and_then(|limits| limits.with_max_batch_bytes(max_batch_bytes))
            .and_then(|limits| limits.with_max_batch_records(max_batch_records))
            .map_err(|err| PyValueError::new_err(err.to_string()))?;
        let options = MemoryStreamOptions {
            retention: duration_option("retention", retention.as_ref())?,
            producer_idle_ttl: duration_option("producer_idle_ttl", producer_idle_ttl.as_ref())?,
            limits,
            fence: match fence {
                true => Fence::On,
                false => Fence::Off,
            },
        };
        // The runtime is built at the first call, inside the Tokio runtime, since it spawns tasks.
        let bridge = StreamBridge::new(IDENTITY, move || {
            MemoryStreamRuntime::create(store_name.clone(), options.clone(), None)
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

// The Python module is `_native`, and the library has a name of its own so that the two native
// modules of this workspace build to different files.
#[pymodule]
#[pyo3(name = "_native")]
fn native(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<MemoryRuntime>()?;
    Ok(())
}

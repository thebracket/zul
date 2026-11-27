//! Python-facing bridge from the Hermes Zulip adapter to `libzul`.
//!
//! Every method blocks the calling Python thread but releases the GIL while it
//! waits, so other Python threads keep running during Zulip long-polls.

use std::time::Duration;

use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};
use pyo3::{IntoPyObjectExt, Py, PyAny, PyResult};
use serde_json::Value;

mod bridge;

use bridge::BridgeCore;

/// Blocking Zulip bridge backed by one dedicated tokio runtime thread.
#[pyclass(module = "zul_bridge")]
struct Bridge {
    core: BridgeCore,
}

#[pymethods]
impl Bridge {
    /// Build a bridge. Explicit arguments override libzul's saved config and
    /// environment before any network request is attempted.
    // The Python-facing constructor mirrors every optional queue-shaping knob.
    #[new]
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (
        base_url = "",
        email = "",
        api_key = "",
        *,
        event_types = None,
        narrow = None,
        all_public_streams = false,
        queue_timeout_secs = None
    ))]
    fn new(
        py: Python<'_>,
        base_url: &str,
        email: &str,
        api_key: &str,
        event_types: Option<Vec<String>>,
        narrow: Option<Vec<Vec<String>>>,
        all_public_streams: bool,
        queue_timeout_secs: Option<u64>,
    ) -> PyResult<Self> {
        let config = py
            .detach(|| {
                bridge::resolve_config(
                    base_url,
                    email,
                    api_key,
                    event_types,
                    narrow,
                    all_public_streams,
                    queue_timeout_secs,
                )
            })
            .map_err(PyValueError::new_err)?;
        let core = BridgeCore::new(config).map_err(PyRuntimeError::new_err)?;
        Ok(Self { core })
    }

    /// Return the authenticated account as `{"email", "full_name"}`.
    fn whoami(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let account = py
            .detach(|| self.core.whoami())
            .map_err(PyRuntimeError::new_err)?;
        json_to_py(py, &account)
    }

    /// Send a stream message and return its Zulip message ID.
    fn send_stream(
        &self,
        py: Python<'_>,
        stream: &str,
        topic: &str,
        content: &str,
    ) -> PyResult<i64> {
        py.detach(|| self.core.send_stream(stream, topic, content))
            .map_err(PyRuntimeError::new_err)
    }

    /// Send a direct message to Zulip user IDs and return its message ID.
    fn send_direct_ids(
        &self,
        py: Python<'_>,
        recipients: Vec<i64>,
        content: &str,
    ) -> PyResult<i64> {
        py.detach(|| self.core.send_direct_ids(&recipients, content))
            .map_err(PyRuntimeError::new_err)
    }

    /// Send a direct message to email addresses and return its message ID.
    fn send_direct_emails(
        &self,
        py: Python<'_>,
        recipients: Vec<String>,
        content: &str,
    ) -> PyResult<i64> {
        py.detach(|| self.core.send_direct_emails(&recipients, content))
            .map_err(PyRuntimeError::new_err)
    }

    /// Send a typing notification for a stream compose box.
    fn typing_stream(&self, py: Python<'_>, stream_id: i64, topic: &str, op: &str) -> PyResult<()> {
        py.detach(|| self.core.typing_stream(stream_id, topic, op))
            .map_err(PyRuntimeError::new_err)
    }

    /// Send a typing notification for a direct-message compose box.
    fn typing_direct(&self, py: Python<'_>, recipients: Vec<i64>, op: &str) -> PyResult<()> {
        py.detach(|| self.core.typing_direct(&recipients, op))
            .map_err(PyRuntimeError::new_err)
    }

    /// Wait up to `timeout_ms` for the next message event.
    ///
    /// Returns `None` on timeout. Fatal poller failures raise `RuntimeError`.
    fn next_event(&self, py: Python<'_>, timeout_ms: i64) -> PyResult<Option<Py<PyAny>>> {
        let timeout = u64::try_from(timeout_ms)
            .map_err(|_| PyValueError::new_err("timeout_ms must be non-negative"))?;
        let event = py
            .detach(|| self.core.next_event(Duration::from_millis(timeout)))
            .map_err(PyRuntimeError::new_err)?;
        event
            .as_ref()
            .map(|event| json_to_py(py, event))
            .transpose()
    }

    /// Stop the poller, delete the event queue, and join the runtime thread.
    /// Safe to call more than once.
    fn close(&self, py: Python<'_>) {
        py.detach(|| self.core.close());
    }
}

/// Report whether libzul finds a base URL, email, and API key. Values are
/// never included.
#[pyfunction]
fn config_status(py: Python<'_>) -> PyResult<Py<PyAny>> {
    let status = py.detach(bridge::config_status);
    json_to_py(py, &status)
}

fn json_to_py(py: Python<'_>, value: &Value) -> PyResult<Py<PyAny>> {
    match value {
        Value::Null => Ok(py.None()),
        Value::Bool(boolean) => boolean.into_py_any(py),
        Value::Number(number) => {
            if let Some(integer) = number.as_i64() {
                integer.into_py_any(py)
            } else if let Some(integer) = number.as_u64() {
                integer.into_py_any(py)
            } else {
                number.as_f64().unwrap_or_default().into_py_any(py)
            }
        }
        Value::String(string) => string.into_py_any(py),
        Value::Array(items) => {
            let list = PyList::empty(py);
            for item in items {
                list.append(json_to_py(py, item)?)?;
            }
            Ok(list.into_any().unbind())
        }
        Value::Object(entries) => {
            let dict = PyDict::new(py);
            for (key, item) in entries {
                dict.set_item(key.as_str(), json_to_py(py, item)?)?;
            }
            Ok(dict.into_any().unbind())
        }
    }
}

/// Extension module entry point.
#[pymodule]
fn zul_bridge(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<Bridge>()?;
    module.add_function(wrap_pyfunction!(config_status, module)?)?;
    module.add("__version__", env!("CARGO_PKG_VERSION"))?;
    Ok(())
}

#[cfg(test)]
mod tests;

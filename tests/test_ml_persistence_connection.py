"""Tests for SQLite connection lifecycle in TrainingDataStore.

Verifies that:
1. _get_connection() yields a working sqlite3.Connection via context manager.
2. Connections are reliably closed on normal exit and on exception.
3. Exceptions propagate through the context manager without being swallowed.
4. Public methods (store_log_sample, store_failure_label, etc.) work end-to-end.
5. Only _get_connection's finally block contains conn.close() (no leak sites).
"""

import importlib.util
import sqlite3
import sys
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import patch

import pytest

# ---------------------------------------------------------------------------
# Direct-import of ml_persistence.py (avoids heavy helpers/__init__.py)
# ---------------------------------------------------------------------------
SRC_DIR = Path(__file__).resolve().parent.parent / "src"
sys.path.insert(0, str(SRC_DIR))

_ML_PERSISTENCE_PATH = SRC_DIR / "helpers" / "ml_persistence.py"
try:
    _spec = importlib.util.spec_from_file_location(
        "helpers.ml_persistence", _ML_PERSISTENCE_PATH
    )
    _mod = importlib.util.module_from_spec(_spec)
    _spec.loader.exec_module(_mod)
    TrainingDataStore = _mod.TrainingDataStore
except (ImportError, ModuleNotFoundError, FileNotFoundError) as _imp_err:
    pytest.skip(
        f"Cannot import TrainingDataStore: {_imp_err}",
        allow_module_level=True,
    )


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def store(tmp_path: Path) -> TrainingDataStore:
    """Create a TrainingDataStore backed by a temp directory.

    Patches get_current_cluster_id so tests run without kubeconfig.
    """
    with patch.object(_mod, "get_current_cluster_id", return_value="test-cluster"):
        return TrainingDataStore(storage_dir=str(tmp_path))


# ---------------------------------------------------------------------------
# _get_connection context-manager behaviour
# ---------------------------------------------------------------------------


def test_yields_live_sqlite_connection(store):
    """_get_connection() yields a working sqlite3.Connection."""
    with store._get_connection() as conn:
        assert isinstance(conn, sqlite3.Connection)
        result = conn.execute("SELECT 1").fetchone()
        assert result == (1,)


def test_closes_connection_on_normal_exit(store):
    """Connection is closed after the with-block exits normally."""
    with store._get_connection() as conn:
        pass  # normal exit

    # After closing, any operation should raise ProgrammingError
    with pytest.raises(sqlite3.ProgrammingError):
        conn.execute("SELECT 1")


def test_closes_connection_on_exception(store):
    """Connection is closed even when the with-block raises."""
    with pytest.raises(RuntimeError):
        with store._get_connection() as conn:
            raise RuntimeError("deliberate")

    # Connection must still be closed
    with pytest.raises(sqlite3.ProgrammingError):
        conn.execute("SELECT 1")


def test_exception_propagates_through_context_manager(store):
    """Exceptions inside the with-block are NOT swallowed."""
    with pytest.raises(ValueError, match="propagated"):
        with store._get_connection() as conn:
            raise ValueError("propagated")


def test_get_connection_returns_context_manager(store):
    """The object returned by _get_connection() supports the context-manager protocol."""
    cm = store._get_connection()
    assert hasattr(cm, "__enter__"), "_get_connection result must have __enter__"
    assert hasattr(cm, "__exit__"), "_get_connection result must have __exit__"


# ---------------------------------------------------------------------------
# Public method roundtrip tests
# ---------------------------------------------------------------------------


def test_store_log_sample_roundtrip(store):
    """store_log_sample returns a non-None integer row ID."""
    with patch.object(_mod, "get_current_cluster_id", return_value="test-cluster"):
        sample = {
            "timestamp": datetime.now().isoformat(),
            "namespace": "test-ns",
            "pod_name": "test-pod-abc",
            "raw_message": "ERROR something went wrong",
            "log_level": "ERROR",
            "error_indicators": 1,
            "message_entropy": 3.14,
        }
        row_id = store.store_log_sample(sample)
        assert row_id is not None
        assert isinstance(row_id, int)


def test_store_failure_label_roundtrip(store):
    """store_failure_label returns a non-None integer row ID."""
    with patch.object(_mod, "get_current_cluster_id", return_value="test-cluster"):
        label = {
            "failure_type": "crash",
            "severity": "high",
            "namespace": "test-ns",
            "resource_name": "test-pod-abc",
            "resource_type": "pod",
            "failure_time": datetime.now().isoformat(),
            "detection_source": "test",
            "error_category": "crash",
        }
        row_id = store.store_failure_label(label)
        assert row_id is not None
        assert isinstance(row_id, int)


def test_get_statistics_returns_expected_keys(store):
    """get_statistics() returns a dict with the essential keys."""
    with patch.object(_mod, "get_current_cluster_id", return_value="test-cluster"):
        stats = store.get_statistics()
        assert isinstance(stats, dict)
        for key in ("total_log_samples", "total_failure_labels", "total_training_runs"):
            assert key in stats, f"Missing key: {key}"


def test_get_training_data_returns_tuple(store):
    """get_training_data() returns a (list, int) tuple."""
    result = store.get_training_data()
    assert isinstance(result, tuple)
    assert len(result) == 2
    samples, count = result
    assert isinstance(samples, list)
    assert isinstance(count, int)


def test_record_training_run_returns_positive_id(store):
    """record_training_run returns a positive integer run ID."""
    run_id = store.record_training_run(
        model_id="model-v1",
        samples_used=100,
        labels_used=50,
        performance_metrics={"accuracy": 0.95},
        trigger_reason="test",
    )
    assert isinstance(run_id, int)
    assert run_id > 0


def test_cleanup_old_data_returns_int(store):
    """cleanup_old_data returns an int >= 0."""
    deleted = store.cleanup_old_data(max_age_days=0)
    assert isinstance(deleted, int)
    assert deleted >= 0


def test_get_failure_labels_in_window_returns_list(store):
    """get_failure_labels_in_window returns a list."""
    with patch.object(_mod, "get_current_cluster_id", return_value="test-cluster"):
        now = datetime.now()
        result = store.get_failure_labels_in_window(
            start_time=now - timedelta(hours=1),
            end_time=now,
        )
        assert isinstance(result, list)


# ---------------------------------------------------------------------------
# Source-level invariant: single conn.close() in the class
# ---------------------------------------------------------------------------


def test_single_conn_close_in_class_source():
    """Only _get_connection's finally block should contain conn.close().

    After the refactor, every public method delegates closing to
    _get_connection's context manager, so the literal string 'conn.close()'
    should appear exactly once in the TrainingDataStore source.
    """
    # Read the source file directly because inspect.getsource() cannot
    # resolve modules loaded via importlib.util.spec_from_file_location.
    full_source = _ML_PERSISTENCE_PATH.read_text()

    # Extract only the TrainingDataStore class body (from 'class TrainingDataStore'
    # to the next top-level class or end of file).
    class_start = full_source.find("class TrainingDataStore")
    assert class_start != -1, "Could not find TrainingDataStore in source"

    # Find the next top-level class definition after TrainingDataStore
    rest = full_source[class_start + len("class TrainingDataStore"):]
    next_class = rest.find("\nclass ")
    if next_class != -1:
        class_source = full_source[class_start:class_start + len("class TrainingDataStore") + next_class]
    else:
        class_source = full_source[class_start:]

    count = class_source.count("conn.close()")
    assert count == 1, (
        f"Expected exactly 1 occurrence of conn.close() in TrainingDataStore "
        f"(inside _get_connection's finally block), but found {count}"
    )

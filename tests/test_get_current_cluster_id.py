"""Tests for get_current_cluster_id() debug logging in exception handlers.

Verifies that:
1. The function returns the cluster name from kubeconfig when available.
2. The first except block logs a debug message on kubeconfig failure.
3. The second except block logs a debug message on in-cluster config failure
   (instead of silently swallowing the exception with a bare `pass`).
4. Both paths failing returns 'unknown' with two debug log calls.
5. The in-cluster fallback works correctly via KUBERNETES_SERVICE_HOST.
6. Empty KUBERNETES_SERVICE_HOST falls through to 'unknown'.
7. No bare `pass` statements remain in the function body (AC7).

Uses the same importlib.util.spec_from_file_location pattern established in
test_secure_model_deserialization.py to avoid pulling in heavy dependencies
(kubernetes, pyyaml, etc.) through the helpers package __init__.py.
"""

import importlib.util
import inspect
import re
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

# Add src/ to the path so we can import the module under test.
SRC_DIR = Path(__file__).resolve().parent.parent / "src"
sys.path.insert(0, str(SRC_DIR))

# Load ml_persistence.py *directly* via importlib so that the helpers package
# __init__.py (which eagerly re-exports every submodule and pulls in heavy
# dependencies like pyyaml, kubernetes, etc.) is never executed.
_ML_PERSISTENCE_PATH = SRC_DIR / "helpers" / "ml_persistence.py"
try:
    _spec = importlib.util.spec_from_file_location(
        "helpers.ml_persistence", _ML_PERSISTENCE_PATH
    )
    _mod = importlib.util.module_from_spec(_spec)
    _spec.loader.exec_module(_mod)
    get_current_cluster_id = _mod.get_current_cluster_id
    logger = _mod.logger
except (ImportError, ModuleNotFoundError, FileNotFoundError) as _imp_err:
    pytest.skip(
        f"Cannot import get_current_cluster_id: {_imp_err}",
        allow_module_level=True,
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestGetCurrentClusterId:
    """Tests for get_current_cluster_id() behavior and logging."""

    def test_returns_cluster_from_kubeconfig_context(self) -> None:
        """Mock kubernetes.config.list_kube_config_contexts to return a valid
        context dict. Assert the function returns the cluster name from
        context and does NOT call logger.debug.
        """
        mock_current = {
            "context": {"cluster": "api-stone-prod-p02-hjvn-p1-openshiftapps-com:6443"}
        }
        mock_contexts = [mock_current]

        # The function does `from kubernetes import config` which resolves
        # via sys.modules["kubernetes"].config -- so we set up the mock on
        # the kubernetes module's config attribute, not as a separate entry.
        mock_k8s = MagicMock()
        mock_k8s.config.list_kube_config_contexts.return_value = (
            mock_contexts,
            mock_current,
        )

        with patch.dict(
            "sys.modules",
            {"kubernetes": mock_k8s, "kubernetes.config": mock_k8s.config},
        ):
            with patch.object(logger, "debug") as mock_debug:
                result = get_current_cluster_id()

            assert result == "api-stone-prod-p02-hjvn-p1-openshiftapps-com:6443"
            mock_debug.assert_not_called()

    def test_first_except_logs_kubeconfig_failure(self) -> None:
        """Mock kubernetes.config import to raise ImportError. Patch
        KUBERNETES_SERVICE_HOST to a value so the in-cluster fallback
        succeeds. Assert logger.debug was called once with a message
        containing 'Could not get cluster ID from kubeconfig'.
        """
        # Make the kubernetes import raise ImportError in the first try block
        with patch.dict("sys.modules", {"kubernetes": None, "kubernetes.config": None}):
            with patch.dict("os.environ", {"KUBERNETES_SERVICE_HOST": "10.0.0.1"}):
                with patch.object(logger, "debug") as mock_debug:
                    result = get_current_cluster_id()

        assert result == "in-cluster-10.0.0.1"
        mock_debug.assert_called_once()
        log_msg = mock_debug.call_args[0][0]
        assert "Could not get cluster ID from kubeconfig" in log_msg

    def test_second_except_logs_in_cluster_failure(self) -> None:
        """Mock kubernetes.config import to raise so the first try fails,
        then mock os.environ.get to raise an exception in the second try
        block. Assert logger.debug was called with a message containing
        'Could not get cluster ID from in-cluster config' AND that the
        exception object is included in the message.
        """
        # Make the kubernetes import raise ImportError in the first try block
        with patch.dict("sys.modules", {"kubernetes": None, "kubernetes.config": None}):
            # We need to make os.environ.get raise inside the function.
            # The function does `import os` then `os.environ.get(...)`.
            # We patch os.environ.get to raise.
            with patch("os.environ.get", side_effect=RuntimeError("env access denied")):
                with patch.object(logger, "debug") as mock_debug:
                    result = get_current_cluster_id()

        assert result == "unknown"
        # The second call should contain the in-cluster config message
        calls = mock_debug.call_args_list
        assert len(calls) >= 2, (
            f"Expected at least 2 logger.debug calls (one for kubeconfig failure, "
            f"one for in-cluster failure), got {len(calls)}"
        )
        second_call_msg = calls[1][0][0]
        assert "Could not get cluster ID from in-cluster config" in second_call_msg
        # The exception object should be included in the message
        assert "env access denied" in second_call_msg

    def test_returns_unknown_when_both_paths_fail(self) -> None:
        """Mock both try blocks to raise exceptions. Assert function returns
        'unknown'. Assert logger.debug was called exactly twice -- once for
        each except block.
        """
        with patch.dict("sys.modules", {"kubernetes": None, "kubernetes.config": None}):
            with patch("os.environ.get", side_effect=RuntimeError("env broken")):
                with patch.object(logger, "debug") as mock_debug:
                    result = get_current_cluster_id()

        assert result == "unknown"
        assert mock_debug.call_count == 2, (
            f"Expected exactly 2 logger.debug calls, got {mock_debug.call_count}. "
            f"Calls: {mock_debug.call_args_list}"
        )

    def test_returns_in_cluster_format_when_env_var_set(self) -> None:
        """Mock the first try block to raise, set
        KUBERNETES_SERVICE_HOST='10.0.0.1'. Assert function returns
        'in-cluster-10.0.0.1'.
        """
        with patch.dict("sys.modules", {"kubernetes": None, "kubernetes.config": None}):
            with patch.dict("os.environ", {"KUBERNETES_SERVICE_HOST": "10.0.0.1"}):
                result = get_current_cluster_id()

        assert result == "in-cluster-10.0.0.1"

    def test_returns_unknown_when_env_var_empty(self) -> None:
        """Mock the first try block to raise, set KUBERNETES_SERVICE_HOST=''.
        Assert function returns 'unknown' (falls through both blocks without
        exception).
        """
        with patch.dict("sys.modules", {"kubernetes": None, "kubernetes.config": None}):
            with patch.dict("os.environ", {"KUBERNETES_SERVICE_HOST": ""}, clear=False):
                result = get_current_cluster_id()

        assert result == "unknown"

    def test_no_bare_pass_in_function(self) -> None:
        """Read the source of get_current_cluster_id using
        inspect.getsource(). Assert that the regex pattern r'^\\s+pass\\s*$'
        does NOT match any line in the function body (AC7 verification).
        """
        source = inspect.getsource(get_current_cluster_id)
        bare_pass_pattern = re.compile(r"^\s+pass\s*$", re.MULTILINE)
        matches = bare_pass_pattern.findall(source)
        assert len(matches) == 0, (
            f"Found {len(matches)} bare 'pass' statement(s) in "
            f"get_current_cluster_id():\n{source}"
        )

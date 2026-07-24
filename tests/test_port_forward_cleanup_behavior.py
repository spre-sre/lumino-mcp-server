"""
Behavioral tests for port-forward subprocess cleanup in KubeArchiveEndpointDiscovery.

Verifies that:
1. Context manager calls stop_port_forward on exit (normal and exception).
2. atexit handler is registered/unregistered correctly.
3. Signal handlers are installed, chain to originals, and are restored.
4. stop_port_forward is idempotent (safe to call multiple times).
5. _register_cleanup_handlers is idempotent (only registers once).
"""

import atexit
import importlib.util
import os
import signal
import sys
from pathlib import Path
from types import ModuleType
from unittest.mock import MagicMock, patch, call

import pytest

# ---------------------------------------------------------------------------
# Direct-import of kubearchive_integration.py, mocking kubernetes at load time
# ---------------------------------------------------------------------------
SRC_DIR = Path(__file__).resolve().parent.parent / "src"

_KA_PATH = SRC_DIR / "helpers" / "kubearchive_integration.py"

# Create mock kubernetes modules before importing the real module
_mock_k8s = MagicMock()
_mock_k8s_client = MagicMock()
_mock_k8s.client = _mock_k8s_client
_mock_k8s_rest = MagicMock()

_patches = {
    "kubernetes": _mock_k8s,
    "kubernetes.client": _mock_k8s_client,
    "kubernetes.client.rest": _mock_k8s_rest,
    "aiohttp": MagicMock(),
}

for mod_name, mock_obj in _patches.items():
    if mod_name not in sys.modules:
        sys.modules[mod_name] = mock_obj

try:
    _spec = importlib.util.spec_from_file_location(
        "helpers.kubearchive_integration", _KA_PATH
    )
    _mod: ModuleType = importlib.util.module_from_spec(_spec)
    _spec.loader.exec_module(_mod)
    KubeArchiveEndpointDiscovery = _mod.KubeArchiveEndpointDiscovery
except (ImportError, ModuleNotFoundError, FileNotFoundError) as _imp_err:
    pytest.skip(
        f"Cannot import KubeArchiveEndpointDiscovery: {_imp_err}",
        allow_module_level=True,
    )


def _make_discovery(**kwargs):
    """Create a KubeArchiveEndpointDiscovery with mocked k8s clients."""
    return KubeArchiveEndpointDiscovery(
        k8s_core_api=MagicMock(),
        k8s_custom_api=MagicMock(),
        **kwargs,
    )


def _make_discovery_with_fake_process():
    """Create a discovery instance with a fake port-forward process attached."""
    d = _make_discovery()
    proc = MagicMock()
    proc.terminate = MagicMock()
    proc.wait = MagicMock()
    proc.kill = MagicMock()
    d._port_forward_process = proc
    d._port_forward_port = 8081
    return d, proc


# ---------------------------------------------------------------------------
# Context manager tests
# ---------------------------------------------------------------------------

class TestContextManagerBehavior:

    def test_enter_returns_same_instance(self):
        d = _make_discovery()
        result = d.__enter__()
        assert result is d

    def test_exit_calls_stop_port_forward(self):
        d = _make_discovery()
        d.stop_port_forward = MagicMock()
        d.__exit__(None, None, None)
        d.stop_port_forward.assert_called_once()

    def test_with_statement_calls_cleanup(self):
        d = _make_discovery()
        d.stop_port_forward = MagicMock()
        with d:
            pass
        d.stop_port_forward.assert_called_once()

    def test_with_statement_calls_cleanup_on_exception(self):
        d = _make_discovery()
        d.stop_port_forward = MagicMock()
        with pytest.raises(ValueError):
            with d:
                raise ValueError("test error")
        d.stop_port_forward.assert_called_once()

    def test_exit_does_not_suppress_exceptions(self):
        d = _make_discovery()
        result = d.__exit__(ValueError, ValueError("x"), None)
        assert result is False


# ---------------------------------------------------------------------------
# stop_port_forward tests
# ---------------------------------------------------------------------------

class TestStopPortForward:

    def test_terminates_running_process(self):
        d, proc = _make_discovery_with_fake_process()
        d.stop_port_forward()
        proc.terminate.assert_called_once()
        proc.wait.assert_called_once_with(timeout=5)

    def test_clears_process_and_port(self):
        d, _ = _make_discovery_with_fake_process()
        d.stop_port_forward()
        assert d._port_forward_process is None
        assert d._port_forward_port is None

    def test_idempotent_no_process(self):
        d = _make_discovery()
        assert d._port_forward_process is None
        d.stop_port_forward()
        d.stop_port_forward()

    def test_kills_process_on_terminate_timeout(self):
        d, proc = _make_discovery_with_fake_process()
        proc.wait.side_effect = Exception("timeout")
        d.stop_port_forward()
        proc.kill.assert_called_once()

    def test_unregisters_cleanup_handlers(self):
        d, _ = _make_discovery_with_fake_process()
        d._unregister_cleanup_handlers = MagicMock()
        d.stop_port_forward()
        d._unregister_cleanup_handlers.assert_called_once()


# ---------------------------------------------------------------------------
# atexit handler tests
# ---------------------------------------------------------------------------

class TestAtexitRegistration:

    def test_register_sets_flag(self):
        d = _make_discovery()
        assert d._atexit_registered is False
        with patch("atexit.register") as mock_reg:
            d._register_cleanup_handlers()
        assert d._atexit_registered is True
        mock_reg.assert_called_once_with(d.stop_port_forward)

    def test_register_is_idempotent(self):
        d = _make_discovery()
        with patch("atexit.register") as mock_reg:
            d._register_cleanup_handlers()
            d._register_cleanup_handlers()
        mock_reg.assert_called_once()

    def test_unregister_clears_flag(self):
        d = _make_discovery()
        d._atexit_registered = True
        with patch("atexit.unregister") as mock_unreg:
            d._unregister_cleanup_handlers()
        assert d._atexit_registered is False
        mock_unreg.assert_called_once_with(d.stop_port_forward)

    def test_unregister_noop_when_not_registered(self):
        d = _make_discovery()
        assert d._atexit_registered is False
        with patch("atexit.unregister") as mock_unreg:
            d._unregister_cleanup_handlers()
        mock_unreg.assert_not_called()


# ---------------------------------------------------------------------------
# Signal handler tests
# ---------------------------------------------------------------------------

class TestSignalHandlerRegistration:

    def test_register_installs_sigterm_and_sigint(self):
        d = _make_discovery()
        original_term = signal.getsignal(signal.SIGTERM)
        original_int = signal.getsignal(signal.SIGINT)
        try:
            d._register_cleanup_handlers()
            assert d._signal_handlers_registered is True
            assert signal.getsignal(signal.SIGTERM) == d._signal_handler
            assert signal.getsignal(signal.SIGINT) == d._signal_handler
        finally:
            signal.signal(signal.SIGTERM, original_term)
            signal.signal(signal.SIGINT, original_int)

    def test_register_saves_original_handlers(self):
        d = _make_discovery()
        original_term = signal.getsignal(signal.SIGTERM)
        original_int = signal.getsignal(signal.SIGINT)
        try:
            d._register_cleanup_handlers()
            assert d._original_sigterm_handler == original_term
            assert d._original_sigint_handler == original_int
        finally:
            signal.signal(signal.SIGTERM, original_term)
            signal.signal(signal.SIGINT, original_int)

    def test_register_is_idempotent(self):
        d = _make_discovery()
        original_term = signal.getsignal(signal.SIGTERM)
        original_int = signal.getsignal(signal.SIGINT)
        try:
            d._register_cleanup_handlers()
            d._register_cleanup_handlers()
            assert d._original_sigterm_handler == original_term
            assert d._original_sigint_handler == original_int
        finally:
            signal.signal(signal.SIGTERM, original_term)
            signal.signal(signal.SIGINT, original_int)

    def test_unregister_restores_original_handlers(self):
        d = _make_discovery()
        original_term = signal.getsignal(signal.SIGTERM)
        original_int = signal.getsignal(signal.SIGINT)
        try:
            d._register_cleanup_handlers()
            d._unregister_cleanup_handlers()
            assert d._signal_handlers_registered is False
            assert signal.getsignal(signal.SIGTERM) == original_term
            assert signal.getsignal(signal.SIGINT) == original_int
        finally:
            signal.signal(signal.SIGTERM, original_term)
            signal.signal(signal.SIGINT, original_int)


class TestSignalHandlerBehavior:

    def test_signal_handler_calls_stop_port_forward(self):
        d, _ = _make_discovery_with_fake_process()
        d.stop_port_forward = MagicMock()
        d._signal_handler(signal.SIGTERM, None)
        d.stop_port_forward.assert_called_once()

    def test_signal_handler_chains_to_original_sigterm(self):
        d = _make_discovery()
        original = MagicMock()
        d._original_sigterm_handler = original
        d._signal_handler(signal.SIGTERM, None)
        original.assert_called_once_with(signal.SIGTERM, None)

    def test_signal_handler_chains_to_original_sigint(self):
        d = _make_discovery()
        original = MagicMock()
        d._original_sigint_handler = original
        d._signal_handler(signal.SIGINT, None)
        original.assert_called_once_with(signal.SIGINT, None)

    def test_signal_handler_restores_sig_dfl_and_reraises(self):
        """Test that SIG_DFL is properly restored and signal is re-raised."""
        d = _make_discovery()
        d._original_sigterm_handler = signal.SIG_DFL
        original_term = signal.getsignal(signal.SIGTERM)
        try:
            with patch("os.kill") as mock_kill:
                d._signal_handler(signal.SIGTERM, None)
                # Should restore SIG_DFL and re-raise signal
                assert signal.getsignal(signal.SIGTERM) == signal.SIG_DFL
                mock_kill.assert_called_once()
                call_args = mock_kill.call_args[0]
                assert call_args[0] == os.getpid()
                assert call_args[1] == signal.SIGTERM
        finally:
            signal.signal(signal.SIGTERM, original_term)

    def test_signal_handler_ignores_sig_ign(self):
        """Test that SIG_IGN is properly handled (ignored after cleanup)."""
        d = _make_discovery()
        d._original_sigterm_handler = signal.SIG_IGN
        with patch("signal.signal") as mock_signal:
            d._signal_handler(signal.SIGTERM, None)
            # Should not restore or re-raise for SIG_IGN
            mock_signal.assert_not_called()

    def test_signal_handler_skips_none_original(self):
        """Test that None original handler is handled gracefully."""
        d = _make_discovery()
        d._original_sigterm_handler = None
        d._signal_handler(signal.SIGTERM, None)
        # Should complete without error


# ---------------------------------------------------------------------------
# Full lifecycle integration
# ---------------------------------------------------------------------------

class TestFullCleanupLifecycle:

    def test_register_then_stop_clears_everything(self):
        d = _make_discovery()
        original_term = signal.getsignal(signal.SIGTERM)
        original_int = signal.getsignal(signal.SIGINT)
        try:
            d._register_cleanup_handlers()
            assert d._atexit_registered is True
            assert d._signal_handlers_registered is True

            d.stop_port_forward()
            assert d._atexit_registered is False
            assert d._signal_handlers_registered is False
        finally:
            signal.signal(signal.SIGTERM, original_term)
            signal.signal(signal.SIGINT, original_int)

    def test_context_manager_with_process_full_cycle(self):
        d, proc = _make_discovery_with_fake_process()
        d._register_cleanup_handlers()
        original_term = signal.getsignal(signal.SIGTERM)
        original_int = signal.getsignal(signal.SIGINT)
        try:
            with d:
                assert d._port_forward_process is proc
            assert d._port_forward_process is None
            assert d._port_forward_port is None
            proc.terminate.assert_called_once()
        finally:
            signal.signal(signal.SIGTERM, original_term)
            signal.signal(signal.SIGINT, original_int)


# ---------------------------------------------------------------------------
# Init field defaults
# ---------------------------------------------------------------------------

class TestInitDefaults:

    def test_cleanup_fields_default_to_false_or_none(self):
        d = _make_discovery()
        assert d._atexit_registered is False
        assert d._signal_handlers_registered is False
        assert d._original_sigterm_handler is None
        assert d._original_sigint_handler is None
        assert d._port_forward_process is None
        assert d._port_forward_port is None

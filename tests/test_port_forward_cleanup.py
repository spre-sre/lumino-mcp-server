"""
Behavioral tests for issue #121: Port-forward subprocess resource leak in
KubeArchiveEndpointDiscovery.

Unlike source-inspection tests, these exercise the real cleanup logic:
context manager, atexit/signal handler registration and restoration, and
signal chaining. They are written to fail on the original defects — most
importantly the blocking bug where signal.SIG_DFL (integer 0, falsy) caused
SIGTERM cleanup to run without the process ever terminating.
"""

import os
import signal
import sys
from unittest import mock

import pytest

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from src.helpers import kubearchive_integration
from src.helpers.kubearchive_integration import KubeArchiveEndpointDiscovery


def _make_discovery():
    """Build an instance with mocked K8s clients and no auto port-forward."""
    return KubeArchiveEndpointDiscovery(
        mock.MagicMock(), mock.MagicMock(), auto_port_forward=False
    )


@pytest.fixture(autouse=True)
def preserve_signal_handlers():
    """Snapshot and restore real SIGTERM/SIGINT dispositions around each test."""
    orig_term = signal.getsignal(signal.SIGTERM)
    orig_int = signal.getsignal(signal.SIGINT)
    try:
        yield
    finally:
        signal.signal(signal.SIGTERM, orig_term)
        signal.signal(signal.SIGINT, orig_int)


@pytest.fixture(autouse=True)
def patched_atexit():
    """Patch the module's atexit so tests never leak real exit handlers."""
    with mock.patch.object(kubearchive_integration, "atexit") as patched:
        yield patched


class TestModuleImport:
    """The helper module must import without side effects."""

    def test_class_importable(self):
        assert KubeArchiveEndpointDiscovery is not None

    def test_construction_does_not_start_port_forward(self):
        d = _make_discovery()
        assert d._port_forward_process is None
        assert d._atexit_registered is False
        assert d._signal_handlers_registered is False


class TestContextManager:
    """The class must behave as a context manager that cleans up on exit."""

    def test_enter_returns_self(self):
        d = _make_discovery()
        with d as ctx:
            assert ctx is d

    def test_exit_stops_running_process(self):
        d = _make_discovery()
        proc = mock.MagicMock()
        d._port_forward_process = proc
        with d:
            pass
        proc.terminate.assert_called_once()
        assert d._port_forward_process is None


class TestStopPortForward:
    """stop_port_forward must terminate the subprocess and clear state."""

    def test_terminates_and_clears_process(self):
        d = _make_discovery()
        proc = mock.MagicMock()
        d._port_forward_process = proc
        d._port_forward_port = 8081
        d.stop_port_forward()
        proc.terminate.assert_called_once()
        proc.wait.assert_called_once()
        assert d._port_forward_process is None
        assert d._port_forward_port is None

    def test_kills_when_terminate_times_out(self):
        d = _make_discovery()
        proc = mock.MagicMock()
        proc.wait.side_effect = Exception("terminate timeout")
        d._port_forward_process = proc
        d.stop_port_forward()
        proc.kill.assert_called_once()
        assert d._port_forward_process is None

    def test_noop_when_no_process(self):
        d = _make_discovery()
        # Must not raise even though no port-forward was ever started.
        d.stop_port_forward()
        assert d._port_forward_process is None


class TestAtexitHandler:
    """atexit registration must be idempotent and reversible."""

    def test_register_adds_atexit_handler(self, patched_atexit):
        d = _make_discovery()
        d._register_cleanup_handlers()
        patched_atexit.register.assert_called_once_with(d.stop_port_forward)
        assert d._atexit_registered is True

    def test_register_is_idempotent(self, patched_atexit):
        d = _make_discovery()
        d._register_cleanup_handlers()
        d._register_cleanup_handlers()
        assert patched_atexit.register.call_count == 1

    def test_stop_unregisters_atexit(self, patched_atexit):
        d = _make_discovery()
        d._register_cleanup_handlers()
        d.stop_port_forward()
        patched_atexit.unregister.assert_called_once_with(d.stop_port_forward)
        assert d._atexit_registered is False


class TestSignalHandlerBehavior:
    """The signal handler must clean up and then chain to the original handler."""

    def test_sigterm_default_handler_reraises_signal(self):
        """Finding #1 (blocking): SIG_DFL is falsy, but SIGTERM must still terminate.

        The buggy version guarded chaining with `if handler and callable(handler)`,
        so a default (SIG_DFL) handler was skipped and the process survived SIGTERM.
        """
        d = _make_discovery()
        proc = mock.MagicMock()
        d._port_forward_process = proc
        d._original_sigterm_handler = signal.SIG_DFL
        with mock.patch.object(kubearchive_integration.signal, "signal") as m_sig, \
             mock.patch.object(kubearchive_integration.os, "kill") as m_kill, \
             mock.patch.object(kubearchive_integration.os, "getpid", return_value=4242):
            d._signal_handler(signal.SIGTERM, None)
        proc.terminate.assert_called_once()
        m_sig.assert_any_call(signal.SIGTERM, signal.SIG_DFL)
        m_kill.assert_called_once_with(4242, signal.SIGTERM)

    def test_sig_ign_handler_is_not_reraised(self):
        d = _make_discovery()
        proc = mock.MagicMock()
        d._port_forward_process = proc
        d._original_sigterm_handler = signal.SIG_IGN
        with mock.patch.object(kubearchive_integration.os, "kill") as m_kill:
            d._signal_handler(signal.SIGTERM, None)
        proc.terminate.assert_called_once()
        m_kill.assert_not_called()

    def test_callable_handler_is_chained(self):
        d = _make_discovery()
        proc = mock.MagicMock()
        d._port_forward_process = proc
        original = mock.MagicMock()
        d._original_sigint_handler = original
        frame = object()
        d._signal_handler(signal.SIGINT, frame)
        proc.terminate.assert_called_once()
        original.assert_called_once_with(signal.SIGINT, frame)


class TestSignalRegistration:
    """Signal handlers must be installed for both signals and restored on stop."""

    def test_registers_sigterm_and_sigint(self):
        d = _make_discovery()
        d._register_cleanup_handlers()
        # Bound methods compare equal but are not identical objects, so use ==.
        assert signal.getsignal(signal.SIGTERM) == d._signal_handler
        assert signal.getsignal(signal.SIGINT) == d._signal_handler
        assert d._signal_handlers_registered is True

    def test_sigint_failure_rolls_back_sigterm(self):
        """Finding #2: if SIGINT registration fails, the SIGTERM handler must be
        rolled back rather than left dangling with the flag still False."""
        d = _make_discovery()
        previous_sigterm = mock.MagicMock(name="previous_sigterm_handler")
        calls = []

        def fake_signal(signum, handler):
            calls.append((signum, handler))
            if signum == signal.SIGINT:
                raise ValueError("signal only works in main thread")
            return previous_sigterm

        with mock.patch.object(kubearchive_integration.signal, "signal", side_effect=fake_signal):
            d._register_cleanup_handlers()

        assert calls[0] == (signal.SIGTERM, d._signal_handler)
        assert calls[1] == (signal.SIGINT, d._signal_handler)
        assert calls[2] == (signal.SIGTERM, previous_sigterm)
        assert d._signal_handlers_registered is False


class TestFromSignalHandlerFlag:
    """Finding #3: cleanup from within a signal handler must not re-restore
    handlers, which would otherwise run the original handler twice."""

    def test_stop_from_signal_handler_skips_restoration(self):
        d = _make_discovery()
        d._signal_handlers_registered = True
        d._original_sigterm_handler = mock.MagicMock()
        d._original_sigint_handler = mock.MagicMock()
        with mock.patch.object(kubearchive_integration.signal, "signal") as m_sig:
            d.stop_port_forward(_from_signal_handler=True)
        m_sig.assert_not_called()
        assert d._signal_handlers_registered is False

    def test_stop_default_restores_signal_handlers(self):
        d = _make_discovery()
        d._signal_handlers_registered = True
        term = mock.MagicMock()
        intr = mock.MagicMock()
        d._original_sigterm_handler = term
        d._original_sigint_handler = intr
        with mock.patch.object(kubearchive_integration.signal, "signal") as m_sig:
            d.stop_port_forward()
        m_sig.assert_any_call(signal.SIGTERM, term)
        m_sig.assert_any_call(signal.SIGINT, intr)
        assert d._signal_handlers_registered is False


class TestDelFallback:
    """__del__ must remain a last-resort cleanup path."""

    def test_del_stops_process(self):
        d = _make_discovery()
        proc = mock.MagicMock()
        d._port_forward_process = proc
        d.__del__()
        proc.terminate.assert_called_once()
        assert d._port_forward_process is None

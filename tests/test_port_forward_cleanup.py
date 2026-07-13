"""
Tests for issue #121: Port-forward subprocess resource leak in KubeArchiveEndpointDiscovery.

These tests verify:
- The class implements context manager protocol (__enter__/__exit__)
- The class registers atexit handler when starting port-forward
- The class registers signal handlers (SIGTERM/SIGINT) for graceful shutdown
- The cleanup handlers are unregistered in stop_port_forward
- __del__ remains as a last-resort fallback
"""

import ast
import os
import re
import pytest

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
MODULE_FILE = os.path.join(REPO_ROOT, "src", "helpers", "kubearchive_integration.py")

if not os.path.isfile(MODULE_FILE):
    raise FileNotFoundError(
        f"Source file not found at {MODULE_FILE}. "
        f"If the test file was moved, update the REPO_ROOT derivation."
    )


def _read_source():
    """Read the kubearchive_integration.py source file."""
    with open(MODULE_FILE, "r") as f:
        return f.read()


def _parse_ast():
    """Parse the kubearchive_integration.py file into an AST."""
    source = _read_source()
    return ast.parse(source)


def _find_class_node(tree, name="KubeArchiveEndpointDiscovery"):
    """Find the AST node for a class definition by name."""
    for node in ast.walk(tree):
        if isinstance(node, ast.ClassDef) and node.name == name:
            return node
    return None


def _find_method_in_class(class_node, method_name):
    """Find a method definition within a class node."""
    for node in class_node.body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            if node.name == method_name:
                return node
    return None


def _extract_method_source_lines(source, class_name, method_name):
    """Extract source lines belonging to a method.

    Returns (start_line, end_line, lines) where lines is a list of
    source lines within the method (1-indexed start/end).
    """
    tree = ast.parse(source)
    class_node = _find_class_node(tree, class_name)
    if class_node is None:
        return None, None, []
    method_node = _find_method_in_class(class_node, method_name)
    if method_node is None:
        return None, None, []
    start = method_node.lineno
    end = method_node.end_lineno
    all_lines = source.splitlines()
    return start, end, all_lines[start - 1 : end]


class TestRequiredImports:
    """Verify required modules are imported."""

    def test_atexit_imported(self):
        """The file must import atexit."""
        source = _read_source()
        assert "import atexit" in source, "atexit is not imported"

    def test_signal_imported(self):
        """The file must import signal."""
        source = _read_source()
        assert "import signal" in source, "signal is not imported"

    def test_re_imported(self):
        """The file must import re (needed for _check_kubeconfig_route_inference)."""
        source = _read_source()
        assert "import re" in source, "re is not imported"


class TestContextManager:
    """Verify the class implements context manager protocol."""

    def test_enter_method_exists(self):
        """The class must have __enter__ method."""
        tree = _parse_ast()
        class_node = _find_class_node(tree)
        assert class_node is not None, "KubeArchiveEndpointDiscovery class not found"
        enter_method = _find_method_in_class(class_node, "__enter__")
        assert enter_method is not None, "__enter__ method not found"

    def test_exit_method_exists(self):
        """The class must have __exit__ method."""
        tree = _parse_ast()
        class_node = _find_class_node(tree)
        assert class_node is not None, "KubeArchiveEndpointDiscovery class not found"
        exit_method = _find_method_in_class(class_node, "__exit__")
        assert exit_method is not None, "__exit__ method not found"

    def test_exit_calls_stop_port_forward(self):
        """__exit__ must call stop_port_forward()."""
        source = _read_source()
        _, _, lines = _extract_method_source_lines(source, "KubeArchiveEndpointDiscovery", "__exit__")
        assert lines, "__exit__ method source not found"
        method_text = "\n".join(lines)
        assert "stop_port_forward" in method_text, "__exit__ does not call stop_port_forward"

    def test_enter_returns_self(self):
        """__enter__ should return self."""
        source = _read_source()
        _, _, lines = _extract_method_source_lines(source, "KubeArchiveEndpointDiscovery", "__enter__")
        assert lines, "__enter__ method source not found"
        method_text = "\n".join(lines)
        assert "return self" in method_text, "__enter__ does not return self"


class TestAtexitHandler:
    """Verify atexit handler is registered and unregistered."""

    def test_atexit_register_in_register_cleanup_handlers(self):
        """_register_cleanup_handlers must call atexit.register(stop_port_forward)."""
        source = _read_source()
        _, _, lines = _extract_method_source_lines(
            source, "KubeArchiveEndpointDiscovery", "_register_cleanup_handlers"
        )
        assert lines, "_register_cleanup_handlers method not found"
        method_text = "\n".join(lines)
        assert "atexit.register" in method_text, "atexit.register not called in _register_cleanup_handlers"
        assert "stop_port_forward" in method_text, "stop_port_forward not passed to atexit.register"

    def test_atexit_unregister_in_unregister_cleanup_handlers(self):
        """_unregister_cleanup_handlers must call atexit.unregister(stop_port_forward)."""
        source = _read_source()
        _, _, lines = _extract_method_source_lines(
            source, "KubeArchiveEndpointDiscovery", "_unregister_cleanup_handlers"
        )
        assert lines, "_unregister_cleanup_handlers method not found"
        method_text = "\n".join(lines)
        assert "atexit.unregister" in method_text, "atexit.unregister not called in _unregister_cleanup_handlers"

    def test_register_cleanup_handlers_called_in_setup_port_forward(self):
        """_setup_port_forward must call _register_cleanup_handlers after starting the process."""
        source = _read_source()
        _, _, lines = _extract_method_source_lines(
            source, "KubeArchiveEndpointDiscovery", "_setup_port_forward"
        )
        assert lines, "_setup_port_forward method not found"
        method_text = "\n".join(lines)
        assert "_register_cleanup_handlers" in method_text, (
            "_setup_port_forward does not call _register_cleanup_handlers"
        )

    def test_unregister_cleanup_handlers_called_in_stop_port_forward(self):
        """stop_port_forward must call _unregister_cleanup_handlers."""
        source = _read_source()
        _, _, lines = _extract_method_source_lines(
            source, "KubeArchiveEndpointDiscovery", "stop_port_forward"
        )
        assert lines, "stop_port_forward method not found"
        method_text = "\n".join(lines)
        assert "_unregister_cleanup_handlers" in method_text, (
            "stop_port_forward does not call _unregister_cleanup_handlers"
        )


class TestSignalHandlers:
    """Verify signal handlers are registered and restored."""

    def test_signal_handler_method_exists(self):
        """The class must have _signal_handler method."""
        tree = _parse_ast()
        class_node = _find_class_node(tree)
        assert class_node is not None, "KubeArchiveEndpointDiscovery class not found"
        handler_method = _find_method_in_class(class_node, "_signal_handler")
        assert handler_method is not None, "_signal_handler method not found"

    def test_signal_handler_calls_stop_port_forward(self):
        """_signal_handler must call stop_port_forward."""
        source = _read_source()
        _, _, lines = _extract_method_source_lines(
            source, "KubeArchiveEndpointDiscovery", "_signal_handler"
        )
        assert lines, "_signal_handler method not found"
        method_text = "\n".join(lines)
        assert "stop_port_forward" in method_text, "_signal_handler does not call stop_port_forward"

    def test_signal_handlers_registered_for_sigterm_and_sigint(self):
        """_register_cleanup_handlers must register handlers for SIGTERM and SIGINT."""
        source = _read_source()
        _, _, lines = _extract_method_source_lines(
            source, "KubeArchiveEndpointDiscovery", "_register_cleanup_handlers"
        )
        assert lines, "_register_cleanup_handlers method not found"
        method_text = "\n".join(lines)
        assert "signal.SIGTERM" in method_text, "SIGTERM handler not registered"
        assert "signal.SIGINT" in method_text, "SIGINT handler not registered"
        assert "signal.signal" in method_text, "signal.signal not called"

    def test_signal_handlers_restored_in_unregister(self):
        """_unregister_cleanup_handlers must restore original signal handlers."""
        source = _read_source()
        _, _, lines = _extract_method_source_lines(
            source, "KubeArchiveEndpointDiscovery", "_unregister_cleanup_handlers"
        )
        assert lines, "_unregister_cleanup_handlers method not found"
        method_text = "\n".join(lines)
        assert "signal.signal" in method_text, "signal.signal not called in _unregister_cleanup_handlers"
        assert "_original_sigterm_handler" in method_text or "_original_sigint_handler" in method_text, (
            "original signal handlers not restored"
        )


class TestInitFields:
    """Verify __init__ initializes cleanup tracking fields."""

    def test_init_has_atexit_registered_field(self):
        """__init__ must initialize _atexit_registered field."""
        source = _read_source()
        _, _, lines = _extract_method_source_lines(
            source, "KubeArchiveEndpointDiscovery", "__init__"
        )
        assert lines, "__init__ method not found"
        method_text = "\n".join(lines)
        assert "_atexit_registered" in method_text, "_atexit_registered field not initialized"

    def test_init_has_signal_handlers_registered_field(self):
        """__init__ must initialize _signal_handlers_registered field."""
        source = _read_source()
        _, _, lines = _extract_method_source_lines(
            source, "KubeArchiveEndpointDiscovery", "__init__"
        )
        assert lines, "__init__ method not found"
        method_text = "\n".join(lines)
        assert "_signal_handlers_registered" in method_text, "_signal_handlers_registered field not initialized"

    def test_init_has_original_handler_fields(self):
        """__init__ must initialize fields for storing original signal handlers."""
        source = _read_source()
        _, _, lines = _extract_method_source_lines(
            source, "KubeArchiveEndpointDiscovery", "__init__"
        )
        assert lines, "__init__ method not found"
        method_text = "\n".join(lines)
        assert "_original_sigterm_handler" in method_text, "_original_sigterm_handler field not initialized"
        assert "_original_sigint_handler" in method_text, "_original_sigint_handler field not initialized"


class TestDelMethodPreserved:
    """Verify __del__ is preserved as last-resort fallback."""

    def test_del_method_exists(self):
        """The class must still have __del__ method."""
        tree = _parse_ast()
        class_node = _find_class_node(tree)
        assert class_node is not None, "KubeArchiveEndpointDiscovery class not found"
        del_method = _find_method_in_class(class_node, "__del__")
        assert del_method is not None, "__del__ method not found"

    def test_del_calls_stop_port_forward(self):
        """__del__ must call stop_port_forward."""
        source = _read_source()
        _, _, lines = _extract_method_source_lines(
            source, "KubeArchiveEndpointDiscovery", "__del__"
        )
        assert lines, "__del__ method not found"
        method_text = "\n".join(lines)
        assert "stop_port_forward" in method_text, "__del__ does not call stop_port_forward"


class TestSyntaxValidity:
    """Verify the file is syntactically valid."""

    def test_ast_parse_succeeds(self):
        """ast.parse must succeed without raising SyntaxError."""
        source = _read_source()
        try:
            ast.parse(source)
        except SyntaxError as e:
            pytest.fail(f"SyntaxError in kubearchive_integration.py: {e}")

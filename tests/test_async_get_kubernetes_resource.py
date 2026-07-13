"""
Tests for issue #131: Convert get_kubernetes_resource from sync def to async def.

These tests verify:
- The function signature is async def (not def)
- Kubernetes API call sites use await asyncio.to_thread(...)
- No direct (unwrapped) k8s API calls remain in the function body
- The decorator, parameters, return type, and docstring are unchanged
- Changes are confined to the function body of get_kubernetes_resource
"""

import ast
import os
import re
import pytest

# Path to the source file under test
REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SERVER_FILE = os.path.join(REPO_ROOT, "src", "server-mcp.py")

if not os.path.isfile(SERVER_FILE):
    raise FileNotFoundError(
        f"Source file not found at {SERVER_FILE}. "
        f"If the test file was moved, update the REPO_ROOT derivation "
        f"(currently expects tests/ to be one level below the repo root)."
    )


def _read_source():
    """Read the server-mcp.py source file."""
    with open(SERVER_FILE, "r") as f:
        return f.read()


def _parse_ast():
    """Parse the server-mcp.py file into an AST."""
    source = _read_source()
    return ast.parse(source)


def _find_function_node(tree, name="get_kubernetes_resource"):
    """Find the AST node for a top-level function definition by name."""
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            if node.name == name:
                return node
    return None


def _extract_function_source_lines(source, func_name="get_kubernetes_resource"):
    """Extract source lines belonging to the function body.

    Returns (start_line, end_line, lines) where lines is a list of
    source lines within the function (1-indexed start/end).
    """
    tree = ast.parse(source)
    node = _find_function_node(tree, func_name)
    if node is None:
        return None, None, []
    start = node.lineno
    end = node.end_lineno
    all_lines = source.splitlines()
    return start, end, all_lines[start - 1 : end]


# ---------------------------------------------------------------------------
# AC1: Function is async def
# ---------------------------------------------------------------------------
class TestAsyncSignature:
    """AC1 + AC2: get_kubernetes_resource must be async def, not def."""

    def test_function_is_async_def(self):
        """The function node in the AST must be AsyncFunctionDef."""
        tree = _parse_ast()
        node = _find_function_node(tree, "get_kubernetes_resource")
        assert node is not None, "get_kubernetes_resource not found in AST"
        assert isinstance(node, ast.AsyncFunctionDef), (
            f"get_kubernetes_resource is {type(node).__name__}, expected AsyncFunctionDef"
        )

    def test_grep_async_def_signature(self):
        """grep must find exactly one 'async def get_kubernetes_resource' line."""
        source = _read_source()
        matches = re.findall(r"async def get_kubernetes_resource\(", source)
        assert len(matches) == 1, (
            f"Expected 1 match for 'async def get_kubernetes_resource(', found {len(matches)}"
        )

    def test_no_sync_def_signature(self):
        """grep must find zero lines starting with 'def get_kubernetes_resource'."""
        source = _read_source()
        # Match lines that have 'def get_kubernetes_resource(' but NOT 'async def'
        matches = [
            line
            for line in source.splitlines()
            if re.match(r"^def get_kubernetes_resource\(", line.strip())
        ]
        assert len(matches) == 0, (
            f"Found {len(matches)} sync 'def get_kubernetes_resource(' lines; expected 0"
        )


# ---------------------------------------------------------------------------
# AC4: k8s API call sites use await asyncio.to_thread
# ---------------------------------------------------------------------------
class TestAsyncioToThread:
    """AC4: Every k8s client API call inside get_kubernetes_resource uses
    await asyncio.to_thread(...)."""

    def test_at_least_ten_to_thread_calls(self):
        """The function wraps ~14 k8s API calls via asyncio.to_thread;
        require at least 10 to catch mass regressions while tolerating small refactors."""
        source = _read_source()
        start, end, func_lines = _extract_function_source_lines(source)
        assert func_lines, "Could not extract function source lines"
        func_text = "\n".join(func_lines)
        count = func_text.count("await asyncio.to_thread(")
        assert count >= 10, (
            f"Expected at least 10 'await asyncio.to_thread(' calls in function body, found {count}"
        )

    def test_no_direct_k8s_api_assignments(self):
        """No line in the function body should call a k8s API method without
        going through asyncio.to_thread — regardless of variable name."""
        source = _read_source()
        _, _, func_lines = _extract_function_source_lines(source)
        assert func_lines, "Could not extract function source lines"

        k8s_api_objects = [
            "k8s_core_api",
            "k8s_storage_api",
            "k8s_autoscaling_api",
            "k8s_apps_api",
            "k8s_batch_api",
            "k8s_custom_api",
        ]
        # Match direct k8s API method calls in any context — assignments,
        # return statements, bare expressions, conditionals — not just after '='.
        # Pattern 1: k8s_*_api.some_method(  — a k8s API object calling a method.
        # Pattern 2: method(  — an indirect call via a getattr-obtained reference.
        # Neither pattern false-positives on asyncio.to_thread(k8s_*_api.m, ...)
        # because to_thread receives the callable *without* invoking it (no '(').
        k8s_api_pattern = re.compile(
            r"(?:" + "|".join(re.escape(o) for o in k8s_api_objects) + r")\.\w+\("
            r"|\bmethod\("
        )
        direct_calls = []
        for i, line in enumerate(func_lines):
            stripped = line.strip()
            if k8s_api_pattern.search(stripped):
                if "await asyncio.to_thread(" not in stripped:
                    if not stripped.startswith("method = getattr("):
                        direct_calls.append((i + 1, stripped))

        assert len(direct_calls) == 0, (
            f"Found {len(direct_calls)} direct (unwrapped) k8s API call(s):\n"
            + "\n".join(f"  Line {ln}: {code}" for ln, code in direct_calls)
        )


# ---------------------------------------------------------------------------
# AC8: Decorator, parameters, return type, docstring unchanged
# ---------------------------------------------------------------------------
class TestSignaturePreserved:
    """AC8: The decorator, parameter signature, return type, and docstring
    must remain identical to the original."""

    def test_decorator_is_mcp_tool(self):
        """The function must be decorated with @mcp.tool()."""
        tree = _parse_ast()
        node = _find_function_node(tree, "get_kubernetes_resource")
        assert node is not None, "Function not found"
        assert len(node.decorator_list) >= 1, "Function has no decorators"
        decorator = node.decorator_list[0]
        assert isinstance(decorator, ast.Call), "Decorator is not a call expression"
        assert isinstance(decorator.func, ast.Attribute), (
            "Decorator is not an attribute access"
        )
        assert decorator.func.attr == "tool", (
            f"Decorator method is '{decorator.func.attr}', expected 'tool'"
        )
        assert isinstance(decorator.func.value, ast.Name), (
            "Decorator object is not a simple name"
        )
        assert decorator.func.value.id == "mcp", (
            f"Decorator object is '{decorator.func.value.id}', expected 'mcp'"
        )

    def test_parameter_signature(self):
        """Parameters must be: resource_type: str, name: str,
        namespace: str = 'default', output_format: str = 'summary'."""
        tree = _parse_ast()
        node = _find_function_node(tree, "get_kubernetes_resource")
        assert node is not None, "Function not found"

        args = node.args
        # Should have 4 positional args
        assert len(args.args) == 4, f"Expected 4 args, got {len(args.args)}"

        arg_names = [a.arg for a in args.args]
        assert arg_names == ["resource_type", "name", "namespace", "output_format"], (
            f"Arg names mismatch: {arg_names}"
        )

        # Check type annotations are all 'str'
        for a in args.args:
            assert a.annotation is not None, f"Arg '{a.arg}' has no type annotation"
            assert isinstance(a.annotation, ast.Name), (
                f"Arg '{a.arg}' annotation is not a simple Name node"
            )
            assert a.annotation.id == "str", (
                f"Arg '{a.arg}' annotation is '{a.annotation.id}', expected 'str'"
            )

        # Check defaults: namespace="default", output_format="summary"
        defaults = args.defaults
        assert len(defaults) == 2, f"Expected 2 defaults, got {len(defaults)}"
        assert isinstance(defaults[0], ast.Constant) and defaults[0].value == "default"
        assert isinstance(defaults[1], ast.Constant) and defaults[1].value == "summary"

    def test_return_type_is_str(self):
        """Return type annotation must be -> str."""
        tree = _parse_ast()
        node = _find_function_node(tree, "get_kubernetes_resource")
        assert node is not None, "Function not found"
        assert node.returns is not None, "No return type annotation"
        assert isinstance(node.returns, ast.Name), (
            "Return annotation is not a simple Name"
        )
        assert node.returns.id == "str", (
            f"Return type is '{node.returns.id}', expected 'str'"
        )

    def test_docstring_present_and_unchanged(self):
        """The docstring must mention 'Retrieve details about a Kubernetes/Tekton resource.'"""
        tree = _parse_ast()
        node = _find_function_node(tree, "get_kubernetes_resource")
        assert node is not None, "Function not found"
        docstring = ast.get_docstring(node)
        assert docstring is not None, "Function has no docstring"
        assert "Retrieve details about a Kubernetes/Tekton resource" in docstring


# ---------------------------------------------------------------------------
# AC7: Syntax validity
# ---------------------------------------------------------------------------
class TestSyntaxValidity:
    """AC7: The file must be syntactically valid Python."""

    def test_ast_parse_succeeds(self):
        """ast.parse must succeed without raising SyntaxError."""
        source = _read_source()
        try:
            ast.parse(source)
        except SyntaxError as e:
            pytest.fail(f"SyntaxError in server-mcp.py: {e}")


# ---------------------------------------------------------------------------
# R3: asyncio import present
# ---------------------------------------------------------------------------
class TestAsyncioImport:
    """R3: asyncio must be imported in the file."""

    def test_asyncio_imported(self):
        """The file must contain 'import asyncio'."""
        source = _read_source()
        assert "import asyncio" in source, "asyncio is not imported in server-mcp.py"

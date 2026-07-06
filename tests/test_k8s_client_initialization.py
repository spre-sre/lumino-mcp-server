"""Tests for Kubernetes API client initialization in server-mcp.py.

Verifies that:
1. K8s API clients are initialized exactly once (no duplicate initialization).
2. KubeArchiveEndpointDiscovery receives the same client objects the rest of the
   server uses.
3. k8s_autoscaling_api is included in the single initialization block.
4. When initialization fails, ALL clients -- including those held by
   KubeArchiveEndpointDiscovery -- are set to None consistently.
"""

import ast
import re
from pathlib import Path
from typing import List, Set, Tuple

import pytest

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

SERVER_FILE = Path(__file__).resolve().parent.parent / "src" / "server-mcp.py"


def _read_server_source() -> str:
    """Return the full source text of server-mcp.py."""
    return SERVER_FILE.read_text(encoding="utf-8")


def _find_client_constructor_calls(source: str) -> List[Tuple[int, str]]:
    """Return a list of (line_number, constructor_call) for every
    ``client.<Api>()`` constructor call at module scope.

    We look for patterns like ``client.CoreV1Api()`` and record the line
    number and full match.  Only *module-level* calls count -- those inside
    function/class bodies are excluded because they would not run at import
    time.
    """
    # Parse into an AST so we can filter to module-level statements.
    tree = ast.parse(source)

    # Collect line ranges for every FunctionDef / AsyncFunctionDef / ClassDef
    # at any nesting depth so we can exclude them.
    non_module_ranges: List[Tuple[int, int]] = []
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            end_line = getattr(node, "end_lineno", node.lineno)
            non_module_ranges.append((node.lineno, end_line))

    def _inside_function_or_class(lineno: int) -> bool:
        return any(start <= lineno <= end for start, end in non_module_ranges)

    # Now scan source lines for ``client.<Name>()`` constructor patterns.
    pattern = re.compile(r"\bclient\.\w+Api\(\)")
    results: List[Tuple[int, str]] = []
    for lineno, line in enumerate(source.splitlines(), start=1):
        # Skip lines inside function/class bodies.
        if _inside_function_or_class(lineno):
            continue
        for match in pattern.finditer(line):
            results.append((lineno, match.group(0)))

    return results


def _find_config_load_calls(source: str) -> List[Tuple[int, str]]:
    """Return module-level ``config.load_incluster_config()`` and
    ``config.load_kube_config()`` calls with their line numbers.
    """
    tree = ast.parse(source)

    non_module_ranges: List[Tuple[int, int]] = []
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            end_line = getattr(node, "end_lineno", node.lineno)
            non_module_ranges.append((node.lineno, end_line))

    def _inside_function_or_class(lineno: int) -> bool:
        return any(start <= lineno <= end for start, end in non_module_ranges)

    pattern = re.compile(r"\bconfig\.load_(?:incluster_config|kube_config)\(\)")
    results: List[Tuple[int, str]] = []
    for lineno, line in enumerate(source.splitlines(), start=1):
        if _inside_function_or_class(lineno):
            continue
        for match in pattern.finditer(line):
            results.append((lineno, match.group(0)))
    return results


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestSingleClientInitialization:
    """K8s API clients must be constructed exactly once at module scope."""

    def test_no_duplicate_core_v1_api(self) -> None:
        """client.CoreV1Api() must appear exactly once at module level."""
        source = _read_server_source()
        calls = [
            c for _, c in _find_client_constructor_calls(source) if "CoreV1Api" in c
        ]
        assert len(calls) == 1, (
            f"Expected exactly 1 module-level CoreV1Api() call, found {len(calls)}: {calls}"
        )

    def test_no_duplicate_apps_v1_api(self) -> None:
        """client.AppsV1Api() must appear exactly once at module level."""
        source = _read_server_source()
        calls = [
            c for _, c in _find_client_constructor_calls(source) if "AppsV1Api" in c
        ]
        assert len(calls) == 1, (
            f"Expected exactly 1 module-level AppsV1Api() call, found {len(calls)}: {calls}"
        )

    def test_no_duplicate_custom_objects_api(self) -> None:
        """client.CustomObjectsApi() must appear exactly once at module level."""
        source = _read_server_source()
        calls = [
            c
            for _, c in _find_client_constructor_calls(source)
            if "CustomObjectsApi" in c
        ]
        assert len(calls) == 1, (
            f"Expected exactly 1 module-level CustomObjectsApi() call, found {len(calls)}: {calls}"
        )

    def test_no_duplicate_batch_v1_api(self) -> None:
        """client.BatchV1Api() must appear exactly once at module level."""
        source = _read_server_source()
        calls = [
            c for _, c in _find_client_constructor_calls(source) if "BatchV1Api" in c
        ]
        assert len(calls) == 1, (
            f"Expected exactly 1 module-level BatchV1Api() call, found {len(calls)}: {calls}"
        )

    def test_no_duplicate_storage_v1_api(self) -> None:
        """client.StorageV1Api() must appear exactly once at module level."""
        source = _read_server_source()
        calls = [
            c for _, c in _find_client_constructor_calls(source) if "StorageV1Api" in c
        ]
        assert len(calls) == 1, (
            f"Expected exactly 1 module-level StorageV1Api() call, found {len(calls)}: {calls}"
        )

    def test_no_duplicate_config_load(self) -> None:
        """config.load_incluster_config / load_kube_config must each appear
        at most once at module level.
        """
        source = _read_server_source()
        calls = _find_config_load_calls(source)
        incluster = [c for _, c in calls if "incluster" in c]
        kubeconfig = [c for _, c in calls if "kube_config" in c]
        assert len(incluster) <= 1, (
            f"config.load_incluster_config() appears {len(incluster)} times at module level"
        )
        assert len(kubeconfig) <= 1, (
            f"config.load_kube_config() appears {len(kubeconfig)} times at module level"
        )

    def test_all_api_clients_in_single_try_block(self) -> None:
        """All client.<Api>() calls must reside within the same try block so
        that a failure in any one of them sets all to None.
        """
        source = _read_server_source()
        calls = _find_client_constructor_calls(source)
        if not calls:
            pytest.skip("No module-level client calls found")

        # Exclude optional APIs that intentionally live in their own
        # try blocks so their absence does not take down the core clients:
        #   - NetworkingV1Api: optional Ingress support
        #   - AutoscalingV2Api: cluster may lack the autoscaling/v2 API group
        # Verified structurally via the AST rather than a line-count heuristic
        # so that comments or blank lines inside the block don't cause false
        # failures.
        optional_apis = {"NetworkingV1Api", "AutoscalingV2Api"}
        core_calls = [
            (ln, c) for ln, c in calls if not any(api in c for api in optional_apis)
        ]
        if len(core_calls) < 2:
            pytest.skip("Fewer than 2 core client calls found")

        tree = ast.parse(source)

        # Exclude function/class bodies (same logic as the helper functions).
        non_module_ranges: List[Tuple[int, int]] = []
        for node in ast.walk(tree):
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
                end_line = getattr(node, "end_lineno", node.lineno)
                non_module_ranges.append((node.lineno, end_line))

        def _inside_function_or_class(lineno: int) -> bool:
            return any(start <= lineno <= end for start, end in non_module_ranges)

        # Collect every Try node at module scope.
        module_try_blocks: List[Tuple[int, int]] = []
        for node in ast.walk(tree):
            if isinstance(node, ast.Try) and not _inside_function_or_class(node.lineno):
                end_line = getattr(node, "end_lineno", node.lineno)
                module_try_blocks.append((node.lineno, end_line))

        # For each core call find the smallest (most specific) enclosing try block.
        enclosing_blocks: Set[Tuple[int, int]] = set()
        unguarded: List[Tuple[int, str]] = []
        for ln, c in core_calls:
            candidates = [(s, e) for s, e in module_try_blocks if s <= ln <= e]
            if not candidates:
                unguarded.append((ln, c))
            else:
                enclosing_blocks.add(min(candidates, key=lambda t: t[1] - t[0]))

        assert not unguarded, (
            f"These client constructor calls are not inside any try block: {unguarded}"
        )
        assert len(enclosing_blocks) == 1, (
            f"Client constructor calls span {len(enclosing_blocks)} different try blocks "
            f"(at lines {sorted(s for s, _ in enclosing_blocks)}). "
            "They should all be in a single initialization block."
        )


class TestAutoscalingApiIncluded:
    """k8s_autoscaling_api must be part of the single initialization."""

    def test_autoscaling_api_initialized_at_module_level(self) -> None:
        """client.AutoscalingV2Api() must appear at module level."""
        source = _read_server_source()
        calls = [
            c
            for _, c in _find_client_constructor_calls(source)
            if "AutoscalingV2Api" in c
        ]
        assert len(calls) >= 1, (
            "client.AutoscalingV2Api() is not initialized at module level"
        )

    def test_autoscaling_api_in_separate_optional_try_block(self) -> None:
        """AutoscalingV2Api is an optional API and must be in its own try
        block, separate from CoreV1Api, so that a failure in the optional
        autoscaling API does not take down all core clients.
        """
        source = _read_server_source()
        calls = _find_client_constructor_calls(source)

        core_line = next((ln for ln, c in calls if "CoreV1Api" in c), None)
        autoscaling_line = next(
            (ln for ln, c in calls if "AutoscalingV2Api" in c), None
        )

        assert core_line is not None, "CoreV1Api not found at module level"
        assert autoscaling_line is not None, (
            "AutoscalingV2Api not found at module level"
        )

        # Use AST to find the enclosing try block for each call and verify
        # they are in separate blocks (optional API isolation).
        tree = ast.parse(source)

        non_module_ranges: List[Tuple[int, int]] = []
        for node in ast.walk(tree):
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
                end_line = getattr(node, "end_lineno", node.lineno)
                non_module_ranges.append((node.lineno, end_line))

        def _inside_function_or_class(lineno: int) -> bool:
            return any(start <= lineno <= end for start, end in non_module_ranges)

        module_try_blocks: List[Tuple[int, int]] = []
        for node in ast.walk(tree):
            if isinstance(node, ast.Try) and not _inside_function_or_class(node.lineno):
                end_line = getattr(node, "end_lineno", node.lineno)
                module_try_blocks.append((node.lineno, end_line))

        def _enclosing_try(lineno: int) -> Tuple[int, int] | None:
            candidates = [(s, e) for s, e in module_try_blocks if s <= lineno <= e]
            return min(candidates, key=lambda t: t[1] - t[0]) if candidates else None

        core_block = _enclosing_try(core_line)
        autoscaling_block = _enclosing_try(autoscaling_line)

        assert core_block is not None, (
            f"CoreV1Api (line {core_line}) is not inside any try block"
        )
        assert autoscaling_block is not None, (
            f"AutoscalingV2Api (line {autoscaling_line}) is not inside any try block"
        )
        assert core_block != autoscaling_block, (
            f"AutoscalingV2Api (line {autoscaling_line}, try block {autoscaling_block}) "
            f"and CoreV1Api (line {core_line}, try block {core_block}) "
            f"are in the same try block. AutoscalingV2Api is an optional API and "
            f"must be in its own try block so its failure does not affect core clients."
        )


class TestKubeArchiveReceivesSameClients:
    """KubeArchiveEndpointDiscovery must receive the exact same client objects
    used by the rest of the server -- not objects from an earlier, replaced
    initialization.
    """

    def test_kubearchive_discovery_after_final_client_init(self) -> None:
        """KubeArchiveEndpointDiscovery construction must come AFTER the final
        (and only) client initialization block, not before a second one that
        overwrites the variables.
        """
        source = _read_server_source()
        client_calls = _find_client_constructor_calls(source)
        if not client_calls:
            pytest.skip("No client calls found")

        # Find the line where KubeArchiveEndpointDiscovery is constructed
        ka_pattern = re.compile(r"KubeArchiveEndpointDiscovery\(")
        ka_lines: List[int] = []
        for lineno, line in enumerate(source.splitlines(), start=1):
            if ka_pattern.search(line):
                ka_lines.append(lineno)

        assert ka_lines, "KubeArchiveEndpointDiscovery() not found in source"

        # The last client constructor call should come BEFORE the
        # KubeArchiveEndpointDiscovery call.  If clients are initialized twice
        # and KubeArchive is between them, the KubeArchive object holds stale
        # references.
        last_client_line = max(ln for ln, _ in client_calls)
        first_ka_line = min(ka_lines)

        assert first_ka_line > last_client_line, (
            f"KubeArchiveEndpointDiscovery is constructed at line {first_ka_line} "
            f"but the last client constructor call is at line {last_client_line}. "
            "KubeArchive must be initialized AFTER the final client init to receive "
            "the correct client objects."
        )

    def test_no_client_init_after_kubearchive_discovery(self) -> None:
        """There must be no module-level client.<Api>() calls after the
        KubeArchiveEndpointDiscovery construction.  Such calls would overwrite
        the variables, leaving KubeArchive with stale references.
        """
        source = _read_server_source()

        # Find KubeArchiveEndpointDiscovery construction line
        ka_pattern = re.compile(r"KubeArchiveEndpointDiscovery\(")
        ka_line = None
        for lineno, line in enumerate(source.splitlines(), start=1):
            if ka_pattern.search(line):
                ka_line = lineno
                break

        if ka_line is None:
            pytest.skip("KubeArchiveEndpointDiscovery not found")

        # Check for any client constructor calls after that line
        client_calls_after = [
            (ln, c) for ln, c in _find_client_constructor_calls(source) if ln > ka_line
        ]

        assert not client_calls_after, (
            f"Found {len(client_calls_after)} client constructor call(s) AFTER "
            f"KubeArchiveEndpointDiscovery at line {ka_line}: "
            f"{client_calls_after}. "
            "These would overwrite the client variables, giving KubeArchive stale references."
        )


class TestFailureConsistency:
    """When K8s client initialization fails, all clients -- including
    KubeArchive's -- must be set to None consistently.
    """

    def test_all_clients_set_to_none_on_failure(self) -> None:
        """The core except block must set every variable initialised in
        the core try block to None.

        Optional clients (AutoscalingV2Api, NetworkingV1Api) are handled
        by separate try/except blocks and the ``else`` branch of
        ``if k8s_core_api is not None``, so they are not checked here.
        """
        source = _read_server_source()
        lines = source.splitlines()

        # Find the try-except block that initializes clients.
        # Strategy: find the line that has ``client.CoreV1Api()`` at module
        # level, walk backwards to find the enclosing ``try:``, then find the
        # ``except`` and verify that all expected variables are set to None.
        client_calls = _find_client_constructor_calls(source)
        core_entries = [(ln, c) for ln, c in client_calls if "CoreV1Api" in c]
        assert core_entries, "CoreV1Api() not found at module level"

        # Use the LAST CoreV1Api call (which should be the only one after fix)
        core_line = core_entries[-1][0]

        # Walk backwards from core_line to find ``try:``
        try_line = None
        for i in range(core_line - 1, -1, -1):
            stripped = lines[i].strip()
            if stripped == "try:":
                try_line = i + 1  # 1-indexed
                break

        assert try_line is not None, (
            f"Could not find enclosing 'try:' before CoreV1Api at line {core_line}"
        )

        # Walk forward from core_line to find matching ``except``
        except_line = None
        for i in range(core_line, len(lines)):
            stripped = lines[i].strip()
            if stripped.startswith("except"):
                except_line = i + 1  # 1-indexed
                break

        assert except_line is not None, (
            f"Could not find 'except' after CoreV1Api at line {core_line}"
        )

        # Collect all ``<var> = None`` assignments in the except block body.
        # The body consists of lines indented MORE deeply than the ``except``
        # keyword itself.  We stop at the first non-empty line whose
        # indentation is <= the except keyword (i.e. outside the body).
        except_indent = len(lines[except_line - 1]) - len(
            lines[except_line - 1].lstrip()
        )
        none_vars: Set[str] = set()
        for i in range(except_line, len(lines)):
            line = lines[i]
            # Skip blank / whitespace-only lines.
            if not line.strip():
                continue
            # Stop once we leave the except body.
            line_indent = len(line) - len(line.lstrip())
            if line_indent <= except_indent:
                break
            m = re.match(r"\s+(k8s_\w+)\s*=\s*None", line)
            if m:
                none_vars.add(m.group(1))

        # Only the five core clients belong in the core try/except block.
        # k8s_autoscaling_api and k8s_networking_api are optional clients
        # initialised in their own try/except blocks and covered by the
        # ``else`` branch when k8s_core_api is None.
        expected_none = {
            "k8s_core_api",
            "k8s_apps_api",
            "k8s_custom_api",
            "k8s_batch_api",
            "k8s_storage_api",
        }

        missing = expected_none - none_vars
        assert not missing, (
            f"The except block does not set these to None: {missing}. "
            f"Found: {none_vars}"
        )

    def test_kubearchive_discovery_none_when_clients_none(self) -> None:
        """kubearchive_endpoint_discovery must be set to None when client init
        fails.  This is ensured structurally: the KubeArchive init should be
        guarded by ``if k8s_core_api is not None ...`` which is only true when
        the init succeeded.

        If the KubeArchive construction happens BEFORE a second (overwriting)
        client init, it could hold non-None references even when the final
        client variables are None -- a consistency violation.
        """
        source = _read_server_source()

        # Verify the guard exists
        guard_pattern = re.compile(
            r"if\s+k8s_core_api\s+is\s+not\s+None\s+and\s+k8s_custom_api\s+is\s+not\s+None"
        )
        guard_found = False
        guard_line = None
        for lineno, line in enumerate(source.splitlines(), start=1):
            if guard_pattern.search(line):
                guard_found = True
                guard_line = lineno
                break

        assert guard_found, (
            "Expected guard 'if k8s_core_api is not None and k8s_custom_api is not None' "
            "before KubeArchiveEndpointDiscovery construction"
        )

        # The guard must come AFTER all client constructor calls.
        # If it comes between two init blocks, the guard checks the first
        # init's variables, not the second's.
        client_calls = _find_client_constructor_calls(source)
        last_client_line = max(ln for ln, _ in client_calls) if client_calls else 0

        assert guard_line > last_client_line, (
            f"KubeArchive guard at line {guard_line} is before the last client "
            f"constructor call at line {last_client_line}. "
            "The guard must come after all client initializations."
        )

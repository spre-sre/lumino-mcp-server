"""
Tests for Kubernetes API client initialization in server-mcp.py.

These tests verify that:
1. K8s API clients are initialized exactly once (no duplicate initialization).
2. KubeArchiveEndpointDiscovery receives the same client objects other tools use.
3. k8s_autoscaling_api is included in the single initialization block.
4. When initialization fails, all clients including KubeArchive's are set to None.
5. namespace_filter regex compilation is ReDoS-safe (guards against catastrophic
   backtracking from user-supplied patterns).

The tests use AST-based source analysis and mock-based behavioral tests
to detect the duplicate-initialization bug and related issues.
"""

import ast
import re
from pathlib import Path
from typing import List, Dict

import pytest

# ---------------------------------------------------------------------------
# Fixtures and helpers
# ---------------------------------------------------------------------------

REPO_ROOT = Path(__file__).resolve().parent.parent
SERVER_MCP_PATH = REPO_ROOT / "src" / "server-mcp.py"


@pytest.fixture
def server_source() -> str:
    """Read the full source of server-mcp.py."""
    return SERVER_MCP_PATH.read_text()


def _find_top_level_assignments(source: str, var_name: str) -> List[int]:
    """Return 1-indexed line numbers where *var_name* is assigned at module
    level (column-offset 0 or inside a top-level try/except body)."""
    tree = ast.parse(source)
    lines: List[int] = []
    for node in ast.walk(tree):
        targets = []
        if isinstance(node, ast.Assign):
            targets = node.targets
        elif isinstance(node, ast.AnnAssign) and node.target:
            targets = [node.target]
        for target in targets:
            if isinstance(target, ast.Name) and target.id == var_name:
                lines.append(node.lineno)
    return lines


def _count_call_sites(source: str, pattern: str) -> int:
    """Count non-commented occurrences of *pattern* in source."""
    count = 0
    for line in source.splitlines():
        stripped = line.lstrip()
        if stripped.startswith("#"):
            continue
        if pattern in stripped:
            count += 1
    return count


def _find_module_level_tries(source: str) -> List[ast.Try]:
    """Return Try nodes at module level, including those directly inside
    top-level If/Else bodies.

    The K8s client init block lives inside ``if _k8s_config_loaded:``,
    so a plain ``ast.iter_child_nodes(tree)`` filter misses it.
    """
    tree = ast.parse(source)
    tries: List[ast.Try] = []
    for node in ast.iter_child_nodes(tree):
        if isinstance(node, ast.Try):
            tries.append(node)
        elif isinstance(node, ast.If):
            for child in node.body + node.orelse:
                if isinstance(child, ast.Try):
                    tries.append(child)
    return tries


# ===========================================================================
# Test 1 -- Clients initialized only once
# ===========================================================================


class TestSingleInitialization:
    """K8s API clients must be created in exactly one initialization block."""

    def test_core_api_assigned_once(self, server_source: str):
        """k8s_core_api should be assigned via client.CoreV1Api() exactly
        once (plus at most one None assignment in the except branch)."""
        creation_lines = []
        for lineno, line in enumerate(server_source.splitlines(), 1):
            stripped = line.lstrip()
            if stripped.startswith("#"):
                continue
            if re.search(r"k8s_core_api\s*=\s*client\.CoreV1Api\(\)", stripped):
                creation_lines.append(lineno)
        assert len(creation_lines) == 1, (
            f"k8s_core_api = client.CoreV1Api() appears on lines {creation_lines}; "
            f"expected exactly 1 creation site, found {len(creation_lines)}"
        )

    def test_apps_api_assigned_once(self, server_source: str):
        """k8s_apps_api should be created exactly once."""
        creation_lines = []
        for lineno, line in enumerate(server_source.splitlines(), 1):
            stripped = line.lstrip()
            if stripped.startswith("#"):
                continue
            if re.search(r"k8s_apps_api\s*=\s*client\.AppsV1Api\(\)", stripped):
                creation_lines.append(lineno)
        assert len(creation_lines) == 1, (
            f"k8s_apps_api = client.AppsV1Api() appears on lines {creation_lines}; "
            f"expected exactly 1 creation site, found {len(creation_lines)}"
        )

    def test_custom_api_assigned_once(self, server_source: str):
        """k8s_custom_api should be created exactly once."""
        creation_lines = []
        for lineno, line in enumerate(server_source.splitlines(), 1):
            stripped = line.lstrip()
            if stripped.startswith("#"):
                continue
            if re.search(
                r"k8s_custom_api\s*=\s*client\.CustomObjectsApi\(\)", stripped
            ):
                creation_lines.append(lineno)
        assert len(creation_lines) == 1, (
            f"k8s_custom_api = client.CustomObjectsApi() appears on lines "
            f"{creation_lines}; expected exactly 1 creation site, "
            f"found {len(creation_lines)}"
        )

    def test_batch_api_assigned_once(self, server_source: str):
        """k8s_batch_api should be created exactly once."""
        creation_lines = []
        for lineno, line in enumerate(server_source.splitlines(), 1):
            stripped = line.lstrip()
            if stripped.startswith("#"):
                continue
            if re.search(r"k8s_batch_api\s*=\s*client\.BatchV1Api\(\)", stripped):
                creation_lines.append(lineno)
        assert len(creation_lines) == 1, (
            f"k8s_batch_api = client.BatchV1Api() appears on lines "
            f"{creation_lines}; expected exactly 1 creation site, "
            f"found {len(creation_lines)}"
        )

    def test_storage_api_assigned_once(self, server_source: str):
        """k8s_storage_api should be created exactly once."""
        creation_lines = []
        for lineno, line in enumerate(server_source.splitlines(), 1):
            stripped = line.lstrip()
            if stripped.startswith("#"):
                continue
            if re.search(r"k8s_storage_api\s*=\s*client\.StorageV1Api\(\)", stripped):
                creation_lines.append(lineno)
        assert len(creation_lines) == 1, (
            f"k8s_storage_api = client.StorageV1Api() appears on lines "
            f"{creation_lines}; expected exactly 1 creation site, "
            f"found {len(creation_lines)}"
        )

    def test_no_duplicate_kubeconfig_loading(self, server_source: str):
        """config.load_incluster_config / load_kube_config should only
        appear once each (inside a single try/except block)."""
        incluster_lines = []
        kubeconfig_lines = []
        for lineno, line in enumerate(server_source.splitlines(), 1):
            stripped = line.lstrip()
            if stripped.startswith("#"):
                continue
            if "config.load_incluster_config()" in stripped:
                incluster_lines.append(lineno)
            if "config.load_kube_config()" in stripped:
                kubeconfig_lines.append(lineno)
        assert len(incluster_lines) == 1, (
            f"config.load_incluster_config() on lines {incluster_lines}; "
            f"expected exactly 1"
        )
        assert len(kubeconfig_lines) == 1, (
            f"config.load_kube_config() on lines {kubeconfig_lines}; expected exactly 1"
        )


# ===========================================================================
# Test 2 -- KubeArchiveEndpointDiscovery uses the same client objects
# ===========================================================================


class TestKubeArchiveReceivesSameClients:
    """KubeArchiveEndpointDiscovery must be constructed with the same
    k8s_core_api and k8s_custom_api objects that the rest of the module uses.

    When there are two init blocks, KubeArchive gets clients from the first
    block while the second block overwrites the module globals -- meaning
    KubeArchive holds stale references.
    """

    def test_kubearchive_init_after_single_client_block(self, server_source: str):
        """KubeArchiveEndpointDiscovery construction must appear AFTER the
        single client-creation try/except block -- not between two blocks.

        Strategy: find the line of the last client.* creation call and the
        line of KubeArchiveEndpointDiscovery construction. The discovery
        must come after the ONLY creation block, meaning there should be
        no further client.*Api() calls after the discovery line.
        """
        discovery_line = None
        client_creation_lines = []
        for lineno, line in enumerate(server_source.splitlines(), 1):
            stripped = line.lstrip()
            if stripped.startswith("#"):
                continue
            if "KubeArchiveEndpointDiscovery(" in stripped:
                if discovery_line is None:
                    discovery_line = lineno
            if re.search(r"client\.\w+Api\(\)", stripped):
                client_creation_lines.append(lineno)

        assert discovery_line is not None, (
            "KubeArchiveEndpointDiscovery construction not found"
        )

        # No client.*Api() creation should appear AFTER the discovery line.
        # If it does, that means the discovery got clients from an earlier
        # block and a later block overwrites them.
        later_creations = [
            line for line in client_creation_lines if line > discovery_line
        ]
        assert len(later_creations) == 0, (
            f"KubeArchiveEndpointDiscovery is constructed at line {discovery_line}, "
            f"but client.*Api() calls appear later at lines {later_creations}. "
            f"This means KubeArchive holds references to clients from an earlier "
            f"initialization block that gets overwritten."
        )

    def test_kubearchive_not_between_two_init_blocks(self, server_source: str):
        """There must not be two separate 'Initialize Kubernetes API clients'
        sections in the source with KubeArchive sandwiched between them."""
        lines = server_source.splitlines()
        core_api_creation_lines = []
        for lineno, line in enumerate(lines, 1):
            stripped = line.lstrip()
            if stripped.startswith("#"):
                continue
            if re.search(r"k8s_core_api\s*=\s*client\.CoreV1Api\(\)", stripped):
                core_api_creation_lines.append(lineno)

        # With a correct single-init, there is only one creation line.
        # With the bug, there are two, and KubeArchive sits between them.
        if len(core_api_creation_lines) > 1:
            # Find KubeArchiveEndpointDiscovery line
            for lineno, line in enumerate(lines, 1):
                if "KubeArchiveEndpointDiscovery(" in line:
                    discovery_line = lineno
                    break
            else:
                discovery_line = None

            if discovery_line is not None:
                first_init = core_api_creation_lines[0]
                second_init = core_api_creation_lines[1]
                sandwiched = first_init < discovery_line < second_init
                assert not sandwiched, (
                    f"KubeArchiveEndpointDiscovery at line {discovery_line} is "
                    f"sandwiched between two k8s_core_api creation sites "
                    f"(lines {first_init} and {second_init}). It receives "
                    f"clients from the first block, but the second block "
                    f"overwrites the module-level variables."
                )

        # The primary assertion: only one creation site should exist
        assert len(core_api_creation_lines) == 1, (
            f"k8s_core_api created {len(core_api_creation_lines)} times"
        )


# ===========================================================================
# Test 3 -- k8s_autoscaling_api in the single initialization
# ===========================================================================


class TestAutoscalingApiIncluded:
    """k8s_autoscaling_api must be created alongside the other clients in
    the same initialization block, and covered by the same error handler."""

    def test_autoscaling_api_in_same_block_as_core_api(self, server_source: str):
        """k8s_autoscaling_api = client.AutoscalingV2Api() must appear in
        the SAME try block as k8s_core_api = client.CoreV1Api().

        When there are two init blocks and autoscaling only lives in the
        second one, it means the first (unguarded) block does not create it.
        This test ensures there is a single block containing both by
        walking the AST and checking that both assignments descend from
        the same top-level Try node.
        """
        # Collect module-level Try nodes (including those inside
        # top-level If/Else bodies such as ``if _k8s_config_loaded``).
        top_level_tries = _find_module_level_tries(server_source)

        def _try_contains(try_node, var_name, api_call_substr):
            """Return True if *try_node* contains an assignment
            ``var_name = <call matching api_call_substr>(...)``."""
            for child in ast.walk(try_node):
                if not isinstance(child, ast.Assign):
                    continue
                for target in child.targets:
                    if isinstance(target, ast.Name) and target.id == var_name:
                        if isinstance(child.value, ast.Call):
                            src = ast.get_source_segment(server_source, child.value)
                            if src and api_call_substr in src:
                                return True
            return False

        core_tries = [
            t for t in top_level_tries if _try_contains(t, "k8s_core_api", "CoreV1Api")
        ]
        autoscaling_tries = [
            t
            for t in top_level_tries
            if _try_contains(t, "k8s_autoscaling_api", "AutoscalingV2Api")
        ]

        assert len(core_tries) == 1, (
            f"Expected k8s_core_api = client.CoreV1Api() in exactly one "
            f"try block, found it in {len(core_tries)}"
        )
        assert len(autoscaling_tries) == 1, (
            f"Expected k8s_autoscaling_api = client.AutoscalingV2Api() in "
            f"exactly one try block, found it in {len(autoscaling_tries)}"
        )

        assert core_tries[0] is autoscaling_tries[0], (
            f"k8s_core_api (try block at line {core_tries[0].lineno}) and "
            f"k8s_autoscaling_api (try block at line "
            f"{autoscaling_tries[0].lineno}) are in separate try blocks. "
            f"They must be in the same initialization block."
        )

    def test_autoscaling_api_created_before_kubearchive_discovery(
        self, server_source: str
    ):
        """k8s_autoscaling_api must be initialized before
        KubeArchiveEndpointDiscovery so it is available to all tools
        in the same scope."""
        lines = server_source.splitlines()
        autoscaling_line = None
        discovery_line = None
        for lineno, line in enumerate(lines, 1):
            stripped = line.lstrip()
            if stripped.startswith("#"):
                continue
            if re.search(
                r"k8s_autoscaling_api\s*=\s*client\.AutoscalingV2Api\(\)", stripped
            ):
                autoscaling_line = lineno
            if "KubeArchiveEndpointDiscovery(" in stripped:
                if discovery_line is None:
                    discovery_line = lineno

        assert autoscaling_line is not None, "k8s_autoscaling_api creation not found"
        assert discovery_line is not None, "KubeArchiveEndpointDiscovery not found"
        assert autoscaling_line < discovery_line, (
            f"k8s_autoscaling_api (line {autoscaling_line}) must be initialized "
            f"BEFORE KubeArchiveEndpointDiscovery (line {discovery_line}). "
            f"Currently it appears after, meaning it lives in a separate "
            f"(later) init block."
        )


# ===========================================================================
# Test 4 -- Failure consistency: all clients set to None together
# ===========================================================================


class TestFailureConsistency:
    """When K8s client initialization fails, ALL clients -- including
    kubearchive_endpoint_discovery -- must be set to None."""

    def test_kubearchive_set_to_none_in_except_block(self, server_source: str):
        """The except block that sets k8s_core_api = None must also set
        kubearchive_endpoint_discovery = None.

        With the current bug, the except block at the second init sets
        k8s_core_api = None etc., but kubearchive_endpoint_discovery was
        already created (from the first init's clients) and is never
        cleared. This leaves a KubeArchiveEndpointDiscovery holding
        references to now-None'd clients.
        """
        lines = server_source.splitlines()

        # Find except blocks that set k8s_core_api = None
        in_except = False
        except_block_vars: Dict[int, List[str]] = {}
        except_start = None
        for lineno, line in enumerate(lines, 1):
            stripped = line.lstrip()
            if stripped.startswith("except") and ":" in stripped:
                in_except = True
                except_start = lineno
                except_block_vars[lineno] = []
                continue
            if in_except:
                # Detect end of except block (non-indented line or new
                # top-level statement)
                if stripped and not line[0].isspace():
                    in_except = False
                    continue
                if "= None" in stripped:
                    var_name = stripped.split("=")[0].strip()
                    except_block_vars[except_start].append(var_name)

        # Find the except block that nullifies k8s_core_api
        relevant_blocks = {
            start: vars_
            for start, vars_ in except_block_vars.items()
            if "k8s_core_api" in vars_
        }

        assert len(relevant_blocks) > 0, (
            "No except block found that sets k8s_core_api = None"
        )

        for start_line, nullified_vars in relevant_blocks.items():
            assert "kubearchive_endpoint_discovery" in nullified_vars, (
                f"Except block at line {start_line} sets these to None: "
                f"{nullified_vars}, but does NOT include "
                f"kubearchive_endpoint_discovery. When clients fail to "
                f"initialize, KubeArchive must also be cleared."
            )

    def test_networking_api_set_to_none_in_except_block(self, server_source: str):
        """k8s_networking_api must also be set to None in the same except
        block as the other clients, for consistency."""
        lines = server_source.splitlines()

        in_except = False
        except_block_vars: Dict[int, List[str]] = {}
        except_start = None
        for lineno, line in enumerate(lines, 1):
            stripped = line.lstrip()
            if stripped.startswith("except") and ":" in stripped:
                in_except = True
                except_start = lineno
                except_block_vars[lineno] = []
                continue
            if in_except:
                if stripped and not line[0].isspace():
                    in_except = False
                    continue
                if "= None" in stripped:
                    var_name = stripped.split("=")[0].strip()
                    except_block_vars[except_start].append(var_name)

        relevant_blocks = {
            start: vars_
            for start, vars_ in except_block_vars.items()
            if "k8s_core_api" in vars_
        }

        assert len(relevant_blocks) > 0, (
            "No except block found that sets k8s_core_api = None"
        )

        for start_line, nullified_vars in relevant_blocks.items():
            assert "k8s_networking_api" in nullified_vars, (
                f"Except block at line {start_line} sets these to None: "
                f"{nullified_vars}, but does NOT include k8s_networking_api. "
                f"All API clients must be nullified together."
            )

    def test_all_clients_in_single_try_except(self, server_source: str):
        """All k8s_*_api client creations should live inside a single
        try/except block, not scattered across multiple blocks or at
        bare module level."""
        tree = ast.parse(server_source)

        # Find module-level Try nodes (including inside top-level If/Else)
        top_level_tries = _find_module_level_tries(server_source)

        # For each Try, check if it contains k8s client creation assignments
        client_pattern = re.compile(r"client\.\w+Api")
        tries_with_clients = []
        for try_node in top_level_tries:
            for child in ast.walk(try_node):
                if isinstance(child, ast.Assign):
                    # Check if the assignment value is a call to client.*Api()
                    if isinstance(child.value, ast.Call):
                        src_segment = ast.get_source_segment(server_source, child.value)
                        if src_segment and client_pattern.search(src_segment):
                            tries_with_clients.append(try_node.lineno)
                            break

        # Also check for bare (non-try-wrapped) client creation at module
        # level or inside top-level If/Else bodies.
        bare_creation_lines = []

        def _check_bare_assigns(nodes):
            for node in nodes:
                if isinstance(node, ast.Assign):
                    if isinstance(node.value, ast.Call):
                        src_segment = ast.get_source_segment(server_source, node.value)
                        if src_segment and client_pattern.search(src_segment):
                            bare_creation_lines.append(node.lineno)

        for node in ast.iter_child_nodes(tree):
            if isinstance(node, ast.Assign):
                _check_bare_assigns([node])
            elif isinstance(node, ast.If):
                _check_bare_assigns(node.body + node.orelse)

        assert len(bare_creation_lines) == 0, (
            f"K8s client creation at module level WITHOUT try/except on "
            f"lines {bare_creation_lines}. All client initialization must "
            f"be inside a try/except block."
        )

        unique_tries = list(set(tries_with_clients))
        assert len(unique_tries) <= 1, (
            f"K8s client.*Api() calls found in {len(unique_tries)} separate "
            f"try blocks (starting at lines {unique_tries}). "
            f"All clients should be created in a single try/except block."
        )


# ===========================================================================
# Test 5 -- ReDoS-safe namespace_filter regex handling
# ===========================================================================


def _find_namespace_filter_compile_sites(source: str) -> List[Dict]:
    """Find all *call sites* where namespace_filter is compiled as a regex.

    Matches both the safe helper ``_safe_compile_namespace_filter(...)``
    and any raw ``re.compile(namespace_filter)`` calls that may be
    reintroduced by accident.  Excludes the function *definition* itself.
    """
    sites = []
    for lineno, line in enumerate(source.splitlines(), 1):
        stripped = line.lstrip()
        if stripped.startswith("#") or stripped.startswith("def "):
            continue
        if re.search(
            r"_safe_compile_namespace_filter\(|re\.compile\(.*namespace_filter",
            stripped,
        ):
            sites.append({"lineno": lineno, "line_text": stripped})
    return sites


class TestNamespaceFilterReDoSSafety:
    """namespace_filter is user-supplied input compiled as a regex.
    Without safeguards, a crafted pattern like ``(a+)+$`` matched against
    ``"aaaaaaaaaaaaaaaaaa!"`` causes catastrophic backtracking (ReDoS).

    These tests verify that:
    - A ``_safe_compile_namespace_filter`` helper exists with length and
      nested-quantifier guards.
    - All namespace_filter compile sites use the safe helper (no raw
      ``re.compile``).
    - The safe helper rejects known ReDoS patterns.
    - The call sites catch ``ValueError`` (raised by the helper) in
      addition to ``re.error``.
    """

    # ---- structural: the safe helper exists and has the right guards ----

    def test_safe_compile_helper_exists(self, server_source: str):
        """_safe_compile_namespace_filter must be defined in server-mcp.py."""
        assert "def _safe_compile_namespace_filter(" in server_source, (
            "_safe_compile_namespace_filter helper not found in server-mcp.py. "
            "Namespace filter compilation must go through a ReDoS-safe helper."
        )

    def test_safe_compile_has_length_check(self, server_source: str):
        """The helper must enforce a maximum pattern length."""
        # Find the function body (between def and next top-level def/class)
        lines = server_source.splitlines()
        in_helper = False
        helper_body_lines = []
        for line in lines:
            if "def _safe_compile_namespace_filter(" in line:
                in_helper = True
                continue
            if in_helper:
                # End of function: next non-indented non-blank line
                if line and not line[0].isspace() and line.strip():
                    break
                helper_body_lines.append(line)

        helper_body = "\n".join(helper_body_lines)
        assert re.search(r"len\(pattern\)", helper_body), (
            "_safe_compile_namespace_filter does not check len(pattern). "
            "A maximum length guard is required to limit regex complexity."
        )

    def test_safe_compile_has_nested_quantifier_check(self, server_source: str):
        """The helper must detect nested quantifiers that cause
        catastrophic backtracking."""
        lines = server_source.splitlines()
        in_helper = False
        helper_body_lines = []
        for line in lines:
            if "def _safe_compile_namespace_filter(" in line:
                in_helper = True
                continue
            if in_helper:
                if line and not line[0].isspace() and line.strip():
                    break
                helper_body_lines.append(line)

        helper_body = "\n".join(helper_body_lines)
        has_quantifier_check = (
            "_NESTED_QUANTIFIER_RE" in helper_body
            or "nested" in helper_body.lower()
            or "quantifier" in helper_body.lower()
        )
        assert has_quantifier_check, (
            "_safe_compile_namespace_filter does not check for nested "
            "quantifiers. Patterns like (a+)+ cause catastrophic "
            "backtracking and must be rejected."
        )

    def test_max_regex_pattern_len_constant_exists(self, server_source: str):
        """A _MAX_REGEX_PATTERN_LEN constant must be defined."""
        assert "_MAX_REGEX_PATTERN_LEN" in server_source, (
            "_MAX_REGEX_PATTERN_LEN constant not found. The safe compile "
            "helper needs a configurable length cap."
        )

    def test_nested_quantifier_regex_constant_exists(self, server_source: str):
        """A _NESTED_QUANTIFIER_RE constant must be defined for detecting
        dangerous regex constructs."""
        assert "_NESTED_QUANTIFIER_RE" in server_source, (
            "_NESTED_QUANTIFIER_RE constant not found. A pre-compiled "
            "pattern for detecting nested quantifiers is required."
        )

    # ---- structural: all call sites use the safe helper ----

    def test_no_raw_re_compile_on_namespace_filter(self, server_source: str):
        """No call site should pass namespace_filter directly to
        re.compile().  All must go through _safe_compile_namespace_filter."""
        raw_sites = []
        for lineno, line in enumerate(server_source.splitlines(), 1):
            stripped = line.lstrip()
            if stripped.startswith("#"):
                continue
            # Detect re.compile(namespace_filter) but NOT inside the
            # _safe_compile_namespace_filter definition itself.
            if re.search(r"re\.compile\(\s*namespace_filter", stripped):
                raw_sites.append(lineno)

        assert len(raw_sites) == 0, (
            f"Raw re.compile(namespace_filter) found at line(s) {raw_sites}. "
            f"All namespace_filter compilation must use "
            f"_safe_compile_namespace_filter() to prevent ReDoS."
        )

    def test_all_namespace_filter_sites_use_safe_helper(self, server_source: str):
        """Every place that compiles namespace_filter must call the safe
        helper, not raw re.compile."""
        sites = _find_namespace_filter_compile_sites(server_source)
        assert len(sites) >= 2, (
            f"Expected at least 2 namespace_filter compile sites "
            f"(prometheus results + topology mapper), found {len(sites)}. "
            f"If call sites were removed, update this test."
        )

        for site in sites:
            assert "_safe_compile_namespace_filter" in site["line_text"], (
                f"Line {site['lineno']} compiles namespace_filter without "
                f"using _safe_compile_namespace_filter: {site['line_text']}"
            )

    def test_call_sites_catch_value_error(self, server_source: str):
        """The except clauses around namespace_filter compilation must
        catch ValueError (raised by the safe helper for dangerous
        patterns), not just re.error."""
        sites = _find_namespace_filter_compile_sites(server_source)
        lines = server_source.splitlines()

        for site in sites:
            lineno = site["lineno"]
            # Scan forward up to 35 lines for the corresponding except
            # (defense-in-depth asyncio.wait_for wrappers add lines)
            region = "\n".join(lines[lineno - 1 : min(lineno + 35, len(lines))])
            has_value_error_catch = bool(
                re.search(r"except\s*\(.*ValueError.*\)", region)
            ) or bool(re.search(r"except\s+ValueError", region))
            assert has_value_error_catch, (
                f"Namespace filter compile site at line {site['lineno']} "
                f"does not catch ValueError.  _safe_compile_namespace_filter "
                f"raises ValueError for dangerous patterns; the caller must "
                f"handle it."
            )

    # ---- behavioral: the safe helper rejects known ReDoS patterns ----

    def test_rejects_nested_quantifier_pattern(self, server_source: str):
        """_safe_compile_namespace_filter must reject (a+)+$ and similar
        nested-quantifier patterns that cause catastrophic backtracking."""
        # Extract _NESTED_QUANTIFIER_RE and _MAX_REGEX_PATTERN_LEN from source
        # and replicate the guard logic to test without importing server-mcp.py.
        match = re.search(r"_MAX_REGEX_PATTERN_LEN\s*=\s*(\d+)", server_source)
        assert match, "_MAX_REGEX_PATTERN_LEN not found"
        max_len = int(match.group(1))

        # Find the _NESTED_QUANTIFIER_RE pattern string
        nq_match = re.search(
            r'_NESTED_QUANTIFIER_RE\s*=\s*re\.compile\(\s*\n?\s*r"([^"]+)"',
            server_source,
        )
        assert nq_match, "_NESTED_QUANTIFIER_RE pattern not found"
        # Reconstruct the full pattern (may span multiple r"..." fragments)
        nq_lines = []
        in_nq = False
        for line in server_source.splitlines():
            if "_NESTED_QUANTIFIER_RE" in line and "re.compile" in line:
                in_nq = True
            if in_nq:
                nq_lines.append(line)
                if line.rstrip().endswith(")"):
                    break

        nq_source = "\n".join(nq_lines)
        # Extract all r"..." fragments and concatenate
        fragments = re.findall(r'r"([^"]*)"', nq_source)
        nq_pattern = "".join(fragments)
        nested_re = re.compile(nq_pattern)

        # Replicate the guard
        def safe_compile(pattern: str) -> re.Pattern:
            if len(pattern) > max_len:
                raise ValueError("too long")
            if nested_re.search(pattern):
                raise ValueError("nested quantifier")
            return re.compile(pattern)

        # These must be rejected
        redos_patterns = [
            r"(a+)+$",
            r"(x*)+y",
            r"([^/]+)+",
            r"(?:a+)+",
            # Overlapping-alternation quantifiers (finding 1/7)
            r"(a|aa)+$",
            r"(b|bb)+",
            r"(x|xx|xxx)+",
        ]
        for pattern in redos_patterns:
            with pytest.raises(ValueError, match="nested quantifier"):
                safe_compile(pattern)

    def test_rejects_overlong_pattern(self, server_source: str):
        """_safe_compile_namespace_filter must reject patterns exceeding
        _MAX_REGEX_PATTERN_LEN."""
        match = re.search(r"_MAX_REGEX_PATTERN_LEN\s*=\s*(\d+)", server_source)
        assert match, "_MAX_REGEX_PATTERN_LEN not found"
        max_len = int(match.group(1))

        # Sanity-check the constant
        assert max_len > 0, "_MAX_REGEX_PATTERN_LEN must be positive"
        assert max_len <= 1000, (
            f"_MAX_REGEX_PATTERN_LEN={max_len} is too permissive. "
            f"A limit above 1000 chars provides insufficient ReDoS protection."
        )

        # Reconstruct the guard logic from source to test without importing
        nq_lines = []
        in_nq = False
        for line in server_source.splitlines():
            if "_NESTED_QUANTIFIER_RE" in line and "re.compile" in line:
                in_nq = True
            if in_nq:
                nq_lines.append(line)
                if line.rstrip().endswith(")"):
                    break
        fragments = re.findall(r'r"([^"]*)"', "\n".join(nq_lines))
        nested_re = re.compile("".join(fragments))

        def safe_compile(pattern: str) -> re.Pattern:
            if len(pattern) > max_len:
                raise ValueError("too long")
            if nested_re.search(pattern):
                raise ValueError("nested quantifier")
            return re.compile(pattern)

        # A pattern just over the limit must be rejected
        overlong = "a" * (max_len + 1)
        with pytest.raises(ValueError, match="too long"):
            safe_compile(overlong)

    def test_accepts_safe_namespace_patterns(self, server_source: str):
        """_safe_compile_namespace_filter must accept normal namespace
        patterns that users would legitimately provide."""
        match = re.search(r"_MAX_REGEX_PATTERN_LEN\s*=\s*(\d+)", server_source)
        assert match
        max_len = int(match.group(1))

        nq_lines = []
        in_nq = False
        for line in server_source.splitlines():
            if "_NESTED_QUANTIFIER_RE" in line and "re.compile" in line:
                in_nq = True
            if in_nq:
                nq_lines.append(line)
                if line.rstrip().endswith(")"):
                    break
        fragments = re.findall(r'r"([^"]*)"', "\n".join(nq_lines))
        nested_re = re.compile("".join(fragments))

        safe_patterns = [
            r"openshift-.*",
            r"kube-system|kube-public",
            r"test-\d{4}",
            r"^prod-",
            r"my-namespace",
            r"(staging|prod)",  # alternation without quantifier -- safe
            r"(\d{3})+",  # bounded quantifier {n} -- safe (finding 4)
        ]
        for pattern in safe_patterns:
            assert len(pattern) <= max_len, f"Test pattern too long: {pattern}"
            assert not nested_re.search(pattern), (
                f"Safe pattern {pattern!r} falsely detected as dangerous "
                f"by _NESTED_QUANTIFIER_RE"
            )
            # Must compile without error
            re.compile(pattern)

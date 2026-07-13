"""
Tests for ReDoS-safe namespace_filter regex compilation in server-mcp.py.

These tests verify that:
- A ``_safe_compile_namespace_filter`` helper exists with length and
  nested-quantifier guards.
- All namespace_filter compile sites use the safe helper (no raw
  ``re.compile``).
- The safe helper rejects known ReDoS patterns.
- The call sites catch ``ValueError`` (raised by the helper) in
  addition to ``re.error``.

The tests use source-level analysis to detect patterns without importing
server-mcp.py (which requires a running Kubernetes cluster).
"""

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


# ===========================================================================
# ReDoS-safe namespace_filter regex handling
# ===========================================================================


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
            # Overlapping-alternation quantifiers
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
            r"(\d{3})+",  # bounded quantifier {n} -- safe
        ]
        for pattern in safe_patterns:
            assert len(pattern) <= max_len, f"Test pattern too long: {pattern}"
            assert not nested_re.search(pattern), (
                f"Safe pattern {pattern!r} falsely detected as dangerous "
                f"by _NESTED_QUANTIFIER_RE"
            )
            # Must compile without error
            re.compile(pattern)

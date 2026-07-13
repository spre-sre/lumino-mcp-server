"""
Shared fixtures for lumino-mcp-server tests.
"""

from pathlib import Path

import pytest

# Resolve the path to src/server-mcp.py relative to the tests directory.
REPO_ROOT = Path(__file__).resolve().parent.parent
SERVER_MCP_PATH = REPO_ROOT / "src" / "server-mcp.py"


@pytest.fixture
def source_code() -> str:
    """Read the full source of src/server-mcp.py as a string."""
    return SERVER_MCP_PATH.read_text()


@pytest.fixture
def source_lines(source_code: str) -> list[str]:
    """Return the source split into individual lines (0-indexed)."""
    return source_code.split("\n")


@pytest.fixture
def server_mcp_path() -> Path:
    """Return the resolved Path to src/server-mcp.py."""
    return SERVER_MCP_PATH

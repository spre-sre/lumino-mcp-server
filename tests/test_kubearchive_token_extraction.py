"""
Tests for KubeArchiveClient._extract_token_from_client().

Verifies that the bearer-prefix stripping is case-insensitive so that
tokens stored as 'bearer sha256~...' (lowercase, as some k8s clients do)
are handled the same as 'Bearer sha256~...' (capitalized).

Covers issue #158: case-insensitive Bearer prefix stripping.
"""

import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, PropertyMock, patch

import pytest

# Add src/ to the path so we can import the module under test.
SRC_DIR = Path(__file__).resolve().parent.parent / "src"
sys.path.insert(0, str(SRC_DIR))

# Import the class under test. The module has side-effects (logging setup),
# but the class itself is safe to instantiate with mocks.
from helpers.kubearchive_integration import KubeArchiveClient


def _make_client(api_key_dict: dict) -> KubeArchiveClient:
    """Build a KubeArchiveClient whose k8s_core_api exposes *api_key_dict*
    via ``k8s_core_api.api_client.configuration.api_key``."""
    config = SimpleNamespace(api_key=api_key_dict)
    api_client = SimpleNamespace(configuration=config)
    k8s_core_api = SimpleNamespace(api_client=api_client)

    discovery = MagicMock()  # endpoint_discovery — unused by the method
    return KubeArchiveClient(
        endpoint_discovery=discovery,
        k8s_core_api=k8s_core_api,
    )


# ------------------------------------------------------------------
# Acceptance-criteria tests for case-insensitive prefix stripping
# ------------------------------------------------------------------

class TestBearerPrefixStripping:
    """The method must strip *any* casing of 'bearer ' and return only the
    token payload."""

    def test_lowercase_bearer_prefix(self):
        """api_key='bearer sha256~token' -> 'sha256~token' (prefix stripped)."""
        c = _make_client({"authorization": "bearer sha256~token"})
        assert c._extract_token_from_client() == "sha256~token"

    def test_capitalized_bearer_prefix(self):
        """api_key='Bearer sha256~token' -> 'sha256~token' (no regression)."""
        c = _make_client({"authorization": "Bearer sha256~token"})
        assert c._extract_token_from_client() == "sha256~token"

    def test_uppercase_bearer_prefix(self):
        """api_key='BEARER sha256~token' -> 'sha256~token'."""
        c = _make_client({"authorization": "BEARER sha256~token"})
        assert c._extract_token_from_client() == "sha256~token"

    def test_mixed_case_bearer_prefix(self):
        """api_key='bEaReR sha256~token' -> 'sha256~token'."""
        c = _make_client({"authorization": "bEaReR sha256~token"})
        assert c._extract_token_from_client() == "sha256~token"


# ------------------------------------------------------------------
# Other paths through the method
# ------------------------------------------------------------------

class TestNoPrefixPassthrough:
    """When there is no 'bearer ' prefix the raw value is returned."""

    def test_no_prefix(self):
        """api_key='sha256~token' -> 'sha256~token' unchanged."""
        c = _make_client({"authorization": "sha256~token"})
        assert c._extract_token_from_client() == "sha256~token"


class TestBearerTokenKeyFallback:
    """When 'authorization' is absent, fall back to the 'BearerToken' key."""

    def test_bearer_token_key_fallback(self):
        c = _make_client({"BearerToken": "some-token"})
        assert c._extract_token_from_client() == "some-token"


class TestNoneReturns:
    """Cases that must return None rather than a token string."""

    def test_no_api_key_returns_none(self):
        """Neither 'authorization' nor 'BearerToken' present -> None."""
        c = _make_client({})
        assert c._extract_token_from_client() is None

    def test_no_k8s_core_api_returns_none(self):
        """self.k8s_core_api is None -> immediate None."""
        discovery = MagicMock()
        c = KubeArchiveClient(endpoint_discovery=discovery, k8s_core_api=None)
        assert c._extract_token_from_client() is None

    def test_exception_returns_none(self):
        """If config.api_key raises, the except block returns None."""
        k8s_core_api = MagicMock()
        # Make .api_client.configuration.api_key raise on access
        type(k8s_core_api.api_client.configuration).api_key = PropertyMock(
            side_effect=RuntimeError("boom"),
        )
        discovery = MagicMock()
        c = KubeArchiveClient(endpoint_discovery=discovery, k8s_core_api=k8s_core_api)
        assert c._extract_token_from_client() is None

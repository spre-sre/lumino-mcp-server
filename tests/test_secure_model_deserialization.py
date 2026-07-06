"""Tests for secure model deserialization in ModelPersistenceManager.

Verifies that:
1. Model IDs are validated to prevent path-traversal attacks.
2. Only alphanumeric/dot/hyphen/underscore characters are accepted.
3. joblib.load paths are restricted to the expected storage directory.
4. Model files are integrity-checked via HMAC-SHA256 before deserialization.
5. Tampered model files are rejected.
"""

import importlib.util
import json
import sys
from pathlib import Path

import pytest

# Add src/ to the path so we can import the module under test.
SRC_DIR = Path(__file__).resolve().parent.parent / "src"
sys.path.insert(0, str(SRC_DIR))

# Load ml_persistence.py *directly* via importlib so that the helpers package
# __init__.py (which eagerly re-exports every submodule and pulls in heavy
# dependencies like pyyaml, kubernetes, etc.) is never executed.  The module
# under test only needs stdlib + numpy + joblib — none of the heavy deps.
_ML_PERSISTENCE_PATH = SRC_DIR / "helpers" / "ml_persistence.py"
try:
    _spec = importlib.util.spec_from_file_location(
        "helpers.ml_persistence", _ML_PERSISTENCE_PATH
    )
    _mod = importlib.util.module_from_spec(_spec)
    _spec.loader.exec_module(_mod)
    ModelPersistenceManager = _mod.ModelPersistenceManager
except (ImportError, ModuleNotFoundError, FileNotFoundError) as _imp_err:
    pytest.skip(
        f"Cannot import ModelPersistenceManager: {_imp_err}",
        allow_module_level=True,
    )


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def manager(tmp_path: Path) -> ModelPersistenceManager:
    """Return a ModelPersistenceManager rooted in a temporary directory."""
    return ModelPersistenceManager(storage_dir=str(tmp_path))


# ---------------------------------------------------------------------------
# Model-ID validation
# ---------------------------------------------------------------------------


class TestModelIdValidation:
    """_validate_model_id must reject unsafe identifiers."""

    @pytest.mark.parametrize(
        "bad_id,reason",
        [
            ("../../etc/passwd", "path traversal with ../"),
            ("../evil", "single-level path traversal"),
            ("foo/../../bar", "embedded path traversal"),
            ("..%2F..%2Fetc%2Fpasswd", "URL-encoded path traversal"),
            ("", "empty string"),
            ("a" * 256, "exceeds 255-character limit"),
            ("/absolute/path", "starts with slash"),
            ("model id with spaces", "contains spaces"),
            ("model;rm -rf", "contains semicolon"),
            ("model\x00null", "contains null byte"),
        ],
    )
    def test_rejects_unsafe_model_id(
        self, manager: ModelPersistenceManager, bad_id: str, reason: str
    ) -> None:
        """model_id values with path-traversal or invalid characters are
        rejected with ValueError.
        """
        with pytest.raises(ValueError):
            manager._validate_model_id(bad_id)

    @pytest.mark.parametrize(
        "good_id",
        [
            "predictive_log_v1_20240101_120000",
            "model-v2.3",
            "a",
            "A1_b2-c3.d4",
        ],
    )
    def test_accepts_valid_model_id(
        self, manager: ModelPersistenceManager, good_id: str
    ) -> None:
        """Legitimate model IDs pass validation without error."""
        manager._validate_model_id(good_id)  # must not raise


# ---------------------------------------------------------------------------
# Path restriction (joblib.load never escapes storage_dir)
# ---------------------------------------------------------------------------


class TestPathRestriction:
    """All public methods that accept a model_id must refuse identifiers
    whose resolved path falls outside the storage directory.
    """

    def test_load_model_rejects_traversal(
        self, manager: ModelPersistenceManager
    ) -> None:
        with pytest.raises(ValueError):
            manager.load_model("../../etc/passwd")

    def test_save_model_rejects_traversal(
        self, manager: ModelPersistenceManager
    ) -> None:
        with pytest.raises(ValueError):
            manager.save_model(object(), "../../evil", {})

    def test_delete_model_rejects_traversal(
        self, manager: ModelPersistenceManager
    ) -> None:
        with pytest.raises(ValueError):
            manager.delete_model("../../evil")

    def test_model_exists_returns_false_for_traversal(
        self, manager: ModelPersistenceManager
    ) -> None:
        """model_exists catches ValueError internally and returns False."""
        assert manager.model_exists("../../evil") is False

    def test_get_model_metadata_rejects_traversal(
        self, manager: ModelPersistenceManager
    ) -> None:
        with pytest.raises(ValueError):
            manager.get_model_metadata("../../evil")


# ---------------------------------------------------------------------------
# HMAC integrity verification
# ---------------------------------------------------------------------------


class TestIntegrityVerification:
    """Model files must be integrity-checked before joblib.load."""

    def _save_dummy_model(
        self, manager: ModelPersistenceManager, model_id: str = "test_model"
    ) -> Path:
        """Save a trivial model via the manager and return the .joblib path."""
        try:
            import joblib  # noqa: F401
        except ImportError:
            pytest.skip("joblib not installed")

        manager.save_model({"weights": [1, 2, 3]}, model_id, {"version": 1})
        return manager.storage_dir / f"{model_id}.joblib"

    def test_roundtrip_save_load_succeeds(
        self, manager: ModelPersistenceManager
    ) -> None:
        """A model saved and loaded without modification passes integrity."""
        self._save_dummy_model(manager)
        model, metadata = manager.load_model("test_model")
        assert model == {"weights": [1, 2, 3]}
        assert "model_hmac" in metadata

    def test_tampered_model_file_rejected(
        self, manager: ModelPersistenceManager
    ) -> None:
        """Modifying the .joblib file after save causes integrity failure."""
        model_file = self._save_dummy_model(manager)

        # Tamper with the file by appending bytes
        with open(model_file, "ab") as f:
            f.write(b"TAMPERED")

        with pytest.raises(ValueError, match="integrity"):
            manager.load_model("test_model")

    def test_replaced_model_file_rejected(
        self, manager: ModelPersistenceManager
    ) -> None:
        """Completely replacing the .joblib file is detected."""
        self._save_dummy_model(manager)
        model_file = manager.storage_dir / "test_model.joblib"

        # Replace with a different file
        try:
            import joblib
        except ImportError:
            pytest.skip("joblib not installed")

        joblib.dump({"malicious": True}, model_file)

        with pytest.raises(ValueError, match="integrity"):
            manager.load_model("test_model")

    def test_hmac_stored_in_metadata_on_save(
        self, manager: ModelPersistenceManager
    ) -> None:
        """save_model must record model_hmac in the metadata sidecar."""
        self._save_dummy_model(manager)
        meta_file = manager.storage_dir / "test_model.meta.json"
        assert meta_file.exists()

        with open(meta_file) as f:
            metadata = json.load(f)

        assert "model_hmac" in metadata
        assert isinstance(metadata["model_hmac"], str)
        assert len(metadata["model_hmac"]) == 64  # SHA-256 hex digest

    def test_legacy_model_without_hmac_warns(
        self, manager: ModelPersistenceManager
    ) -> None:
        """Models saved without HMAC (legacy) load with a warning, not an
        error, to avoid breaking existing deployments.
        """
        try:
            import joblib
        except ImportError:
            pytest.skip("joblib not installed")

        model_id = "legacy_model"
        model_file = manager.storage_dir / f"{model_id}.joblib"
        meta_file = manager.storage_dir / f"{model_id}.meta.json"

        # Simulate a legacy save: write model and metadata without HMAC
        joblib.dump({"old": True}, model_file)
        with open(meta_file, "w") as f:
            json.dump({"model_id": model_id, "version": 0}, f)

        # Update the index so the manager knows about it
        index = manager._load_index()
        index["models"].append(
            {"model_id": model_id, "file_path": str(model_file), "is_active": False}
        )
        manager._save_index(index)

        # Should load successfully (with a logged warning, not an exception)
        model, metadata = manager.load_model(model_id)
        assert model == {"old": True}


# ---------------------------------------------------------------------------
# Signing key management
# ---------------------------------------------------------------------------


class TestSigningKey:
    """The per-installation signing key must be created deterministically
    and reused across calls.
    """

    def test_signing_key_created_on_first_use(
        self, manager: ModelPersistenceManager
    ) -> None:
        key_file = manager.storage_dir / ModelPersistenceManager._SIGNING_KEY_FILE
        assert not key_file.exists()
        key = manager._get_signing_key()
        assert key_file.exists()
        assert len(key) == 32  # 256-bit key

    def test_signing_key_reused(self, manager: ModelPersistenceManager) -> None:
        """Subsequent calls return the same key."""
        key1 = manager._get_signing_key()
        key2 = manager._get_signing_key()
        assert key1 == key2

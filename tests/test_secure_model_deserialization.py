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

    def test_strict_mode_rejects_missing_meta_json(
        self, manager: ModelPersistenceManager, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """In strict mode (default), a model with no .meta.json at all is
        rejected because there is no HMAC to verify.
        """
        monkeypatch.setenv("LUMINO_STRICT_MODEL_LOADING", "true")
        try:
            import joblib
        except ImportError:
            pytest.skip("joblib not installed")

        model_id = "no_meta_model"
        model_file = manager.storage_dir / f"{model_id}.joblib"

        # Write a model file with no accompanying metadata
        joblib.dump({"orphan": True}, model_file)

        # Update the index so the manager knows about it
        index = manager._load_index()
        index["models"].append(
            {"model_id": model_id, "file_path": str(model_file), "is_active": False}
        )
        manager._save_index(index)

        with pytest.raises(ValueError, match="no HMAC signature"):
            manager.load_model(model_id)

    def test_strict_mode_rejects_meta_without_hmac_key(
        self, manager: ModelPersistenceManager, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """In strict mode, a model with meta.json but no model_hmac key is
        rejected.
        """
        monkeypatch.setenv("LUMINO_STRICT_MODEL_LOADING", "true")
        try:
            import joblib
        except ImportError:
            pytest.skip("joblib not installed")

        model_id = "legacy_no_hmac"
        model_file = manager.storage_dir / f"{model_id}.joblib"
        meta_file = manager.storage_dir / f"{model_id}.meta.json"

        joblib.dump({"old": True}, model_file)
        with open(meta_file, "w") as f:
            json.dump({"model_id": model_id, "version": 0}, f)

        index = manager._load_index()
        index["models"].append(
            {"model_id": model_id, "file_path": str(model_file), "is_active": False}
        )
        manager._save_index(index)

        with pytest.raises(ValueError, match="no HMAC signature"):
            manager.load_model(model_id)

    def test_tampered_hmac_value_rejected(
        self, manager: ModelPersistenceManager, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A model with a tampered (wrong) HMAC value is rejected."""
        monkeypatch.setenv("LUMINO_STRICT_MODEL_LOADING", "true")
        self._save_dummy_model(manager)
        meta_file = manager.storage_dir / "test_model.meta.json"

        # Tamper with the stored HMAC
        with open(meta_file) as f:
            metadata = json.load(f)
        metadata["model_hmac"] = "a" * 64  # wrong HMAC
        with open(meta_file, "w") as f:
            json.dump(metadata, f, indent=2)

        with pytest.raises(ValueError, match="integrity"):
            manager.load_model("test_model")

    def test_stripped_hmac_from_existing_metadata_rejected(
        self, manager: ModelPersistenceManager, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Stripping model_hmac from an existing metadata file must not
        allow loading in strict mode -- this is a downgrade attack.
        """
        monkeypatch.setenv("LUMINO_STRICT_MODEL_LOADING", "true")
        self._save_dummy_model(manager)
        meta_file = manager.storage_dir / "test_model.meta.json"

        # Remove the model_hmac key from metadata (downgrade attack)
        with open(meta_file) as f:
            metadata = json.load(f)
        del metadata["model_hmac"]
        with open(meta_file, "w") as f:
            json.dump(metadata, f, indent=2)

        with pytest.raises(ValueError, match="no HMAC signature"):
            manager.load_model("test_model")

    def test_permissive_mode_allows_legacy_model(
        self, manager: ModelPersistenceManager, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """When LUMINO_STRICT_MODEL_LOADING=false, legacy models without
        HMAC load with a warning (not an error) for migration purposes.
        """
        monkeypatch.setenv("LUMINO_STRICT_MODEL_LOADING", "false")
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

        index = manager._load_index()
        index["models"].append(
            {"model_id": model_id, "file_path": str(model_file), "is_active": False}
        )
        manager._save_index(index)

        # Should load successfully in permissive mode
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
        self,
        manager: ModelPersistenceManager,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
    ) -> None:
        # Redirect Path.home() to tmp_path so we don't touch the real
        # ~/.lumino directory during tests.
        fake_home = tmp_path / "fakehome"
        fake_home.mkdir()
        monkeypatch.setattr(Path, "home", staticmethod(lambda: fake_home))

        key_file = fake_home / ".lumino" / ModelPersistenceManager._SIGNING_KEY_FILE
        assert not key_file.exists()
        key = manager._get_signing_key()
        assert key_file.exists()
        assert len(key) == 32  # 256-bit key

    def test_signing_key_reused(
        self,
        manager: ModelPersistenceManager,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
    ) -> None:
        """Subsequent calls return the same key."""
        fake_home = tmp_path / "fakehome"
        fake_home.mkdir()
        monkeypatch.setattr(Path, "home", staticmethod(lambda: fake_home))

        key1 = manager._get_signing_key()
        key2 = manager._get_signing_key()
        assert key1 == key2

    def test_corrupted_key_file_rejected(
        self,
        manager: ModelPersistenceManager,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
    ) -> None:
        """A truncated or empty key file raises ValueError."""
        fake_home = tmp_path / "fakehome"
        fake_home.mkdir()
        monkeypatch.setattr(Path, "home", staticmethod(lambda: fake_home))

        key_dir = fake_home / ".lumino"
        key_dir.mkdir(parents=True, exist_ok=True)
        key_file = key_dir / ModelPersistenceManager._SIGNING_KEY_FILE
        # Write a truncated key (only 10 bytes instead of 32)
        key_file.write_bytes(b"x" * 10)

        with pytest.raises(ValueError, match="corrupted"):
            manager._get_signing_key()

"""Persistent storage for ML models, training data, failure events, and model versioning."""
# ============================================================================
# ML PERSISTENCE MODULE
# ============================================================================
#
# This module provides persistent storage for ML models, training data,
# failure event collection, and model version management for the
# predictive_log_analyzer tool.
# ============================================================================

import io
import json
import hashlib
import hmac as hmac_mod
import logging
import os
import re
import sqlite3
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple
import numpy as np

# Joblib is included with scikit-learn
try:
    import joblib
except ImportError:
    from sklearn.externals import joblib

logger = logging.getLogger("lumino-mcp")


# ============================================================================
# CLUSTER IDENTIFICATION
# ============================================================================


def get_current_cluster_id() -> str:
    """Get the current Kubernetes cluster identifier.

    Returns the cluster name from kubeconfig context, which uniquely identifies
    the cluster. Falls back to 'unknown' if not available.

    Returns:
        Cluster identifier string (e.g., 'api-stone-prod-p02-hjvn-p1-openshiftapps-com:6443')
    """
    try:
        from kubernetes import config

        contexts, current = config.list_kube_config_contexts()
        if current and "context" in current:
            cluster = current["context"].get("cluster", "unknown")
            return cluster
    except Exception as e:
        logger.debug(f"Could not get cluster ID from kubeconfig: {e}")

    # Fallback: try to get from in-cluster config
    try:
        import os

        # In-cluster, use the API server from environment
        api_server = os.environ.get("KUBERNETES_SERVICE_HOST", "")
        if api_server:
            return f"in-cluster-{api_server}"
    except Exception:
        pass

    return "unknown"


# ============================================================================
# MODEL PERSISTENCE MANAGER
# ============================================================================


class ModelPersistenceManager:
    """Manages persistent storage of ML models and their metadata.

    Models are stored using joblib serialization with JSON sidecar files
    for metadata. Provides model indexing, cleanup, and version tracking.
    """

    def __init__(self, storage_dir: Optional[str] = None):
        """Initialize the model persistence manager.

        Args:
            storage_dir: Directory for model storage. Defaults to ~/.lumino/models
        """
        if storage_dir is None:
            storage_dir = str(Path.home() / ".lumino" / "models")

        self.storage_dir = Path(storage_dir).expanduser()
        self.storage_dir.mkdir(parents=True, exist_ok=True)
        self.model_index_file = self.storage_dir / "model_index.json"
        self._ensure_index_exists()

    def _ensure_index_exists(self) -> None:
        """Ensure the model index file exists."""
        if not self.model_index_file.exists():
            self._save_index(
                {
                    "current_model_id": None,
                    "models": [],
                    "last_cleanup": datetime.now().isoformat(),
                }
            )

    # ---- Security: model ID validation and integrity verification ----------

    # Valid model ID pattern: starts with alphanumeric, then alphanumeric/dot/hyphen/underscore
    _MODEL_ID_PATTERN = re.compile(r"^[a-zA-Z0-9][a-zA-Z0-9._-]*$")
    _SIGNING_KEY_FILE = ".model_signing_key"

    def _validate_model_id(self, model_id: str) -> None:
        """Validate model_id to prevent path traversal and injection attacks.

        Args:
            model_id: The model identifier to validate

        Raises:
            ValueError: If model_id contains unsafe characters or patterns
        """
        if not model_id:
            raise ValueError("model_id cannot be empty")
        if len(model_id) > 255:
            raise ValueError("model_id too long (max 255 characters)")
        if not self._MODEL_ID_PATTERN.match(model_id):
            raise ValueError(
                f"Invalid model_id '{model_id}': must start with an alphanumeric "
                "character and contain only alphanumeric characters, dots, "
                "hyphens, and underscores"
            )
        # Resolve the path and verify it stays within the storage directory
        resolved_storage = self.storage_dir.resolve()
        model_path = (self.storage_dir / f"{model_id}.joblib").resolve()
        if (
            not str(model_path).startswith(str(resolved_storage) + os.sep)
            and model_path.parent != resolved_storage
        ):
            raise ValueError(
                f"model_id '{model_id}' resolves outside storage directory"
            )

    def _get_signing_key(self) -> bytes:
        """Get or create the model signing key for HMAC verification.

        The key is stored in the parent of the models directory
        (~/.lumino/.model_signing_key) so that an attacker with write access
        to the models/ directory alone cannot read the key and forge valid
        HMACs.

        Returns:
            The 256-bit signing key

        Raises:
            ValueError: If the key file exists but is not exactly 32 bytes
                (corrupted or truncated key)
        """
        import tempfile

        key_dir = Path.home() / ".lumino"
        key_dir.mkdir(parents=True, exist_ok=True)
        key_file = key_dir / self._SIGNING_KEY_FILE

        # Try to atomically create the key file (O_CREAT|O_EXCL prevents a
        # TOCTOU race where two concurrent processes both generate different
        # keys and the last writer silently wins).
        key = os.urandom(32)
        try:
            # Write to a temp file in the same directory first, then
            # atomically rename to the final path.  This prevents a
            # half-written key file from being read if os.write() is
            # interrupted.
            fd, tmp_path = tempfile.mkstemp(dir=str(key_dir), prefix=".key_tmp_")
            try:
                os.write(fd, key)
            finally:
                os.close(fd)
            os.chmod(tmp_path, 0o600)

            # Atomic rename -- if the target already exists on POSIX, the
            # rename is still atomic but another process won.  Use the
            # link+unlink trick to emulate O_EXCL semantics for the final
            # path.
            try:
                os.link(tmp_path, str(key_file))
                os.unlink(tmp_path)
            except (FileExistsError, OSError):
                # Another process (or a previous run) already created the
                # key file.  Discard ours and read theirs.
                os.unlink(tmp_path)
                existing_key = key_file.read_bytes()
                if len(existing_key) != 32:
                    raise ValueError(
                        f"Model signing key at {key_file} is corrupted "
                        f"({len(existing_key)} bytes, expected 32). "
                        "Delete the file and retry to generate a new key."
                    )
                return existing_key

        except Exception:
            # If we get here unexpectedly, ensure we still try to read an
            # existing key file before giving up.
            if key_file.exists():
                existing_key = key_file.read_bytes()
                if len(existing_key) != 32:
                    raise ValueError(
                        f"Model signing key at {key_file} is corrupted "
                        f"({len(existing_key)} bytes, expected 32). "
                        "Delete the file and retry to generate a new key."
                    )
                return existing_key
            raise

        # Validate the key we just wrote
        written_key = key_file.read_bytes()
        if len(written_key) != 32:
            raise ValueError(
                f"Model signing key at {key_file} is corrupted "
                f"({len(written_key)} bytes, expected 32). "
                "Delete the file and retry to generate a new key."
            )
        logger.info("Generated new model signing key")
        return written_key

    def _compute_file_hmac(self, file_path: Path) -> str:
        """Compute HMAC-SHA256 of a file for integrity verification.

        Args:
            file_path: Path to the file

        Returns:
            Hex-encoded HMAC-SHA256 string
        """
        with open(file_path, "rb") as f:
            data = f.read()
        return self._compute_bytes_hmac(data)

    def _compute_bytes_hmac(self, data: bytes) -> str:
        """Compute HMAC-SHA256 of a byte buffer for integrity verification.

        Args:
            data: The bytes to compute the HMAC over

        Returns:
            Hex-encoded HMAC-SHA256 string
        """
        key = self._get_signing_key()
        h = hmac_mod.new(key, data, digestmod=hashlib.sha256)
        return h.hexdigest()

    def _verify_file_hmac(self, file_path: Path, expected_hmac: str) -> bool:
        """Verify HMAC-SHA256 of a file against an expected value.

        Uses constant-time comparison to prevent timing attacks.

        Args:
            file_path: Path to the file to verify
            expected_hmac: The expected HMAC hex string

        Returns:
            True if the HMAC matches
        """
        actual_hmac = self._compute_file_hmac(file_path)
        return hmac_mod.compare_digest(actual_hmac, expected_hmac)

    def _verify_bytes_hmac(self, data: bytes, expected_hmac: str) -> bool:
        """Verify HMAC-SHA256 of a byte buffer against an expected value.

        Uses constant-time comparison to prevent timing attacks.

        Args:
            data: The bytes to verify
            expected_hmac: The expected HMAC hex string

        Returns:
            True if the HMAC matches
        """
        actual_hmac = self._compute_bytes_hmac(data)
        return hmac_mod.compare_digest(actual_hmac, expected_hmac)

    # ---- End security helpers ----------------------------------------------

    def _load_index(self) -> Dict[str, Any]:
        """Load the model index from disk."""
        try:
            with open(self.model_index_file, "r") as f:
                return json.load(f)
        except (json.JSONDecodeError, FileNotFoundError):
            return {
                "current_model_id": None,
                "models": [],
                "last_cleanup": datetime.now().isoformat(),
            }

    def _save_index(self, index: Dict[str, Any]) -> None:
        """Save the model index to disk."""
        with open(self.model_index_file, "w") as f:
            json.dump(index, f, indent=2)

    def save_model(self, model: Any, model_id: str, metadata: Dict[str, Any]) -> str:
        """Save a model to disk with metadata.

        The save is performed atomically: the model is first written to a
        temporary file, the HMAC is computed, the metadata (including HMAC)
        is written, and only then the temp file is atomically renamed to
        the final path.  This ensures no model file ever exists on disk
        without a corresponding valid HMAC in the metadata sidecar.

        Args:
            model: The trained model object (e.g., IsolationForest)
            model_id: Unique identifier for the model
            metadata: Model metadata (training info, performance, etc.)

        Returns:
            The model file path
        """
        import tempfile

        self._validate_model_id(model_id)

        model_file = self.storage_dir / f"{model_id}.joblib"
        meta_file = self.storage_dir / f"{model_id}.meta.json"

        # Step 1: Write model to a temporary file in the same directory.
        # Using the same directory ensures os.rename() is atomic (same
        # filesystem).
        fd, tmp_path = tempfile.mkstemp(
            dir=str(self.storage_dir), prefix=f".{model_id}_tmp_", suffix=".joblib"
        )
        os.close(fd)
        tmp_model_path = Path(tmp_path)

        try:
            # Save model to temp file using joblib
            joblib.dump(model, tmp_model_path)

            # Step 2: Compute integrity HMAC over the temp file
            model_hmac = self._compute_file_hmac(tmp_model_path)

            # Step 3: Add timestamps and integrity signature to metadata
            metadata["model_id"] = model_id
            metadata["created_at"] = metadata.get(
                "created_at", datetime.now().isoformat()
            )
            metadata["last_used_at"] = datetime.now().isoformat()
            metadata["file_path"] = str(model_file)
            metadata["model_hmac"] = model_hmac

            # Write metadata (with HMAC) BEFORE renaming the model file.
            with open(meta_file, "w") as f:
                json.dump(metadata, f, indent=2)

            # Step 4: Atomically rename temp model file to final path.
            # After this point, the model file and its HMAC are consistent.
            os.replace(str(tmp_model_path), str(model_file))

        except Exception:
            # Clean up temp file on any failure
            if tmp_model_path.exists():
                tmp_model_path.unlink()
            raise

        # Update index
        index = self._load_index()

        # Remove existing entry if present
        index["models"] = [m for m in index["models"] if m["model_id"] != model_id]

        # Add new entry
        index["models"].append(
            {
                "model_id": model_id,
                "file_path": str(model_file),
                "created_at": metadata["created_at"],
                "is_active": True,
            }
        )

        # Set as current model
        index["current_model_id"] = model_id

        # Deactivate other models
        for m in index["models"]:
            if m["model_id"] != model_id:
                m["is_active"] = False

        self._save_index(index)

        logger.info(f"Saved model {model_id} to {model_file}")
        return str(model_file)

    def load_model(self, model_id: str) -> Tuple[Any, Dict[str, Any]]:
        """Load a model and its metadata from disk.

        Performs security checks before deserialization:
        - Validates model_id against path traversal attacks
        - Verifies file integrity via HMAC-SHA256 signature
        - Ensures the model file is within the expected storage directory

        Args:
            model_id: The model identifier to load

        Returns:
            Tuple of (model, metadata)

        Raises:
            FileNotFoundError: If the model file does not exist
            ValueError: If model_id is invalid or integrity check fails
        """
        self._validate_model_id(model_id)

        model_file = self.storage_dir / f"{model_id}.joblib"
        meta_file = self.storage_dir / f"{model_id}.meta.json"

        if not model_file.exists():
            raise FileNotFoundError(f"Model {model_id} not found at {model_file}")

        # Read the model file bytes into memory ONCE.  Both HMAC
        # verification and joblib deserialization operate on this same
        # buffer, eliminating the TOCTOU window between disk-based
        # _verify_file_hmac() and joblib.load() where an attacker could
        # swap the file between the two calls.
        model_bytes = model_file.read_bytes()

        # Load metadata first -- JSON deserialization is safe and gives us
        # the HMAC we need to verify before touching joblib/pickle.
        metadata = {}
        if meta_file.exists():
            with open(meta_file, "r") as f:
                metadata = json.load(f)

        # Determine strict vs. permissive mode for unsigned models.
        # Strict mode (default) refuses to load models without an HMAC
        # signature.  Legacy/permissive mode requires explicit opt-in via
        # LUMINO_STRICT_MODEL_LOADING=false so existing deployments with
        # unsigned models can migrate gracefully.
        strict_mode = os.environ.get(
            "LUMINO_STRICT_MODEL_LOADING", "true"
        ).lower() not in ("false", "0", "no")

        # Verify model file integrity before deserialization.
        # joblib.load() uses pickle internally which can execute arbitrary
        # code, so the file MUST be verified before it is deserialized.
        stored_hmac = metadata.get("model_hmac")
        if stored_hmac:
            if not self._verify_bytes_hmac(model_bytes, stored_hmac):
                raise ValueError(
                    f"Model {model_id} failed integrity verification. "
                    "The model file may have been tampered with. "
                    "Re-train and save the model to generate a valid signature."
                )
            logger.debug(f"Model {model_id} passed integrity verification")
        else:
            if strict_mode:
                raise ValueError(
                    f"Model {model_id} has no HMAC signature and strict "
                    "model loading is enabled (default). Models without "
                    "integrity signatures cannot be loaded. Re-save the "
                    "model to generate an HMAC, or set "
                    "LUMINO_STRICT_MODEL_LOADING=false to allow loading "
                    "unsigned legacy models."
                )
            logger.warning(
                f"Model {model_id} has no HMAC signature. "
                "Loading in permissive mode (LUMINO_STRICT_MODEL_LOADING=false). "
                "This model was saved before integrity verification was added. "
                "Re-save the model to generate an HMAC signature."
            )

        # Deserialize from the in-memory buffer -- never re-read from disk.
        model = joblib.load(io.BytesIO(model_bytes))

        # Update last used time
        metadata["last_used_at"] = datetime.now().isoformat()
        with open(meta_file, "w") as f:
            json.dump(metadata, f, indent=2)

        logger.info(f"Loaded model {model_id} from {model_file}")
        return model, metadata

    def model_exists(self, model_id: str) -> bool:
        """Check if a model exists on disk."""
        try:
            self._validate_model_id(model_id)
        except ValueError:
            return False
        model_file = self.storage_dir / f"{model_id}.joblib"
        return model_file.exists()

    def get_model_metadata(self, model_id: str) -> Optional[Dict[str, Any]]:
        """Get metadata without loading the full model."""
        self._validate_model_id(model_id)
        meta_file = self.storage_dir / f"{model_id}.meta.json"

        if not meta_file.exists():
            return None

        with open(meta_file, "r") as f:
            return json.load(f)

    def get_current_model_id(self) -> Optional[str]:
        """Get the ID of the current active model."""
        index = self._load_index()
        return index.get("current_model_id")

    def list_models(self) -> List[Dict[str, Any]]:
        """List all stored models with metadata."""
        index = self._load_index()
        models = []

        for entry in index.get("models", []):
            metadata = self.get_model_metadata(entry["model_id"])
            if metadata:
                models.append(metadata)

        return models

    def delete_model(self, model_id: str) -> bool:
        """Delete a model and its metadata.

        Args:
            model_id: The model to delete

        Returns:
            True if deleted, False if not found
        """
        self._validate_model_id(model_id)
        model_file = self.storage_dir / f"{model_id}.joblib"
        meta_file = self.storage_dir / f"{model_id}.meta.json"

        deleted = False

        if model_file.exists():
            model_file.unlink()
            deleted = True

        if meta_file.exists():
            meta_file.unlink()
            deleted = True

        # Update index
        index = self._load_index()
        index["models"] = [m for m in index["models"] if m["model_id"] != model_id]

        # Clear current model if it was deleted
        if index.get("current_model_id") == model_id:
            index["current_model_id"] = None
            # Set most recent as current
            if index["models"]:
                index["models"][-1]["is_active"] = True
                index["current_model_id"] = index["models"][-1]["model_id"]

        self._save_index(index)

        if deleted:
            logger.info(f"Deleted model {model_id}")

        return deleted

    def cleanup_old_models(self, max_age_days: int = 30, keep_min: int = 3) -> int:
        """Remove old models, keeping minimum number of recent ones.

        Args:
            max_age_days: Maximum age for models to keep
            keep_min: Minimum number of models to retain

        Returns:
            Number of models deleted
        """
        index = self._load_index()
        models = index.get("models", [])

        if len(models) <= keep_min:
            return 0

        cutoff = datetime.now() - timedelta(days=max_age_days)
        deleted_count = 0

        # Sort by creation time (oldest first)
        models_sorted = sorted(
            models, key=lambda m: m.get("created_at", ""), reverse=False
        )

        # Keep at least keep_min models
        candidates_for_deletion = (
            models_sorted[:-keep_min] if len(models_sorted) > keep_min else []
        )

        for model_entry in candidates_for_deletion:
            try:
                created_at = datetime.fromisoformat(model_entry.get("created_at", ""))
                if created_at < cutoff:
                    if self.delete_model(model_entry["model_id"]):
                        deleted_count += 1
            except (ValueError, TypeError):
                continue

        if deleted_count > 0:
            index["last_cleanup"] = datetime.now().isoformat()
            self._save_index(index)
            logger.info(f"Cleaned up {deleted_count} old models")

        return deleted_count


# ============================================================================
# TRAINING DATA STORE
# ============================================================================


class TrainingDataStore:
    """Persistent storage for training data with labels using SQLite."""

    def __init__(self, storage_dir: Optional[str] = None):
        """Initialize the training data store.

        Args:
            storage_dir: Directory for database. Defaults to ~/.lumino/training_data
        """
        if storage_dir is None:
            storage_dir = str(Path.home() / ".lumino" / "training_data")

        self.storage_dir = Path(storage_dir).expanduser()
        self.storage_dir.mkdir(parents=True, exist_ok=True)
        self.db_path = self.storage_dir / "training_data.db"
        self._init_database()

    def _init_database(self) -> None:
        """Initialize SQLite database schema."""
        conn = sqlite3.connect(str(self.db_path))
        cursor = conn.cursor()

        # Log samples table - includes cluster_id for multi-cluster support
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS log_samples (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                sample_hash TEXT UNIQUE,
                cluster_id TEXT,
                timestamp TEXT,
                namespace TEXT,
                pod_name TEXT,
                features BLOB,
                raw_message TEXT,
                log_level TEXT,
                error_indicators INTEGER,
                message_entropy REAL,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # Failure labels table - includes cluster_id for multi-cluster support
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS failure_labels (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                failure_id TEXT UNIQUE,
                cluster_id TEXT,
                failure_type TEXT,
                severity TEXT,
                namespace TEXT,
                resource_name TEXT,
                resource_type TEXT,
                failure_time TEXT,
                detection_source TEXT,
                error_category TEXT,
                metadata TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # Log-Failure correlation table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS log_failure_correlations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                log_sample_id INTEGER REFERENCES log_samples(id),
                failure_label_id INTEGER REFERENCES failure_labels(id),
                cluster_id TEXT,
                correlation_score REAL,
                time_delta_seconds INTEGER,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(log_sample_id, failure_label_id)
            )
        """)

        # Training run history - includes cluster_id
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS training_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                model_id TEXT,
                cluster_id TEXT,
                training_started TEXT,
                training_completed TEXT,
                samples_used INTEGER,
                labels_used INTEGER,
                performance_metrics TEXT,
                trigger_reason TEXT,
                status TEXT
            )
        """)

        # Create indexes
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_log_samples_namespace ON log_samples(namespace)"
        )
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_log_samples_timestamp ON log_samples(timestamp)"
        )
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_log_samples_cluster ON log_samples(cluster_id)"
        )
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_failure_labels_type ON failure_labels(failure_type)"
        )
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_failure_labels_time ON failure_labels(failure_time)"
        )
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_failure_labels_namespace ON failure_labels(namespace)"
        )
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_failure_labels_cluster ON failure_labels(cluster_id)"
        )

        # Migration: Add cluster_id column to existing tables if not present
        self._migrate_add_cluster_id(cursor)

        conn.commit()
        conn.close()

        logger.debug(f"Initialized training data store at {self.db_path}")

    def _migrate_add_cluster_id(self, cursor: sqlite3.Cursor) -> None:
        """Migrate existing tables to add cluster_id column if not present."""
        tables_to_migrate = [
            "log_samples",
            "failure_labels",
            "log_failure_correlations",
            "training_runs",
        ]

        for table in tables_to_migrate:
            try:
                # Check if cluster_id column exists
                cursor.execute(f"PRAGMA table_info({table})")
                columns = [row[1] for row in cursor.fetchall()]

                if "cluster_id" not in columns:
                    cursor.execute(f"ALTER TABLE {table} ADD COLUMN cluster_id TEXT")
                    logger.info(f"Added cluster_id column to {table} table")
            except sqlite3.Error as e:
                logger.debug(f"Migration for {table}: {e}")

    def _get_connection(self) -> sqlite3.Connection:
        """Get a database connection."""
        return sqlite3.connect(str(self.db_path))

    def store_log_sample(
        self, sample: Dict[str, Any], cluster_id: Optional[str] = None
    ) -> Optional[int]:
        """Store a preprocessed log sample.

        Args:
            sample: Dict with keys: timestamp, namespace, pod_name, features,
                   raw_message, log_level, error_indicators, message_entropy
            cluster_id: Optional cluster identifier. If not provided, auto-detected.

        Returns:
            The sample ID or None if duplicate
        """
        # Get cluster ID if not provided
        if cluster_id is None:
            cluster_id = get_current_cluster_id()

        # Create hash for deduplication - include cluster_id for multi-cluster support
        hash_content = (
            f"{cluster_id}{sample.get('namespace', '')}{sample.get('raw_message', '')}"
        )
        sample_hash = hashlib.md5(hash_content.encode()).hexdigest()

        # Serialize features if present
        features_blob = None
        if "features" in sample:
            features = sample["features"]
            if isinstance(features, np.ndarray):
                features_blob = features.tobytes()
            elif isinstance(features, list):
                features_blob = np.array(features).tobytes()

        conn = self._get_connection()
        cursor = conn.cursor()

        try:
            cursor.execute(
                """
                INSERT OR IGNORE INTO log_samples
                (sample_hash, cluster_id, timestamp, namespace, pod_name, features,
                 raw_message, log_level, error_indicators, message_entropy)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
                (
                    sample_hash,
                    cluster_id,
                    sample.get("timestamp"),
                    sample.get("namespace"),
                    sample.get("pod_name"),
                    features_blob,
                    sample.get("raw_message", "")[:1000],  # Limit message size
                    sample.get("log_level"),
                    sample.get("error_indicators", 0),
                    sample.get("message_entropy", 0.0),
                ),
            )

            conn.commit()
            return cursor.lastrowid if cursor.rowcount > 0 else None
        except sqlite3.Error as e:
            logger.warning(f"Failed to store log sample: {e}")
            return None
        finally:
            conn.close()

    def store_failure_label(
        self, label: Dict[str, Any], cluster_id: Optional[str] = None
    ) -> Optional[int]:
        """Store a failure label.

        Args:
            label: Dict with keys: failure_id, failure_type, severity, namespace,
                  resource_name, resource_type, failure_time, detection_source,
                  error_category, metadata
            cluster_id: Optional cluster identifier. If not provided, auto-detected.

        Returns:
            The label ID or None if duplicate
        """
        # Get cluster ID if not provided
        if cluster_id is None:
            cluster_id = get_current_cluster_id()

        # Generate failure_id if not provided - include cluster_id for uniqueness
        failure_id = label.get("failure_id")
        if not failure_id:
            id_content = f"{cluster_id}{label.get('namespace', '')}{label.get('resource_name', '')}{label.get('failure_time', '')}"
            failure_id = hashlib.md5(id_content.encode()).hexdigest()

        # Serialize metadata
        metadata_str = None
        if "metadata" in label and label["metadata"]:
            metadata_str = json.dumps(label["metadata"])

        conn = self._get_connection()
        cursor = conn.cursor()

        try:
            cursor.execute(
                """
                INSERT OR IGNORE INTO failure_labels
                (failure_id, cluster_id, failure_type, severity, namespace, resource_name,
                 resource_type, failure_time, detection_source, error_category, metadata)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
                (
                    failure_id,
                    cluster_id,
                    label.get("failure_type"),
                    label.get("severity"),
                    label.get("namespace"),
                    label.get("resource_name"),
                    label.get("resource_type"),
                    label.get("failure_time"),
                    label.get("detection_source"),
                    label.get("error_category"),
                    metadata_str,
                ),
            )

            conn.commit()
            return cursor.lastrowid if cursor.rowcount > 0 else None
        except sqlite3.Error as e:
            logger.warning(f"Failed to store failure label: {e}")
            return None
        finally:
            conn.close()

    def store_correlation(
        self,
        log_sample_id: int,
        failure_label_id: int,
        correlation_score: float,
        time_delta_seconds: int,
    ) -> Optional[int]:
        """Store a log-failure correlation."""
        conn = self._get_connection()
        cursor = conn.cursor()

        try:
            cursor.execute(
                """
                INSERT OR IGNORE INTO log_failure_correlations
                (log_sample_id, failure_label_id, correlation_score, time_delta_seconds)
                VALUES (?, ?, ?, ?)
            """,
                (
                    log_sample_id,
                    failure_label_id,
                    correlation_score,
                    time_delta_seconds,
                ),
            )

            conn.commit()
            return cursor.lastrowid if cursor.rowcount > 0 else None
        except sqlite3.Error as e:
            logger.warning(f"Failed to store correlation: {e}")
            return None
        finally:
            conn.close()

    def get_failure_labels_in_window(
        self,
        start_time: datetime,
        end_time: datetime,
        failure_types: Optional[List[str]] = None,
        namespace: Optional[str] = None,
        cluster_id: Optional[str] = None,
        current_cluster_only: bool = True,
    ) -> List[Dict[str, Any]]:
        """Get failure labels within a time window.

        Args:
            start_time: Window start
            end_time: Window end
            failure_types: Optional filter for failure types
            namespace: Optional filter for namespace
            cluster_id: Optional specific cluster to query
            current_cluster_only: If True, only return labels from current cluster (default)

        Returns:
            List of failure label dicts
        """
        conn = self._get_connection()
        cursor = conn.cursor()

        query = """
            SELECT id, failure_id, cluster_id, failure_type, severity, namespace, resource_name,
                   resource_type, failure_time, detection_source, error_category, metadata
            FROM failure_labels
            WHERE failure_time >= ? AND failure_time <= ?
        """
        params = [start_time.isoformat(), end_time.isoformat()]

        # Filter by cluster
        if cluster_id:
            query += " AND cluster_id = ?"
            params.append(cluster_id)
        elif current_cluster_only:
            current = get_current_cluster_id()
            # Include both current cluster and legacy data without cluster_id
            query += " AND (cluster_id = ? OR cluster_id IS NULL)"
            params.append(current)

        if failure_types:
            placeholders = ",".join("?" * len(failure_types))
            query += f" AND failure_type IN ({placeholders})"
            params.extend(failure_types)

        if namespace:
            query += " AND namespace = ?"
            params.append(namespace)

        query += " ORDER BY failure_time DESC"

        cursor.execute(query, params)
        rows = cursor.fetchall()
        conn.close()

        labels = []
        for row in rows:
            metadata = None
            if row[11]:
                try:
                    metadata = json.loads(row[11])
                except json.JSONDecodeError:
                    metadata = {}

            labels.append(
                {
                    "id": row[0],
                    "failure_id": row[1],
                    "cluster_id": row[2],
                    "failure_type": row[3],
                    "severity": row[4],
                    "namespace": row[5],
                    "resource_name": row[6],
                    "resource_type": row[7],
                    "failure_time": row[8],
                    "detection_source": row[9],
                    "error_category": row[10],
                    "metadata": metadata,
                }
            )

        return labels

    def get_training_data(
        self,
        since: Optional[datetime] = None,
        limit: Optional[int] = None,
        namespace: Optional[str] = None,
    ) -> Tuple[List[Dict[str, Any]], int]:
        """Retrieve training data samples.

        Args:
            since: Only get samples after this time
            limit: Maximum samples to return
            namespace: Filter by namespace

        Returns:
            Tuple of (samples list, total count)
        """
        conn = self._get_connection()
        cursor = conn.cursor()

        query = "SELECT * FROM log_samples WHERE 1=1"
        params = []

        if since:
            query += " AND created_at >= ?"
            params.append(since.isoformat())

        if namespace:
            query += " AND namespace = ?"
            params.append(namespace)

        query += " ORDER BY created_at DESC"

        if limit:
            query += " LIMIT ?"
            params.append(limit)

        cursor.execute(query, params)
        rows = cursor.fetchall()

        # Get total count
        count_query = "SELECT COUNT(*) FROM log_samples WHERE 1=1"
        count_params = []
        if since:
            count_query += " AND created_at >= ?"
            count_params.append(since.isoformat())
        if namespace:
            count_query += " AND namespace = ?"
            count_params.append(namespace)

        cursor.execute(count_query, count_params)
        total_count = cursor.fetchone()[0]

        conn.close()

        samples = []
        for row in rows:
            samples.append(
                {
                    "id": row[0],
                    "sample_hash": row[1],
                    "timestamp": row[2],
                    "namespace": row[3],
                    "pod_name": row[4],
                    "features": row[5],
                    "raw_message": row[6],
                    "log_level": row[7],
                    "error_indicators": row[8],
                    "message_entropy": row[9],
                    "created_at": row[10],
                }
            )

        return samples, total_count

    def get_statistics(self, current_cluster_only: bool = False) -> Dict[str, Any]:
        """Get statistics about stored training data.

        Args:
            current_cluster_only: If True, only count data from current cluster

        Returns:
            Dict with statistics
        """
        conn = self._get_connection()
        cursor = conn.cursor()

        stats = {}
        current_cluster = get_current_cluster_id()
        stats["current_cluster"] = current_cluster

        # Build cluster filter (parameterized to prevent SQL injection)
        if current_cluster_only and current_cluster:
            cluster_where = " WHERE (cluster_id = ? OR cluster_id IS NULL)"
            cluster_and = " AND (cluster_id = ? OR cluster_id IS NULL)"
            cluster_params = [current_cluster]
        else:
            cluster_where = ""
            cluster_and = ""
            cluster_params = []

        # Log samples stats
        cursor.execute(
            f"SELECT COUNT(*) FROM log_samples{cluster_where}", cluster_params
        )
        stats["total_log_samples"] = cursor.fetchone()[0]

        cursor.execute(
            f"SELECT COUNT(DISTINCT namespace) FROM log_samples{cluster_where}",
            cluster_params,
        )
        stats["unique_namespaces"] = cursor.fetchone()[0]

        # Cluster distribution for log samples
        cursor.execute(
            "SELECT cluster_id, COUNT(*) FROM log_samples GROUP BY cluster_id"
        )
        stats["log_samples_by_cluster"] = {
            (k or "legacy"): v for k, v in cursor.fetchall()
        }

        # Failure labels stats
        cursor.execute(
            f"SELECT COUNT(*) FROM failure_labels{cluster_where}", cluster_params
        )
        stats["total_failure_labels"] = cursor.fetchone()[0]

        cursor.execute(
            f"SELECT failure_type, COUNT(*) FROM failure_labels{' WHERE 1=1' + cluster_and if cluster_and else ''} GROUP BY failure_type",
            cluster_params,
        )
        stats["failure_types"] = dict(cursor.fetchall())

        cursor.execute(
            f"SELECT severity, COUNT(*) FROM failure_labels{' WHERE 1=1' + cluster_and if cluster_and else ''} GROUP BY severity",
            cluster_params,
        )
        stats["severity_distribution"] = dict(cursor.fetchall())

        # Cluster distribution for failure labels
        cursor.execute(
            "SELECT cluster_id, COUNT(*) FROM failure_labels GROUP BY cluster_id"
        )
        stats["failure_labels_by_cluster"] = {
            (k or "legacy"): v for k, v in cursor.fetchall()
        }

        # Correlations
        cursor.execute("SELECT COUNT(*) FROM log_failure_correlations")
        stats["total_correlations"] = cursor.fetchone()[0]

        # Training runs
        cursor.execute("SELECT COUNT(*) FROM training_runs")
        stats["total_training_runs"] = cursor.fetchone()[0]

        cursor.execute(
            "SELECT MAX(training_completed) FROM training_runs WHERE status = 'completed'"
        )
        result = cursor.fetchone()
        stats["last_training"] = result[0] if result else None

        conn.close()
        return stats

    def record_training_run(
        self,
        model_id: str,
        samples_used: int,
        labels_used: int,
        performance_metrics: Dict[str, float],
        trigger_reason: str,
        status: str = "completed",
    ) -> int:
        """Record a training run in history."""
        conn = self._get_connection()
        cursor = conn.cursor()

        cursor.execute(
            """
            INSERT INTO training_runs
            (model_id, training_started, training_completed, samples_used,
             labels_used, performance_metrics, trigger_reason, status)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
            (
                model_id,
                datetime.now().isoformat(),
                datetime.now().isoformat(),
                samples_used,
                labels_used,
                json.dumps(performance_metrics),
                trigger_reason,
                status,
            ),
        )

        conn.commit()
        run_id = cursor.lastrowid
        conn.close()

        return run_id

    def cleanup_old_data(self, max_age_days: int = 90) -> int:
        """Remove old training data.

        Args:
            max_age_days: Maximum age for data to keep

        Returns:
            Number of records deleted
        """
        cutoff = (datetime.now() - timedelta(days=max_age_days)).isoformat()

        conn = self._get_connection()
        cursor = conn.cursor()

        deleted = 0

        # Delete old correlations first (foreign key constraint)
        cursor.execute(
            """
            DELETE FROM log_failure_correlations
            WHERE log_sample_id IN (
                SELECT id FROM log_samples WHERE created_at < ?
            )
        """,
            (cutoff,),
        )
        deleted += cursor.rowcount

        # Delete old log samples
        cursor.execute("DELETE FROM log_samples WHERE created_at < ?", (cutoff,))
        deleted += cursor.rowcount

        # Delete old failure labels
        cursor.execute("DELETE FROM failure_labels WHERE created_at < ?", (cutoff,))
        deleted += cursor.rowcount

        conn.commit()
        conn.close()

        if deleted > 0:
            logger.info(f"Cleaned up {deleted} old training data records")

        return deleted


# ============================================================================
# FAILURE EVENT COLLECTOR
# ============================================================================


class FailureEventCollector:
    """Collects failure events from various sources for training labels."""

    # Mapping from Kubernetes event reasons to failure types
    FAILURE_REASON_MAP = {
        "OOMKilled": "oom",
        "CrashLoopBackOff": "crash",
        "ImagePullBackOff": "image",
        "ErrImagePull": "image",
        "FailedScheduling": "scheduling",
        "FailedMount": "storage",
        "FailedAttachVolume": "storage",
        "FailedCreatePodContainer": "config",
        "CreateContainerConfigError": "config",
        "Unhealthy": "health",
        "BackOff": "crash",
        "Failed": "general",
        "Error": "general",
        "FailedKillPod": "general",
        "NetworkNotReady": "network",
        "FailedCreatePodSandBox": "network",
    }

    # Severity mapping
    SEVERITY_MAP = {
        "oom": "critical",
        "crash": "high",
        "image": "medium",
        "scheduling": "medium",
        "storage": "high",
        "config": "medium",
        "health": "medium",
        "network": "high",
        "general": "medium",
    }

    def __init__(self, training_store: TrainingDataStore):
        """Initialize the failure event collector.

        Args:
            training_store: The training data store instance
        """
        self.training_store = training_store

    def collect_from_events(self, events: List[Dict[str, Any]], namespace: str) -> int:
        """Collect failure labels from Kubernetes events.

        Args:
            events: List of event dicts from Kubernetes API
            namespace: The namespace these events belong to

        Returns:
            Number of failure labels stored
        """
        stored = 0

        for event in events:
            # Skip non-warning events
            event_type = event.get("type", "Normal")
            if event_type != "Warning":
                continue

            reason = event.get("reason", "")
            failure_type = self.FAILURE_REASON_MAP.get(reason)

            if not failure_type:
                # Check message for error patterns
                message = event.get("message", "").lower()
                if any(word in message for word in ["error", "failed", "crash", "oom"]):
                    failure_type = "general"
                else:
                    continue

            # Extract event details
            involved_object = event.get("involved_object", {})

            label = {
                "failure_type": failure_type,
                "severity": self.SEVERITY_MAP.get(failure_type, "medium"),
                "namespace": namespace,
                "resource_name": involved_object.get(
                    "name", event.get("name", "unknown")
                ),
                "resource_type": involved_object.get("kind", "unknown").lower(),
                "failure_time": event.get("last_timestamp")
                or event.get("first_timestamp"),
                "detection_source": "kubernetes_event",
                "error_category": failure_type,
                "metadata": {
                    "reason": reason,
                    "message": event.get("message", "")[:500],
                    "count": event.get("count", 1),
                },
            }

            if self.training_store.store_failure_label(label):
                stored += 1

        if stored > 0:
            logger.debug(
                f"Collected {stored} failure labels from events in {namespace}"
            )

        return stored

    def collect_from_pipeline_runs(
        self, pipeline_runs: List[Dict[str, Any]], namespace: str
    ) -> int:
        """Collect failure labels from failed pipeline runs.

        Args:
            pipeline_runs: List of PipelineRun dicts
            namespace: The namespace

        Returns:
            Number of failure labels stored
        """
        stored = 0

        for pr in pipeline_runs:
            status = pr.get("status", {})
            conditions = status.get("conditions", [])

            # Check if pipeline failed
            is_failed = False
            failure_message = ""

            for condition in conditions:
                if condition.get("type") == "Succeeded":
                    if condition.get("status") == "False":
                        is_failed = True
                        failure_message = condition.get("message", "")
                    break

            if not is_failed:
                continue

            metadata = pr.get("metadata", {})

            # Determine failure type from message
            failure_type = "pipeline_failure"
            message_lower = failure_message.lower()
            if "timeout" in message_lower:
                failure_type = "timeout"
            elif "image" in message_lower:
                failure_type = "image"
            elif "permission" in message_lower or "denied" in message_lower:
                failure_type = "permission"
            elif "resource" in message_lower or "quota" in message_lower:
                failure_type = "resource_limits"

            label = {
                "failure_type": failure_type,
                "severity": "high",
                "namespace": namespace,
                "resource_name": metadata.get("name", "unknown"),
                "resource_type": "pipelinerun",
                "failure_time": status.get("completionTime")
                or metadata.get("creationTimestamp"),
                "detection_source": "pipeline_status",
                "error_category": failure_type,
                "metadata": {
                    "pipeline": pr.get("spec", {})
                    .get("pipelineRef", {})
                    .get("name", "unknown"),
                    "message": failure_message[:500],
                },
            }

            if self.training_store.store_failure_label(label):
                stored += 1

        if stored > 0:
            logger.debug(
                f"Collected {stored} failure labels from pipeline runs in {namespace}"
            )

        return stored

    def collect_from_pod_status(self, pods: List[Any], namespace: str) -> int:
        """Collect failure labels from pod statuses.

        Args:
            pods: List of V1Pod objects
            namespace: The namespace

        Returns:
            Number of failure labels stored
        """
        stored = 0

        for pod in pods:
            try:
                pod_name = pod.metadata.name
                pod_status = pod.status

                if not pod_status:
                    continue

                # Check phase
                if pod_status.phase not in ["Failed", "Unknown"]:
                    # Check container statuses for issues
                    container_statuses = pod_status.container_statuses or []

                    for cs in container_statuses:
                        # Check for crash loop
                        if cs.restart_count > 3:
                            label = {
                                "failure_type": "crash",
                                "severity": "high",
                                "namespace": namespace,
                                "resource_name": pod_name,
                                "resource_type": "pod",
                                "failure_time": datetime.now().isoformat(),
                                "detection_source": "pod_status",
                                "error_category": "crash",
                                "metadata": {
                                    "container": cs.name,
                                    "restart_count": cs.restart_count,
                                },
                            }

                            if self.training_store.store_failure_label(label):
                                stored += 1

                        # Check waiting state
                        if cs.state and cs.state.waiting:
                            reason = cs.state.waiting.reason or ""
                            failure_type = self.FAILURE_REASON_MAP.get(reason)

                            if failure_type:
                                label = {
                                    "failure_type": failure_type,
                                    "severity": self.SEVERITY_MAP.get(
                                        failure_type, "medium"
                                    ),
                                    "namespace": namespace,
                                    "resource_name": pod_name,
                                    "resource_type": "pod",
                                    "failure_time": datetime.now().isoformat(),
                                    "detection_source": "pod_status",
                                    "error_category": failure_type,
                                    "metadata": {
                                        "container": cs.name,
                                        "reason": reason,
                                        "message": cs.state.waiting.message or "",
                                    },
                                }

                                if self.training_store.store_failure_label(label):
                                    stored += 1

                        # Check terminated state for OOM
                        if cs.state and cs.state.terminated:
                            reason = cs.state.terminated.reason or ""
                            if reason == "OOMKilled":
                                label = {
                                    "failure_type": "oom",
                                    "severity": "critical",
                                    "namespace": namespace,
                                    "resource_name": pod_name,
                                    "resource_type": "pod",
                                    "failure_time": datetime.now().isoformat(),
                                    "detection_source": "pod_status",
                                    "error_category": "oom",
                                    "metadata": {
                                        "container": cs.name,
                                        "exit_code": cs.state.terminated.exit_code,
                                    },
                                }

                                if self.training_store.store_failure_label(label):
                                    stored += 1

                else:
                    # Pod is in Failed or Unknown state
                    label = {
                        "failure_type": "pod_failure",
                        "severity": "high",
                        "namespace": namespace,
                        "resource_name": pod_name,
                        "resource_type": "pod",
                        "failure_time": datetime.now().isoformat(),
                        "detection_source": "pod_status",
                        "error_category": "general",
                        "metadata": {
                            "phase": pod_status.phase,
                            "reason": pod_status.reason or "",
                        },
                    }

                    if self.training_store.store_failure_label(label):
                        stored += 1

            except Exception as e:
                logger.debug(f"Error processing pod status: {e}")
                continue

        if stored > 0:
            logger.debug(
                f"Collected {stored} failure labels from pod statuses in {namespace}"
            )

        return stored

    def correlate_logs_with_failures(
        self,
        log_samples: List[Dict[str, Any]],
        failures: List[Dict[str, Any]],
        time_window_minutes: int = 30,
    ) -> List[Dict[str, Any]]:
        """Correlate log samples with failure events by time proximity.

        Args:
            log_samples: List of log sample dicts with 'timestamp' and 'namespace'
            failures: List of failure label dicts with 'failure_time' and 'namespace'
            time_window_minutes: Time window for correlation (logs before failure)

        Returns:
            List of correlation dicts with log_sample_id, failure_label_id, score
        """
        correlations = []
        window_seconds = time_window_minutes * 60

        for log in log_samples:
            log_time_str = log.get("timestamp")
            log_namespace = log.get("namespace")
            log_id = log.get("id")

            if not log_time_str or not log_id:
                continue

            try:
                # Parse log timestamp
                log_time = datetime.fromisoformat(log_time_str.replace("Z", "+00:00"))
            except (ValueError, TypeError):
                continue

            for failure in failures:
                failure_time_str = failure.get("failure_time")
                failure_namespace = failure.get("namespace")
                failure_id = failure.get("id")

                if not failure_time_str or not failure_id:
                    continue

                # Only correlate within same namespace
                if log_namespace != failure_namespace:
                    continue

                try:
                    failure_time = datetime.fromisoformat(
                        failure_time_str.replace("Z", "+00:00")
                    )
                except (ValueError, TypeError):
                    continue

                # Check if log is before failure within window
                time_delta = (failure_time - log_time).total_seconds()

                if 0 <= time_delta <= window_seconds:
                    # Calculate correlation score (higher for logs closer to failure)
                    # Score ranges from 0.5 (at window edge) to 1.0 (at failure time)
                    score = 0.5 + 0.5 * (1 - time_delta / window_seconds)

                    correlation = {
                        "log_sample_id": log_id,
                        "failure_label_id": failure_id,
                        "correlation_score": score,
                        "time_delta_seconds": int(time_delta),
                    }

                    correlations.append(correlation)

                    # Store correlation in database
                    self.training_store.store_correlation(
                        log_id, failure_id, score, int(time_delta)
                    )

        logger.debug(f"Created {len(correlations)} log-failure correlations")
        return correlations


# ============================================================================
# MODEL VERSION MANAGER
# ============================================================================


class ModelVersionManager:
    """Manages model versions and decides when to retrain."""

    def __init__(
        self,
        persistence_manager: ModelPersistenceManager,
        training_store: TrainingDataStore,
    ):
        """Initialize the model version manager.

        Args:
            persistence_manager: The model persistence manager
            training_store: The training data store
        """
        self.persistence = persistence_manager
        self.training_store = training_store
        self._predictions_cache: List[Dict[str, Any]] = []

    def get_current_model_id(self) -> Optional[str]:
        """Get the ID of the current active model."""
        return self.persistence.get_current_model_id()

    def should_retrain(
        self,
        model_id: str,
        performance_threshold: float = 0.65,
        max_age_hours: int = 24,
        new_data_threshold: int = 1000,
    ) -> Tuple[bool, str]:
        """Determine if model should be retrained.

        Args:
            model_id: The model to check
            performance_threshold: Minimum acceptable accuracy
            max_age_hours: Maximum model age before retraining
            new_data_threshold: Number of new samples that triggers retraining

        Returns:
            Tuple of (should_retrain, reason)
        """
        # Check if model exists
        if not self.persistence.model_exists(model_id):
            return True, "no_model"

        metadata = self.persistence.get_model_metadata(model_id)
        if not metadata:
            return True, "no_metadata"

        # Check age
        created_at_str = metadata.get("created_at")
        if created_at_str:
            try:
                created_at = datetime.fromisoformat(created_at_str)
                age_hours = (datetime.now() - created_at).total_seconds() / 3600

                if age_hours > max_age_hours:
                    return True, "age_exceeded"
            except (ValueError, TypeError):
                pass

        # Check performance
        performance = metadata.get("performance_metrics", {})
        accuracy = performance.get("accuracy", 0)

        if accuracy < performance_threshold:
            return True, "performance_degraded"

        # Check for new data
        training_samples = metadata.get("training_samples", 0)
        stats = self.training_store.get_statistics()
        current_samples = stats.get("total_log_samples", 0)

        new_samples = current_samples - training_samples
        if new_samples >= new_data_threshold:
            return True, "new_data_available"

        return False, "model_valid"

    def generate_new_model_id(self) -> str:
        """Generate a new model ID with timestamp."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        return f"predictive_log_v1_{timestamp}"

    def record_prediction(
        self,
        model_id: str,
        prediction: Dict[str, Any],
        actual_outcome: Optional[bool] = None,
    ) -> None:
        """Record a prediction for performance tracking.

        Args:
            model_id: The model that made the prediction
            prediction: The prediction result
            actual_outcome: Optional actual outcome (for validation)
        """
        record = {
            "model_id": model_id,
            "timestamp": datetime.now().isoformat(),
            "prediction": prediction,
            "actual_outcome": actual_outcome,
        }

        self._predictions_cache.append(record)

        # Keep cache limited
        if len(self._predictions_cache) > 1000:
            self._predictions_cache = self._predictions_cache[-500:]

    def calculate_model_performance(self, model_id: str) -> Dict[str, float]:
        """Calculate current model performance from recorded predictions.

        Note: This requires actual outcomes to be recorded, which happens
        when predictions are validated against actual failures.
        """
        model_predictions = [
            p
            for p in self._predictions_cache
            if p["model_id"] == model_id and p.get("actual_outcome") is not None
        ]

        if not model_predictions:
            return {"accuracy": 0.0, "precision": 0.0, "recall": 0.0, "sample_size": 0}

        # Calculate metrics
        true_positives = sum(
            1
            for p in model_predictions
            if p["actual_outcome"] and len(p["prediction"].get("predictions", [])) > 0
        )

        false_positives = sum(
            1
            for p in model_predictions
            if not p["actual_outcome"]
            and len(p["prediction"].get("predictions", [])) > 0
        )

        false_negatives = sum(
            1
            for p in model_predictions
            if p["actual_outcome"] and len(p["prediction"].get("predictions", [])) == 0
        )

        true_negatives = sum(
            1
            for p in model_predictions
            if not p["actual_outcome"]
            and len(p["prediction"].get("predictions", [])) == 0
        )

        total = len(model_predictions)
        accuracy = (true_positives + true_negatives) / total if total > 0 else 0.0
        precision = (
            true_positives / (true_positives + false_positives)
            if (true_positives + false_positives) > 0
            else 0.0
        )
        recall = (
            true_positives / (true_positives + false_negatives)
            if (true_positives + false_negatives) > 0
            else 0.0
        )

        return {
            "accuracy": accuracy,
            "precision": precision,
            "recall": recall,
            "sample_size": total,
        }


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================


def build_labels_from_correlations(
    correlations: List[Dict[str, Any]], num_samples: int
) -> np.ndarray:
    """Build a binary label array from log-failure correlations.

    Args:
        correlations: List of correlation dicts with log_sample_id
        num_samples: Total number of log samples

    Returns:
        Binary array where 1 = correlated with failure, 0 = normal
    """
    labels = np.zeros(num_samples, dtype=np.int32)

    for corr in correlations:
        sample_id = corr.get("log_sample_id")
        if sample_id is not None and 0 <= sample_id < num_samples:
            labels[sample_id] = 1

    return labels


def parse_time_period(period: str) -> timedelta:
    """Parse a time period string into timedelta.

    Args:
        period: Time period like "1h", "24h", "7d", "30d"

    Returns:
        timedelta object
    """
    period = period.lower().strip()

    if period.endswith("h"):
        hours = int(period[:-1])
        return timedelta(hours=hours)
    elif period.endswith("d"):
        days = int(period[:-1])
        return timedelta(days=days)
    elif period.endswith("m"):
        minutes = int(period[:-1])
        return timedelta(minutes=minutes)
    else:
        # Default to hours
        try:
            hours = int(period)
            return timedelta(hours=hours)
        except ValueError:
            return timedelta(hours=24)

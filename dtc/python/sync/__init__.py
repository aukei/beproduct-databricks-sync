"""
Sync utilities for DTC integration.

Provides:
- phase1: Pure-Python BeProduct -> DTC upsert core (no Spark; locally testable)
- phase2: Pure-Python DTC -> BeProduct pushback core (no Spark; locally testable)
- snapshot: Snapshot management and creation (requires pyspark)
- change_detection: Change detection and audit trail (requires pyspark)

The Spark-dependent modules are imported lazily so that `phase1`/`phase2` (which
have no Spark dependency) can be imported and unit-tested outside Databricks.
"""

from . import phase1
from . import phase2

__all__ = ["phase1", "phase2"]

try:  # pragma: no cover - only available inside Databricks/PySpark
    from .snapshot import SnapshotManager
    from .change_detection import ChangeDetector
    __all__ += ["SnapshotManager", "ChangeDetector"]
except Exception:  # pyspark not installed (e.g. local dev / unit tests)
    SnapshotManager = None
    ChangeDetector = None

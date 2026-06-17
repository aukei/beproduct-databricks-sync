"""
Sync utilities for DTC integration.

Provides:
- phase1: Pure-Python BeProduct -> DTC upsert core (no Spark; locally testable)
- phase2: Pure-Python DTC -> BeProduct pushback core (no Spark; locally testable)

Both modules are Spark-free so they can be imported and unit-tested outside
Databricks. Notebooks supply the Spark/IO wrappers around them.
"""

from . import phase1
from . import phase2

__all__ = ["phase1", "phase2"]

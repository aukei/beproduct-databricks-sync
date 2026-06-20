# Databricks notebook source
"""
Wait Cluster — Cluster Warm-up Sentinel
========================================

This is a deliberately trivial task whose ONLY purpose is to be the first
notebook scheduled on the shared job cluster. Because it has no dependencies
it fires immediately when the job starts and absorbs the cluster cold-start
time into its own task duration, keeping that latency out of Step 1 (bp_style_sync)
and Step 3 (pull_dtc).

The two parallel chains (BeProduct and DTC) both depend on this task, so by the
time they begin the cluster is guaranteed to be warm.

Nothing is read or written here. The only meaningful side-effect is the
``dbutils.jobs.taskValues.set`` call that stamps the cluster startup finish
timestamp for optional monitoring.
"""

# COMMAND ----------

import time

_t0 = time.time()

# Trigger a trivial Spark action so that the executor JVM is initialised and
# included in the warm-up window, not deferred to Step 1.
spark.range(1).count()

elapsed = round(time.time() - _t0, 1)
print(f"Cluster warm — executor ready in {elapsed}s")

# COMMAND ----------

dbutils.jobs.taskValues.set(key="cluster_ready_elapsed_s", value=elapsed)

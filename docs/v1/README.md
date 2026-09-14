# v1 documentation archive

These are the **v1 pipeline documents**, moved here unchanged when the `v2` branch
consolidated the doc set. Nothing was deleted — every verified discovery, live
case study and hard-won bug fix recorded here is still the historical record.

**For current behaviour, read the v2 docs instead:**

| Looking for | Read |
|---|---|
| What runs, in what order, and what gates a row | [../PIPELINE.md](../PIPELINE.md) |
| Which field goes which way, keys, PATCH allow-list | [../SYNC_CONTRACT.md](../SYNC_CONTRACT.md) |
| Why v2 exists, what changed, rollout and rollback | [../MIGRATION_V1_V2.md](../MIGRATION_V1_V2.md) |
| Systems, repo layout, Delta data model | [../ARCHITECTURE.md](../ARCHITECTURE.md) |

## What is in here

| File | Superseded by |
|---|---|
| `PHASE0_WORKFLOW.md` | PIPELINE.md — Stage 00 |
| `PHASE1_WORKFLOW.md` | PIPELINE.md — Stages 20/25/40; SYNC_CONTRACT.md |
| `PHASE2_WORKFLOW.md` | PIPELINE.md — Stage 50; SYNC_CONTRACT.md |
| `PHASE3_WORKFLOW.md` | PIPELINE.md — companion jobs |
| `PHASE5_WORKFLOW.md` | PIPELINE.md — Stage 00 (master-data/directory modes) |
| `PHASE7_WORKFLOW.md` | SYNC_CONTRACT.md — sample submit history |
| `PHASE9_WORKFLOW.md` | PIPELINE.md — Stage 30 + `duty_compute` |
| `PHASE10_WORKFLOW.md` | PIPELINE.md — Stages 20/40 |
| `DIAGRAM.md` | PIPELINE.md — "The DAG" |
| `PIPELINE_GATES.md` | PIPELINE.md — per-stage "Gates" subsections |

## Why the phase numbering was retired

v2 merges Phases 1, 7, 10 and 9b's push half into a single write stage, so the
phase numbers stopped mapping onto anything that runs. The v1 → v2 task map at the
end of [../PIPELINE.md](../PIPELINE.md) translates between them.

Phase numbers still appear throughout AGENTS.md's verified-discoveries and
decisions logs, which are append-only history and were deliberately not rewritten.
Use the task map when reading them.

## Still-live artifacts described here

Some notebooks these documents describe are unchanged in v2 and still deployed —
`p0_*`, `p1p7_beproduct_style_sync`, `p1_pull_masters_to_delta`,
`p1_dtc_request_manager`, `p9a_pull_lineplan_to_delta`, `p9b1_compute_duty_rates`,
`p2_push_dtc_to_beproduct`, `p3_beproduct_to_dtc_images`. Where these docs describe
those notebooks' internals they remain accurate; where they describe the DAG, the
ordering or the write path, they do not.

Retired entirely, documented here for history only: Phase 8a/8b (DTC FABRIC →
Material Master, replaced by the separate "MaterialLib" application, 2026-09-01;
tables dropped).

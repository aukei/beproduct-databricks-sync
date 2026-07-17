# Phase 7: BeProduct Sample-App Submit History → DTC

**Status:** Implemented ✅ (pure-Python formatter unit-tested, all 6 DTC columns
confirmed in the 198-field WIP_ITS_USE view, 2026-07-07)

Phase 7 enriches the Phase 1 DTC push with the **complete sample-app submit
history** for each style. It follows the same shape as Phase 3 (image upload):
it is not a separate DAG step but an extension of the Phase 1 transform + push
pipeline.

> Phase 1 field push: `PHASE1_WORKFLOW.md`.
> Phase 3 image upload: `PHASE3_WORKFLOW.md`.
> Data model: `docs/ARCHITECTURE.md`. Field-mapping SSOT: `beproduct_style_interested_fields.txt`.

---

## What it does

BeProduct stores, per style, up to **6 sample applications** of type
`SampleRequestMulti` — one for each stage:

| App title | Prefix | DTC column |
|-----------|--------|------------|
| Proto Sample | `proto` | `Proto Sample - Sample Status` |
| PreLine Sample | `preline` | `Pre-line Sample - Status` *(lowercase 'l', dash)* |
| SMS Sample | `sms` | `SMS - Sample Status` |
| Fit Sample | `fit` | `1st Fit Sample Approval Status` |
| PP Sample | `pp` | `2nd Fit Sample Approval Status` |
| TOP Sample | `top` | `TOP Sample Approval Status` |

For each app, Phase 7 reads the **complete list of submit rounds** for that style
and writes a single DTC cell containing a compact JSON array — one triple per
submit taken from that submit's **first size**:

```
[submit_name, submitStatus, submitStatusDate]
```

Example (Boy Short Sleeve Tee — PP Sample):
```json
[["1ST Submit","Approved with Corrections","2026-05-11T11:39:48.528Z"]]
```

Empty history (no submits yet) → `""` (value skipped by `phase1.norm`, not pushed).

---

## Data flow

```
BeProduct API (app_get × 6 per style)
       ↓  beproduct_style_sync.py  (Step 1 in DAG)
  ktb_styles.{proto,preline,sms,fit,pp,top}_sample_json
       ↓  (raw JSON arrays of submit×size records, '[]' when empty)
  beproduct_to_dtc_transform.py  (Step 2, format_sample_udf)
       ↓  sync.samples.format_sample_field()  — pure Python, unit-tested
  beproduct_to_dtc_staging.{proto,preline,sms,fit,pp,top}_sample_status
       ↓  beproduct_to_dtc_push.py  (Step 5 — same push as Phase 1)
  DTC WIP_ITS_USE  sample status columns
```

Phase 7 **rides the Phase 1 push** — no separate DAG task is needed. The sample
status columns appear in `phase1.FIELD_MAPPING` alongside the regular Phase 1
style fields.

---

## BeProduct API shape

```python
# Per style, per app (called in parallel, 10 workers):
resp = api.style.app_get(header_id=hid, app_id=aid)
# → resp["data"]["submits"][n]["sizes"][m] contains:
#     submitStatus, submitStatusDate, dueDate, receivedDate, fitDate
```

**Key facts (AGENTS.md, validated 2026-06-19):**
- `app.modifiedAt` is **independent** of `style.modifiedAt` — editing a sample
  does NOT bump the style. Therefore Step 1 runs **FULL** daily (not incremental)
  to capture sample-only changes.
- App IDs are **constant per folder** (not per style). Cached in
  `beproduct_style_app_registry` by `00_init_style_app_registry`. Do NOT call
  `app_list()` per style.
- `app.modifiedAt == "0001-01-01T00:00:00"` means the app exists but has no data.

---

## Formatter: `sync.samples.format_sample_field`

Module: `dtc/python/sync/samples.py`. Unit-tested: `dtc/tests/test_samples.py`.

```python
from sync.samples import format_sample_field, SAMPLE_SUBMIT_FIELDS

# Raw input: the ktb_styles.{prefix}_sample_json column
raw = '[{"submit_id":"s1","submit_name":"1ST Submit","size":"S",' \
      '"submit_status":"Approved","submit_status_date":"2026-05-01T00:00:00Z",...}]'

# Output: compact JSON, first size per submit, ordered by submit
result = format_sample_field(raw)
# → '[["1ST Submit","Approved","2026-05-01T00:00:00Z"]]'
```

**Rules:**
- Records are grouped by `submit_id` (falls back to `submit_name` if absent).
- Only the **first size** of each submit is used (first record per submit_id in
  the flattened array).
- Multiple submit rounds → multiple triples in the array (order preserved).
- Compact separators (`","` and `":"`) so `phase1.norm()` is stable — no
  incidental whitespace that would cause false diff re-pushes.

---

## SAMPLE_SUBMIT_FIELDS mapping

Defined in `dtc/python/sync/samples.py`:

```python
SAMPLE_SUBMIT_FIELDS = {
    "proto_sample_json":   {"staging": "proto_sample_status",   "dtc": "Proto Sample - Sample Status"},
    "preline_sample_json": {"staging": "preline_sample_status", "dtc": "Pre-line Sample - Status"},
    "sms_sample_json":     {"staging": "sms_sample_status",     "dtc": "SMS - Sample Status"},
    "fit_sample_json":     {"staging": "fit_sample_status",     "dtc": "1st Fit Sample Approval Status"},
    "pp_sample_json":      {"staging": "pp_sample_status",      "dtc": "2nd Fit Sample Approval Status"},
    "top_sample_json":     {"staging": "top_sample_status",     "dtc": "TOP Sample Approval Status"},
}
```

The staging column names flow directly into `phase1.FIELD_MAPPING`; updating
`SAMPLE_SUBMIT_FIELDS` automatically keeps both in sync (verified by
`test_samples.py` test [11]).

---

## Transform wiring (Spark UDF)

In `beproduct_to_dtc_transform.py`:

```python
from sync.samples import format_sample_field, SAMPLE_SUBMIT_FIELDS
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

format_sample_udf = udf(format_sample_field, StringType())

for _raw_col, _spec in SAMPLE_SUBMIT_FIELDS.items():
    _staging_col = _spec["staging"]
    if _raw_col in df_with_request_name.columns:
        df_with_request_name = df_with_request_name.withColumn(
            _staging_col, format_sample_udf(col(_raw_col))
        )
    else:
        df_with_request_name = df_with_request_name.withColumn(_staging_col, lit(""))
```

---

## DTC column notes

All 6 DTC columns confirmed present in the 198-field `WIP_ITS_USE` view
(2026-07-07). Important naming gotcha:

- `"Pre-line Sample - Status"` uses **lowercase `l`** in "line" and a **dash**
  separator. Not "Pre-Line" or "Pre-Line Sample Submission Status".

Phase 1 push treats these like any other updatable field — they are overwritten
on every push (not default-fill). An empty string (`""`) is treated as `None`
by `phase1.norm()` and skipped, so styles with no sample data do not blank out
existing DTC values.

---

## Good test candidates (rich sample data)

| Style | Apps with data |
|-------|---------------|
| `Iris - Test- Top-111` | Proto (Approved), Fit (Requested), TOP (Approved) |
| `Boy  Short Sleeve Tee` | Proto (Approved with Corrections), PP (Approved with Corrections) |
| `HOODED-K263` | Proto (Requested, 27 POMs) |

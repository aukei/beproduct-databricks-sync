# Phase 3: BeProduct Image → DTC "Style Image"

**Status**: Implemented ✅ (core unit-tested + live-verified on UAT 2026-06-18)

Phase 3 uploads each BeProduct front image into the matching DTC request's
**Style Image** cell. It is a separate step that runs **after** Phase 1, because
the Style Image is **binary** and cannot ride the Phase 1 JSON `sheetData` PATCH,
and its endpoint targets an **existing row** by `rowIndex` (so the row must exist
first).

Image sync stays strictly **BeProduct → DTC** and one-directional: it never clears
a DTC image and never reads one back into BeProduct. Already-imaged rows are left
untouched, so re-runs are idempotent.

> Forward field sync: `PHASE1_WORKFLOW.md`. Reverse fields: `PHASE2_WORKFLOW.md`.
> Data model: `docs/ARCHITECTURE.md`.

---

## Why a separate step

| Constraint | Consequence |
|------------|-------------|
| Style Image is binary | Can't be set via the JSON `sheetData` PATCH used for text columns |
| Image endpoint keys off `rowindex` on an **existing** row | Must run after Phase 1 has created/updated the rows |
| DTC accepts jpg/png, **rejects webp (HTTP 400)** | Non-native raster types are transcoded to PNG before upload |

---

## Flow

```
(after p1p7_beproduct_to_dtc_push, Phase 1)
1. Refresh dtc_wip_<customer>           dtc/notebooks/p1_pull_masters_to_delta.py
   (so it reflects rows Phase 1 inserted)
2. Image upload                         beproduct/p3_beproduct_to_dtc_images.py
   → DTC "Style Image" cell (blank cells only)
```

`p3_beproduct_to_dtc_images.py`, per in-scope resolved request:

1. **Reloads the sheet live** (`connector.get_sheet`) for the freshest `rowIndex`
   and current Style Image state (it therefore also sees rows Phase 1 just
   inserted, independent of step 1's table refresh).
2. Matches each DTC row to its BeProduct staging row on the in-request key
   `(BP Style#, Color / Wash)` (Phase 6; was `LF Style#`).
3. **Sibling-copy check (added 2026-09-03, owner spec)**: for every
   blank-image row, first checks whether ANY OTHER row with the SAME `BP
   Style#` in this request already has a real Style Image (a style's front
   image is a HEADER-level BeProduct attribute — one per style, not per
   colorway — so every colorway is expected to carry the same image). If a
   sibling has one, its own already-uploaded DTC-hosted image URL is reused
   as the source — no BeProduct CDN download at all for this row.
4. Otherwise (no sibling has an image yet), falls back to the original path:
   for a row that is **blank-image AND** whose BeProduct staging row has a
   **valid `front_image_url`**: downloads the image from the BeProduct CDN,
   classifies it, transcodes if needed, then POSTs it to the DTC image
   endpoint. (Both paths POST via the same multipart upload mechanics —
   only WHICH url is downloaded differs; see `ImageUploadOp.source`.)
5. Logs every decision (uploaded / converted / skipped / failed) to
   `lft.beproduct.beproduct_to_dtc_sync_log` with `stage='images'`.

Decision logic is the pure, unit-tested `dtc/python/sync/phase3.py`
(`compute_image_uploads`, `classify_image_type`); all HTTP/Spark lives in the
notebook and `connectors/dtc.py`.

---

## Upload rule (per row)

Upload an image only when the DTC row's **Style Image** cell is **not
populated**, the row has a `rowIndex`, AND EITHER:

- another row with the SAME **BP Style#** in this request already has a
  real Style Image (source = sibling's own DTC-hosted URL, no BeProduct
  download), OR
- a matching BeProduct staging row exists with a **valid http(s)
  `front_image_url`** (source = BeProduct CDN, the original full-extraction
  path).

Rows already imaged are skipped silently (idempotent). Blank rows with
neither a same-style sibling image nor a usable BeProduct source URL, or
vector/unknown image types, are recorded as `skipped`.

---

## Image type handling (`classify_image_type`)

| Source type | Action |
|-------------|--------|
| jpg / png | upload as-is (DTC-native) |
| webp / gif / bmp / tiff | **transcode → PNG** (Pillow) then upload |
| svg / pdf / unknown | **skip** (`unsupported_type`) — cannot rasterise here |

Classification trusts the HTTP `Content-Type` first, falling back to the URL/file
extension when the type is generic (e.g. `application/octet-stream`).

---

## DTC image endpoint (validated live)

```
POST /v1/sheets/{sheetId}/views/{viewId}/images?rowindex={int}&columnname=Style Image
body: multipart/form-data, image bytes as a file part named "file"   -> success
```

- The query param is lowercase **`rowindex`**; `columnname` is the column display
  name (`"Style Image"`).
- DTC **rejects `webp` with HTTP 400** — hence the transcode step.
- Connector: `DTCConnector.upload_row_image` → `RestClient.post_multipart` (the only
  client method that sends a multipart `files=` body and does NOT force
  `application/json`).

Open data-side caveat: a subset of BeProduct CDN `frontImage.origin` URLs can
return **HTTP 403** (Azure SAS auth) on download; this is per-file CDN/SAS behaviour,
not a Phase 3 code defect, and is logged as `download_failed`.

---

## Parameters (`p3_beproduct_to_dtc_images.py`)

| Parameter | Default | Purpose |
|-----------|---------|---------|
| `catalog` / `schema` | `lft` / `beproduct` | target tables |
| `staging_table` | `beproduct_to_dtc_staging` | source of `front_image_url` |
| `dtc_environment` | `uat` | selects `dtc_api_key_<env>` secret |
| `dtc_workspace` | `KTB` | DTC workspace |
| `dry_run` | `true` | compute + log, no upload |
| `http_timeout` | `30` | CDN download timeout (s) |
| `max_uploads` | `0` | per-run upload cap (0 = no cap) |

In the live daily pipeline (the multi-task job `BeProduct_DTC_sync_dag`,
`scripts/deploy_job.py`) Phase 3 is task `phase3_images`, gated by
`gate_phase3` (condition on `run_phase3`), preceded by `repull_dtc` (a
targeted `dtc_wip` re-pull) — and, since 2026-08-31, the whole WIP chain
(including Phase 3) only runs after `phase0_push` (`run_if=ALL_DONE`, so
disabling `run_phase0` doesn't deadlock it). The old single-notebook
orchestrator `beproduct/orchestrate_sync.py` (Step 8/Step 7 numbering) is
retired and kept only as a manual fallback.

---

## Tests

| Test | Scope |
|------|-------|
| `dtc/tests/test_phase3.py` | upload planning (blank/idempotent/missing-url/rowIndex) + image-type classification (unit) |

---

## Review the results

```sql
SELECT * FROM lft.beproduct.beproduct_to_dtc_sync_log
WHERE stage = 'images'
ORDER BY log_time DESC
LIMIT 100;
```

# Agent guide — BeProduct ⇄ DTC sync

Read this first. It is the durable memory for this repo so each new change can build
on prior **verified** discoveries instead of re-deriving them.

## What this repo does

Bi-directional sync between **BeProduct** (style PLM) and **DTC** ("Data Collab"
sheets), staged through **Databricks/Delta**.

- **Phase 0 — DTC XTS Master → BeProduct Directory**: logically precedes Style/
  Material/Costing sync. Pulls DTC workspace "KTB", document "XTS Master"
  (requests "XTS Supplier Master"/"XTS Factory Master" — "XTS Mill Master" is
  intentionally OUT OF SCOPE for now, see decision below) into
  `lft.beproduct.dtc_xts_master_ktb` (`dtc/notebooks/p0_pull_xts_master_to_delta.py`),
  then upserts into `lft.beproduct.beproduct_directory`
  (`beproduct/p0_xts_master_to_directory_upsert.py`), matched by **`name` +
  `partner_type` together** (BeProduct's real Directory key — NOT `id`/
  `directory_id`, and NOT `name` alone). **Wired into the daily DAG**
  (2026-08-31) as the FIRST step, gated by `run_phase0` (default `true`):
  `wait_cluster → gate_phase0 → phase0_pull → phase0_upsert → phase0_push`
  (`p5utl_beproduct_master_data_sync` mode=`PUSH_DIRECTORY`, which pushes only
  rows where `id IS NULL OR extracted_at IS NULL OR modified_at > extracted_at`).
  Every Style/Material/Costing task then waits on `phase0_push`
  (`run_if=ALL_DONE`, so disabling `run_phase0` doesn't deadlock them).
- **Phase 1 — BeProduct → DTC** (`docs/PHASE1_WORKFLOW.md`): push BeProduct-owned
  style fields into the matching DTC request (upsert); create + share missing
  in-scope requests.
- **Phase 2 — DTC → BeProduct** (`docs/PHASE2_WORKFLOW.md`): push DTC-owned fields
  back into the BeProduct style.
- **Phase 3 — BeProduct → DTC image** (`docs/PHASE3_WORKFLOW.md`): upload the front
  image into the DTC "Style Image" cell (binary, separate step).
- **Phase 7 — BeProduct → DTC sample history**: push BeProduct sample-app submit
  history (all 6 apps: Proto/PreLine/SMS/Fit/PP/TOP) into the matching DTC status
  columns as one quoted, comma-separated line PER submit: `"submit_name",
  "submitStatus","submitStatusDate"` — NOT a JSON array (no `[` `]` at all),
  multiple submits stacked on separate newline-separated lines (confirmed
  2026-08-28; `phase1.norm()` preserves embedded newlines so the multi-line
  structure survives to the actual DTC push).
  Proto → "Proto Sample - Sample Status", PreLine → "Pre-line Sample - Status",
  SMS → "SMS - Sample Status", Fit → "2nd Fit Sample Approval Status",
  PP → "PP Sample Submission Approval Status", TOP → "TOP Sample Approval Status".
  All 6 DTC columns confirmed in the 204-field view
  (2026-08-28; Fit/PP destinations changed from the original 2026-07-07 mapping
  after a DTC WIP doc restructure — see Verified discoveries log).
- **Phase 8a/8b — RETIRED (2026-09-01).** DTC FABRIC → Delta → BeProduct
  Material Master (`pull_fabric_dtc` / planned Material Master upsert) is
  confirmed by the project team to be replaced by a separate "MaterialLib"
  application. Removed entirely from the deployed DAG (`gate_phase8a` +
  `pull_fabric_dtc` tasks, `run_phase8a`/`include_test_sheets`/
  `fabric_document` job parameters — not just gated off). The notebook
  `dtc/notebooks/p8a_pull_fabric_to_delta.py` is left in the repo as a
  historical/manual-fallback artifact; its output tables
  `dtc_fabric_<customer>` / `dtc_fabric_registry` were DROPPED from Delta
  (2026-09-01, owner-confirmed) rather than left empty/stale. See
  decisions log.
- **Phase 9a — DTC LinePlan + Costing Chart**: pull `"KTB LinePlan"` document into
  `dtc_lineplan_ktb`; join WIP × LinePlan on `"Lineplan Ref #"`; transpose 4
  vendor/factory slots → `lft.beproduct.costing_chart`. Runs as two parallel tasks
  (`pull_lineplan_dtc` → `p9a_build_costing_chart`, gated by `run_phase9a`). The
  `p9a_build_costing_chart` task also depends on `pull_dtc` (WIP data).
- **Phase 9b — NT Orbit Duty Tools**: for `costing_chart` rows missing
  `hts_code` / `duty_rate_us` / `duty_rate_ca` / `duty_rate_mx` / `tariff_rate`,
  calls the NT Orbit Duty Tools 3rd-party API
  (https://orbitduty.neotangent.com/API-DOCS/, `POST /api/v1/calculate/single/`)
  — up to one call per row per still-blank market (US/CA/MX). Each call is
  ~30s (sometimes exceeds it — the connector's default HTTP timeout was
  raised 30s→60s 2026-09-01 after live timeouts were observed) and calls
  are made SERIALLY by default (`orbit_parallel_calls=false`) rather than
  via a thread pool, since the API's concurrency tolerance under real load
  is unconfirmed and serial failures are easier to diagnose; set
  `orbit_parallel_calls=true` (+ tune `max_workers`, still hardcoded to 4 in
  the deployed job) to trade safety for throughput once that's validated.
  `costing_chart` is FULLY OVERWRITTEN by every Phase 9a run, so
  in-run dedup alone isn't enough — a PERSISTENT cross-run cache table
  (`duty_cache_table`, default `lft.beproduct.nt_orbit_duty_cache`, never
  wiped) keyed on `(product_description, origin_country, import_country)` is
  checked FIRST; a hit within `cache_ttl_days` (default 180 — tariff policy
  does change over time) skips the API call entirely, and only genuinely new
  or stale combinations reach NT Orbit. Fills the gaps on `costing_chart`
  (write-once), and optionally (`push_to_wip=true`) PATCHes the HTS/Duty
  values back onto the live DTC WIP per-slot columns (Tariff Rate has no WIP
  column yet — see decisions log). Auth is Microsoft Entra ID delegated
  OAuth2 (refresh_token → access_token), NOT the DTC x-api-key scheme — see
  `dtc/python/client/entra_auth.py` and the one-time setup CLI
  `scripts/nt_orbit_oauth_setup.py`. Entra rotates the refresh_token on
  (most) uses; the notebook auto-persists the rotated value to
  `lft.beproduct.nt_orbit_oauth_state` every run and prefers it over the
  static secret next time — so, like BeProduct's OAuth, this is "seed once,
  then fully automatic" (no manual secret-scope updates needed) as long as
  the job keeps running at least every ~90 days, which it does (3x/day).
  Runs as job task `fill_duty_rates`, gated by `run_phase9b` (**live in the
  DAG as of 2026-09-01**: `run_phase9b=true`, `push_duty_to_wip=true` on the
  deployed job `BeProduct_DTC_sync_dag` — both were `false` during earlier
  development), after `build_costing_chart`. The `p9b_fill_duty_rates.py`
  notebook's OWN standalone widget default for `dry_run` is still `"true"`
  (safe for ad-hoc/interactive runs from the ADB portal without job
  parameters); the deployed job's `dry_run` parameter is `false` (shared
  across every phase, same as Phase 1/2/3), so scheduled runs push for real.
  Costing chart table name is a job parameter `costing_chart_table` (default
  `lft.beproduct.costing_chart`; testing override
  `lft.beproduct.costing_chart_kei`); cache table/TTL are also job
  parameters (`duty_cache_table` / `duty_cache_ttl_days`). Notebook:
  `dtc/notebooks/p9b_fill_duty_rates.py`; pure logic + tests:
  `dtc/python/sync/duty.py` / `dtc/tests/test_duty.py`.
- **Phase 10 — BOM enrichment from externally-processed techpack data**:
  fulfills a Phase 1 gap (BOM data isn't in the BeProduct API at all).
  **Source (CHANGED 2026-09-09, "2nd revision", supersedes the original
  design below)**: `customer_teckpack_style_log.custom_fields` (path:
  `xts_data.TECH_PACK_EXTRACTION.Table[Type="BOM"].ColumnHeader`/`Data`),
  fetched via a two-hop join —
  `customer_teckpack_style_latest.latest_techpack_style_log_id →
  customer_teckpack_style_log.teckpack_style_log_id` (the "latest" table
  now only resolves WHICH log row is current; it is no longer the BOM data
  source itself, reversing the 2026-09-03 design). Joined onto `ktb_styles`
  by `(bp_style_number=style_no, season||" - "||year=style_season)`, INNER
  JOIN throughout both hops. Parses for "Main Fabric" (exactly 1 by
  construction) and "Fabric" (0 or more) segments by `**MaterialCategory`
  ONLY. **Field mapping (corrected 2026-09-09)**: `Fabric Group` ←
  `**MaterialCategory`; `Placement` ← `**Placement`; `Mill Fabric Article #`
  ← `**SupplierRefNo`; `Content` ← `**MaterialContent` (REINSTATED as a
  real Phase 10 output — briefly removed entirely the same day after an
  earlier `bom_unified.material_name` mapping was found pushing
  material-CODE-shaped garbage into live DTC for some styles).
  **Upsert semantics (revised 2026-09-03, refined 2026-09-10)** — NOT the
  original all-or-nothing design (a single already-enriched row used to
  short-circuit the WHOLE style to a no-op): per existing row, per run, a
  match by `(Fabric Group, Mill Fabric Article #)` upserts `Placement`/
  `Content` independently, a row with a currently-BLANK `Mill Fabric
  Article #` is backfilled in place (one-way, disambiguated by Placement
  when needed — fixes a live "frozen row" bug, `KTB-00025`), an un-enriched
  row gets full first-time enrichment, and any other real value is left
  completely untouched (never revert). Each new "Fabric" segment duplicates
  every existing row of the SAME COLORWAY once — **scoped per colorway, not
  globally across the whole style** (fixed 2026-09-10 — a style's multiple
  colors used to share one combined "is this segment represented?" pool,
  permanently starving a 2nd color of its own segments once a 1st color
  already claimed them; see decisions log, `KTB-00029`). Blank-vs-blank
  Placement/Content values (`None` vs `""`) are never treated as a diff
  (also fixed 2026-09-10, avoiding a spurious lean-PATCH violation).
  **CRITICAL**: `alb_tpm_*` catalogs are Lakebase databases registered in
  Unity Catalog — queryable ONLY from serverless compute, not the classic
  shared job cluster (live-confirmed 2026-09-02, `UnauthorizedAccessException`;
  also blocks running this specific notebook via an ad-hoc CLASSIC debug
  cluster/Command Execution session — only a serverless context works).
  `fill_bom_data`'s task therefore runs on SERVERLESS compute
  (`nb_task(..., serverless=True)`), the only task in this job that does.
  Runs BEFORE `build_costing_chart` so up-to-date material data reaches
  Phase 9b's NT Orbit calls; since it never mutates Delta directly (DTC
  push only), a dedicated `repull_dtc_bom` task (full
  `p1_pull_masters_to_delta` re-pull, `run_if=ALL_DONE`) runs immediately
  after so `build_costing_chart` sees the enrichment. **Depends on
  `repull_dtc`, NOT `pull_master_dtc` directly** — Phase 10 must enrich the
  COMPLETE post-`phase1_push` style×color state; `repull_dtc` is what makes
  those rows visible in Delta, and is a SHARED, unconditional prerequisite
  for both `phase3_images` and `fill_bom_data`. Gated by `run_phase10`
  (**deployed job default is `true`** — flipped 2026-09-03 after extensive
  live validation), checked INSIDE the notebook itself (NOT via a DAG-level
  condition task, unlike most other phases — a `gate_phase10` condition
  task caused a Databricks `EXCLUDED`-status cascade that broke Phase 9a/9b
  whenever it evaluated false; see decisions log). `gate_phase1` was removed
  the same way for the identical reason, now that the whole Phase 3/9/10
  chain transitively depends on `phase1_push` via `repull_dtc`. Notebook:
  `dtc/notebooks/p10_pull_bom_and_enrich.py`; pure logic + tests:
  `dtc/python/sync/bom.py` / `dtc/tests/test_bom.py`. Full current spec:
  `docs/PHASE10_WORKFLOW.md`; every gating condition: `docs/PIPELINE_GATES.md`.

Each field syncs **one way only** (no loops). Direction table below.
Components, data flow, and the full ADB data model: `docs/ARCHITECTURE.md`.
The whole pipeline runs as a **multi-task Databricks job** (`BeProduct_DTC_sync_dag`,
job 294837488757511), defined in `scripts/deploy_job.py`. The old single-notebook
orchestrator `beproduct/orchestrate_sync.py` is retired (kept as a manual fallback).

## Single source of truth (SSOT) for field mapping

**`docs/beproduct_style_interested_fields.txt`** — DTC column ⇄ BeProduct field,
fieldId, JSONPath, sync direction. Always update it first, then the code constants.

### Where a field mapping lives in code (edit ALL that apply, together)

| Direction | Constant | File |
|-----------|----------|------|
| BeProduct → DTC | `FIELD_MAPPING` | `dtc/python/sync/phase1.py` |
| BeProduct → DTC (image only) | `STYLE_IMAGE_COL` + `compute_image_uploads` | `dtc/python/sync/phase3.py` |
| DTC → BeProduct (header) | `REVERSE_HEADER_FIELDS` | `dtc/python/sync/phase2.py` |
| DTC → BeProduct (colorway) | `REVERSE_COLORWAY_FIELDS` | `dtc/python/sync/phase2.py` |
| DTC → BeProduct (no target yet) | `UNSUPPORTED_FIELDS` | `dtc/python/sync/phase2.py` |
| BeProduct extraction (raw → master) | `COMPULSORY_FIELDS` / `INTERESTED_FIELDS` | `beproduct/p1p7_beproduct_style_sync.py` |
| BeProduct sample-app status (title → column prefix) | `SAMPLE_APPS` | `beproduct/p1p7_beproduct_style_sync.py` + `beproduct/00_init_style_app_registry.py` |
| BeProduct sample submits → DTC (Phase 7) | `SAMPLE_SUBMIT_FIELDS` + `format_sample_field` | `dtc/python/sync/samples.py` (+ `phase1.FIELD_MAPPING`, transform staging) |
| BeProduct → DTC transform (denormalize) | `FIELD_MAPPING` + staging `select` | `beproduct/p1p7_beproduct_to_dtc_transform.py` |
| DTC WIP costing/duty push | `duty.WIP_HTS_COL` / `duty.WIP_DUTY_COL` / `duty.WIP_TARIFF_COL` | `dtc/python/sync/duty.py` |

Then update unit tests: `dtc/tests/test_phase1.py`, `dtc/tests/test_phase2.py`,
`dtc/tests/test_phase3.py`, `dtc/tests/test_samples.py`.

**Every one of the above is also a WIP `sheetData` PATCH allow-list** — see
Ground rule #6 for the complete, canonical field set and the lean-PATCH-body
requirement (each payload builder above is the sole source of PATCH keys, so
this stays true by construction; verify it stays true after any change).

### Current direction partition

- **BeProduct → DTC**: Product Status, Style Description, Class, Sub Class, Division,
  Brand, Garment Finish, Tech Pack Stage, Gender (pending DTC col);
  BP Style# (new match key, pending DTC col), LF Style# (optional), Legacy Code (optional);
  Supplier (default-fill "Supplier" when blank; pending DTC col).
  **Filter**: styles with Product Status = "Finalized" are excluded from staging/DTC sync.
  Fabric Group / Placement are default-fill ONLY (fixed 2026-09-03) — Phase 1
  sets the "MAIN MATERIAL CONTENT" placeholder on INSERT alone; Phase 10
  (TPM/BOM data) is the sole ongoing owner of real values — see decisions log.
- **BeProduct → DTC (Phase 7, sample submit history)**: All 6 apps mapped.
  Proto → "Proto Sample - Sample Status", PreLine → "Pre-line Sample - Status",
  SMS → "SMS - Sample Status", Fit → "2nd Fit Sample Approval Status",
  PP → "PP Sample Submission Approval Status", TOP → "TOP Sample Approval Status".
  Each value is one quoted, comma-separated line PER submit: `"submit_name",
  "submitStatus","submitStatusDate"` — NOT a JSON array (no `[` `]` at all),
  multiple submits stacked on separate newline-separated lines (confirmed
  2026-08-28; `phase1.norm()` preserves embedded newlines so the multi-line
  structure survives to the actual DTC push — superseded a same-day flat-JSON-
  array iteration, itself a fix of the original nested array-of-arrays that
  always showed doubled `[[`/`]]` for the common single-submit case). All 6
  DTC columns confirmed in the 204-field view (2026-08-28; Fit/PP destinations
  changed from the original 2026-07-07 mapping after a DTC WIP doc restructure
  — was Fit → "1st Fit Sample Approval Status", PP → "2nd Fit Sample Approval
  Status"). Note: "Pre-line Sample - Status" uses lowercase 'l' and dash.
  Requested PP destination "PP Sample Approval Status" does not exist live —
  "PP Sample Submission Approval Status" was the only plausible match
  (confirmed via `get_view_definition`) and has since been **confirmed
  correct by the project team** (2026-08-28).
- **DTC → BeProduct**: Main Vendor (Sampling) (`parent_vendor`), Main Factory
  (Sampling) (`factory`), Main Factory Customer ID (`customer_factory_code`,
  wired up 2026-09-03 — see decisions log), Factory Production Country for
  Main Factory (`country_of_origin` / "COO", wired up 2026-09-09 — see
  decisions log; the first Phase 2 field requiring a value transform,
  DTC 2-char country code → BeProduct country name, via
  `phase2.resolve_coo_country_name()` + `beproduct_master_coo`) [header];
  Lot# (`drawing_number_walmart`) [colorway].
  [REMOVED Phase 6]: "Legacy Code" DTC→BP. "Customer Style#" DTC column not created.
- **Keys** (match, not overwritten): BP Style# (`header_number`), Color/Wash (`colorName`)
  [in-request]; [Customer, BP Style# (header_number), SeasonCode, Brand (brand_hk)]
  [composite/request-routing].
- **BeProduct → DTC, image only (Phase 3)**: Style Image (`front_image_url` →
  DTC "Style Image"). Binary, so it does NOT ride the Phase 1 sheetData PATCH;
  uploaded by `p3_beproduct_to_dtc_images` via the multipart images endpoint, only
  when the DTC cell is blank and BeProduct has a valid `front_image_url`. Still
  one-directional (never read back to BeProduct, never in `phase2.REVERSE_*`).

## Ground rules / invariants

1. **Validate live before/while coding.** Local creds in `.env`
   (`BEPRODUCT_*`, `DATABRICKS_*`, `NT_ORBIT_*`); on Databricks use secret scope
   `beproduct` (`client_id`, `client_secret`, `refresh_token`, `company_domain`,
   `dtc_api_key_<env>`, and Phase 9b's `nt_orbit_tenant_id`, `nt_orbit_client_id`,
   `nt_orbit_client_secret`, `nt_orbit_refresh_token` — seeded once via
   `scripts/nt_orbit_oauth_setup.py`; rotated refresh tokens are then persisted
   to the Delta control table `lft.beproduct.nt_orbit_oauth_state`, NOT back
   into the secret scope, since `dbutils.secrets` is read-only). **Update
   2026-08-31: Phase 9b now uses a DEDICATED confidential app registration**
   (client_id `d270069e-20cc-4e63-ba38-156fb0ee9296`, tenant_id
   `c4d8a220-a9ec-4572-8c77-ab36a3ecdbae` — supersedes the earlier "reuse an
   existing shared client_id, no secret" assumption below, and also
   supersedes an earlier client_id `c486611e-9bfc-49d5-8930-7d1943884b03`
   whose redirect_uri was registered under the "Mobile and desktop
   applications" platform and hit AADSTS700025 — see decisions log), so
   `nt_orbit_client_secret` IS set and used. The Entra login is still a
   DELEGATED-USER sign-in (auchunkei@lifung.com), never client-credentials
   (app-only) — NT Orbit authorizes the signed-in person, not the app. Real
   secret VALUES must only ever live in the untracked local `.env` and the
   Databricks secret scope — never in `.env.example` or any other tracked
   file. There is a sacrificial in-scope DTC request for reversible write
   tests: `KTB FW26 Wrangler` (UAT request `6a26581854e92e7acd8fa71b`).
   Because this is now a dedicated app registration, its redirect URI IS
   ours to register, so `python scripts/nt_orbit_oauth_setup.py --flow authcode
   --redirect-uri http://localhost:8765/callback` (register that same URI on
   the app in the Entra portal first) is the recommended one-time login —
   fully scripted, no manual copy/paste. `--flow manual` (Postman's
   `https://oauth.pstmn.io/v1/browser-callback`, no redirect-URI registration
   needed) and `--flow devicecode` (RFC 8628) remain as no-portal-access
   fallbacks; all three end in the same `access_token`/`refresh_token` pair.
2. **Match BeProduct fields by `fieldId`, not display name** (names are inconsistently
   cased / have trailing spaces).
3. **One field, one direction.** Never add a field to both `phase1.FIELD_MAPPING` and
   `phase2.REVERSE_*`.
4. **Notebooks can't run locally** (Spark/dbutils). Put deterministic logic in
   `dtc/python/sync/phase1.py` / `phase2.py` (pure-Python, unit-tested); keep
   notebooks as thin Spark/IO wrappers.
5. **In-request match key = (BP Style#, Color/Wash)**; season+brand are fixed per
   request. Brand is one-per-request and agrees with the request name. (Phase 6:
   was (LF Style#, Color/Wash). Composite key uses brand_hk, not brands_multi.)
6. **Every DTC WIP `sheetData` PATCH body must be LEAN** — include only fields
   from the canonical allow-list below (a subset per call is fine; never
   anything outside it), per explicit DTC-developer advice that PATCH bodies
   should be as small as possible. Batch multiple field-changes for the SAME
   row into ONE PATCH object whenever they're computed in the same pass
   (never issue two separate PATCH calls for one row when they could be
   merged) — see `phase1.chunked()` usage in all three sites below.
   Live-confirmed 2026-09-04 that all three current WIP-PATCH call sites are
   already structurally compliant (the payload builder itself is the only
   source of keys, so nothing else can leak in):

   **Phase 1 + Phase 10 fields** (`phase1.FIELD_MAPPING` / `bom.to_wip_fields()`
   — style×color×material data, written at Steps 10/30):
   `Product Status`, `Style Description`, `Class`, `Sub Class`, `Division`,
   `Brand`, `Color / Wash`, `Garment Finish`, `Tech Pack Stage`, `BP Style#`,
   `LF Style#`, `Legacy Code`, `Gender`, `Supplier` (hardcoded prefill
   default-fill value, not sourced from BeProduct), `Fabric Group`,
   `Placement`, `Mill Fabric Article #` (Phase 10 sources this from BOM
   `**SupplierRefNo` via `customer_teckpack_style_log.custom_fields` —
   corrected 2026-09-09 "2nd revision", was `bom_unified.material_name`
   before that and `material_no` before that), `Proto Sample - Sample
   Status`, `Pre-line Sample - Status`, `SMS - Sample Status`, `2nd Fit
   Sample Approval Status`, `PP Sample Submission Approval Status`, `TOP
   Sample Approval Status`. `Style Image` is explicitly EXCLUDED from every
   sheetData PATCH (image cells can ONLY be set via Phase 3's separate
   multipart `/images` endpoint — DTC rejects any sheetData write to it).
   **`Content` IS a Phase 1/10 PATCH key** (REINSTATED 2026-09-09 "2nd
   revision", sourced from BOM `**MaterialContent` — reverses the
   2026-09-09-morning removal, which itself had reversed the original
   2026-09-03 decision to write it from the since-abandoned
   `bom_unified.material_name`; see decisions log for the full history).

   **Phase 9b fields** (`duty.WIP_HTS_COL` / `duty.WIP_DUTY_COL` /
   `duty.WIP_TARIFF_COL` — costing/duty data, written at Step 55, a
   SEPARATE PATCH call from Phase 1/10's): per vendor slot (`Main`/`1`/`2`/`3`)
   — `"<slot> Factory HTS Code"` (`"Main Factory HTS Code"` for Main),
   `"<slot> Factory Duty Rate (US/CA/MX)"`. Tariff Rate columns
   (`"<slot> Factory Tariff rate"`) are DEFINED in code
   (`duty.WIP_TARIFF_COL`) but NOT YET LIVE in the WIP view
   (`duty.WIP_TARIFF_COLS_LIVE = False`, confirmed 2026-07-17) — tariff_rate
   currently stays `costing_chart`-only; flip that flag once DTC adds the
   columns, no other code change needed.

## Verified discoveries log (append-dated; do not delete)

**BeProduct Directory (validated 2026-06-23):**
- `api.directory.directory_list()` returns 3852 records (vendors, factories, mills).
  `api.directory.directory_contact_list(header_id)` returns 0 contacts for ALL records —
  confirmed by the org: no BeProduct user accounts have been attached to any
  mill/factory/supplier. `beproduct_directory_contacts` therefore stays empty and
  `fetch_contacts` widget defaults to `false` (skips the extra ~3800 API calls).
  Do NOT assume contacts exist when reasoning about Directory data.
- `beproduct_master_parent_vendor` and `beproduct_master_factory` each contain 3852
  choices — same count as Directory records (they are the flattened choice lists
  derived from the same partner database).
- Directory pull throughput: ~2 records/sec via `directory_list()`, ~30 min for 3852
  records. Pull is `PULL_ONLY` mode only. `PUSH_DIRECTORY` skips the pull and uses
  change detection (`modified_at > extracted_at`) to push only pending rows.

**DTC "XTS Master" document (Phase 0, live-validated 2026-08-28):**
- Workspace "KTB", document "XTS Master" has exactly the 3 requested requests
  (`"XTS Supplier Master"`, `"XTS Factory Master"`, `"XTS Mill Master"`), plus 3
  `"(BACKUP)"`/differently-named siblings (`"XTS (BACKUP) Supplier Master"`,
  `"XTS (BACKUP) Factory Master"`, `"KTB (BACKUP) Mill Master"`) — all
  `requestIsActive='Y'`. **Scope decision (2026-08-28): only Supplier and
  Factory are pulled** — `"XTS Mill Master"` is intentionally out of scope for
  now (see next bullet for why), and the `(BACKUP)` siblings are deliberately
  excluded regardless. The Phase 0 pull matches **exact** `requestReference`
  only (`sync.xts_master.XTS_REQUESTS`).
- All 6 requests share the SAME 4 views (`Supplier`, `Mill`, `Factory`,
  `WIP_ITS_USE` — identical view IDs across all 6 requests in the document,
  confirmed via `get_views`), even though only `Supplier`/`Factory` are used.
  The `WIP_ITS_USE` view here (id `6a3907f6df772fd797ee5b7c`) is the same one
  already flagged elsewhere in this doc as belonging to a *different*
  document than KTB WIP — do not use it for anything; Phase 0 never touches
  it. The `Mill` view (id `6a3b351185ceba6dca6712e5`) has NO code column at
  all, and in UAT its 8 rows are 100% brand-config rows (`Type="Fabric
  Brand"`, same 8 brand names as Supplier's brand rows below) — there is
  currently NO real Mill company data in this environment, which is why it
  was excluded from scope rather than pulled empty.
- **The document is NOT a rich vendor-master sheet.** `GET /v1/views/{id}`
  (authoritative field list, not sample rows) confirms the in-scope views:
  - `"Supplier"` view: `Supplier Name`, `Supplier Code`, `Customer Vendor ID`,
    `Type`, plus 9 request-sharing/access-config columns (`Brand Views`,
    `Group Users Name`, `Agent Alert Recipient (...)`, etc.) — NO
    address/state/zip/city/phone/fax/website/notes at all.
  - `"Factory"` view: only 4 fields — `Factory Name`, `Factory Code`,
    `Customer Factory ID`, `Production Country`. No `Type` column, no
    sharing/config columns, no address/phone/etc. either.
  - None of address/state/zip/city/phone/fax/website/notes exist ANYWHERE in
    either in-scope view — they are always `NULL` for every XTS-sourced
    `beproduct_directory` row until DTC adds them.
- **CRITICAL data-quality finding**: the `Supplier` sheet is polluted with
  BRAND-level access-sharing config rows interleaved with real company rows,
  distinguishable ONLY by the `Type` column: `Type="Brand"` (8/42 rows in
  UAT, e.g. "Wrangler", "Blue Bell", "Slam Jam" — brand names, not companies,
  always blank code) vs `Type="Supplier"` for real rows (34/42 rows, each
  with a real code). `sync.xts_master.EXCLUDE_TYPE_VALUES` / `is_brand_row()`
  filters these out. Partner type is derived from WHICH REQUEST/view a row
  came from, never from the `Type` cell's literal value.
- **Directory match key correction (2026-08-28, same day, supersedes the
  initial hypothesis below)**: BeProduct's Directory record is keyed by
  **`name` + `partner_type` TOGETHER**, not `name` alone as first assumed.
  This means the SAME name legitimately recurring under a DIFFERENT partner
  type (see next bullet) is simply two separate, valid Directory records —
  NOT a collision requiring a pick. `sync.xts_master.find_duplicate_keys` /
  `dedupe_by_key` operate on the `(name, partner_type)` pair; a true
  collision only exists if that exact pair repeats (e.g. a duplicate row
  within one sheet).
- 19 of the 34 real Supplier rows share the exact same `name` AND code as a
  Factory row (e.g. `"SUPPLIER ASPGAR"` / code `ASPGAR` appears in both
  sheets) — the same physical entity apparently acts as both a sourcing
  supplier and a production factory. Under the corrected (name, partner_type)
  key model this is expected and unproblematic (both records are created);
  it was initially (incorrectly) treated as a same-name collision needing a
  SUPPLIER-wins tie-break before the key model was clarified the same day.

**DTC API (validated 2026-06-17):**
- Sheet upsert: `PATCH /v1/sheets/{sheetId}/views/{viewId}` body
  `{"sheetData":[{...,"rowId"|"rowIndex":..}]}` → 204. A single PATCH **cannot mix**
  rowId (update) and rowIndex (insert) — send separate batches.
- Row delete EXISTS: `DELETE /v1/sheets/{sheetId}/views/{viewId}/rows` body
  `{"rowIndexes":[...]}` → 204 (keys off rowIndex, not rowId).
- Cell image upload (Phase 3, **LIVE-VALIDATED 2026-06-17**, 41 uploads OK):
  `POST /v1/sheets/{sheetId}/views/{viewId}/images?rowindex={int}&columnname=Style Image`
  with the image bytes as a **multipart/form-data** file part (binary). Operates
  on an EXISTING row (keys off lowercase `rowindex`), so it must run AFTER Phase 1
  creates the rows. The JSON sheetData PATCH cannot set images. **Confirmed**: the
  multipart file PART NAME is **`file`**, `columnname="Style Image"` and the
  `rowindex` query param all work (jpg + png uploaded successfully). Connector:
  `DTCConnector.upload_row_image` → `RestClient.post_multipart` (the only client
  method that sends `files=`/`params=` and does NOT force `application/json`).
  **DTC REJECTS `webp` with HTTP 400** (3/3 webp rows failed; jpg/png accepted).
  `p3_beproduct_to_dtc_images` now classifies via `phase3.classify_image_type` and
  **transcodes webp/gif/bmp/tiff → PNG (Pillow) before upload**; jpg/png upload
  as-is; vector/unknown (svg, pdf, ...) are skipped with `unsupported_type`.
  Separately, a subset of BeProduct CDN `frontImage.origin` URLs return **HTTP 403**
  on download (Azure SAS auth — "signature" error) even within the SAS validity
  window; affects only some files (36 png succeeded, 53 png failed), so it is a
  per-file BeProduct CDN/SAS issue, NOT a Phase 3 code defect. Still open.
- Request listing works via `GET /v1/requests` with `workspaceName`+`filters` in the
  **request body** (not query params; query param → 400 "Invalid workspaceName").
- Registry refresh is the shared `sync.registry.refresh` (used by
  `00_init_request_registry`, `p1_pull_masters_to_delta`, `p1_dtc_request_manager`):
  `search_requests(workspace, document_name=document, filters={"requestIsActive":"Y"})`
  lists **active** requests (server-side filter — DTC dev confirmed inactive requests
  400 on get-by-id; field is `requestIsActive`). A client-side `is_active_item` guard
  remains as cheap insurance. It then
  **pre-filters on the listed `requestReference` so only ACTIVE + IN-SCOPE names are
  read/registered** by-id (`get_request`/`get_views`). Inactive and
  out-of-scope/foreign requests are skipped entirely — NOT enriched, NOT registered —
  which is why `get_request` is never called on them (they were the HTTP 400 noise).
  Explicit
  `request_ids` are read by-id without the pre-filter. Upsert `mode=merge` preserves
  `last_extracted`/`last_pushed`/`row_count`; `replace` wipes them. After a full
  auto-discover (non-empty listing) it **reconciles**: registry rows in the scanned
  scope (`environment`+`customer`+`document`) absent from the scan are **marked**
  `request_is_active='N'`/`in_scope=false` (mark, not delete — sync state survives,
  re-discovery flips them back). So the registry doesn't keep stale inactive/
  out-of-scope rows. Skipped for explicit `request_ids` and empty listings.
- Allowed columns must come from the **view definition** (`GET /v1/views/{viewId}` →
  178 dynamicFields), NOT from sheet cells (empty columns don't appear in `sheetData`,
  which previously caused false "missing column" findings). `WIP_ITS_USE` column
  `Division` was once `Division?` (the `?` was removed).

**BeProduct schema (KTB folder, validated 2026-06-17 via `folder_schema` /
`folder_colorway_schema`):**
- `BRANDS` (`brands_multi`) and `CUSTOMER` (`sold_to_customer`) are MultiSelect
  **lists of plain strings** → read `value[0]` (NOT `[0].value`); guard empty list.
- `Lot Code` is a **colorway** field, fieldId **`drawing_number_walmart`** (the id is
  misleading). Header no longer defines it; ignore any legacy
  `headerData.fields[id="drawing_number_walmart"]`. Filled by DTC → BeProduct.
- Pushback uses `api.style.attributes_update(header_id, fields={fieldId:val},
  colorways=[{"id":colorway_id,"fields":{fieldId:val}}])` — colorway writes need the
  colorway **id**, so the transform carries `colorway_id` into staging
  (`colorways_json` from `p1p7_beproduct_style_sync`).

**BeProduct schema Phase 6 structural changes (validated live 2026-06-26):**
- `header_number` field **renamed** in BeProduct from "LF Style Number" → "BP Style Number".
  Field_id and data values **unchanged**. This field is the intended new match key for DTC
  (target DTC column "BP Style#"). Staging column renamed `lf_style_number` → `bp_style_number`.
- New field `lf_style_number` (field_id: `lf_style_number`, display name "LF STYLE NUMBER"):
  completely separate from `header_number`. Currently `None` for all checked styles.
  Will be populated by upstream integration. Pushed to DTC's **existing** "LF Style#" column.
- New field `brand_hk` (field_id: `brand_hk`, display name "Brand"): single-value string.
  Used as the Brand key in the composite key `[Customer, BP Style#, SeasonCode, Brand]`.
  Replaces `brands_multi` for request-routing and scope checks. `brands_multi` ("BRANDS")
  was retained in ktb_styles as non-key metadata at the time — **REMOVED entirely
  2026-09-03** (owner spec: "take out 'brands' from BP tables — use 'brand'
  (brand_hk) only now"); see decisions log.
- Ignore field_id `brand` (display name "BRAND") — legacy, not the composite key Brand.
- Composite key target: `[Customer, BP Style# (header_number), SeasonCode, Brand (brand_hk)]`
- In-request match key target: `("BP Style#", "Color / Wash")` (was `("LF Style#", ...)`).
- **No new "Legacy Code" BeProduct field.** The existing `customer_style_number` field
  (CUSTOMER STYLE NUMBER / PLM #) is what DTC calls "Legacy Code". It will be populated by
  upstream integration and flows BP→DTC (optional, sent when non-blank).
  Previously "Legacy Code" was DTC→BP; that role now belongs to the new DTC "Customer Style#".
- `customer_style_number` will also be populated by upstream. Both `lf_style_number` and
  `customer_style_number` are upstream-filled; the sync reads them from BP and writes to DTC.

**DTC WIP view columns — status (confirmed by scanning all 75 active KTB requests, 2026-07-02):**
  Columns **NOT YET IN VIEW** (DTC admin must add; sync code is ready but dormant):
  - `"BP Style#"` — **BLOCKER**: MATCH_KEY_COLS is already `("BP Style#", ...)` in code; sync will
    produce `missing_bp_style` exceptions for all rows until DTC creates this column and the existing
    "LF Style#" data is migrated to "BP Style#" by the DTC admin.
  - `"Gender"` — Phase 1 mapping ready in `FIELD_MAPPING`; dormant until DTC adds column.
  - `"Supplier"` — Phase 1 `DEFAULT_FILL_COLS` logic ready; dormant until DTC adds column.
  Columns **DECIDED NOT TO CREATE**:
  - `"Customer Style#"` — removed from `phase2.REVERSE_HEADER_FIELDS` (2026-07-02). No DTC→BP
    path for `customer_style_number`. "Legacy Code" (BP→DTC only) is the only direction.
  Columns **EXIST IN VIEW** (confirmed with data in ≥7 of 75 KTB requests, just blank on fresh ones):
  - `"Legacy Code"` (9 rows), `"Garment Finish"` (69), `"Tech Pack Stage"` (141),
    `"Main Vendor (Sampling)"` (46), `"BP Style#"` (0 — in view def, all blank),
    `"Gender"` (0 — in view def, all blank), `"Supplier"` (2 rows).
    All confirmed via `GET /v1/views/69f04983501f3d9cf4fc379c` → **194 dynamicFields**
    (2026-07-03, with proxy). The earlier view ID `6a3907f6df772fd797ee5b7c` is a
    different document ("XTS Master", 8 fields) — do not use for KTB WIP.
    `allowed_cols` in push notebook now **UNIONs** data-scan result with `FALLBACK_COLS` to avoid
    silently dropping these columns on fresh sheets where they're blank.

**BeProduct Style Applications (sample status, validated 2026-06-19):**
- A style has **applications** (`api.style.app_list(header_id)` →
  `[{id,title,type}]`; `api.style.app_get(header_id, app_id)` → content). The 6
  SAMPLE apps are Proto / PreLine / SMS / Fit / PP / TOP Sample (type
  `SampleRequestMulti`). KTB-folder sample app IDs: Proto `a765845f-…`,
  PreLine `8979ea71-…`, SMS `91094294-…`, Fit `a5a51c66-…`, PP `e5b7564d-…`,
  TOP `ca05cf47-…` (verified identical across styles).
- **App IDs are constant per FOLDER, not per style** (confirmed across two styles,
  23/23 ids matched). Cache them once via `00_init_style_app_registry` →
  `beproduct_style_app_registry`; do NOT call `app_list` per style.
- **`app.modifiedAt` is INDEPENDENT of `style.modifiedAt`.** Editing a sample app
  does NOT bump the style; editing attributes does NOT bump the app. Nothing in the
  style payload hints an app changed. So there is **no incremental shortcut** — the
  only way to read sample status is one `app_get` per (style × app). `app.modifiedAt
  == "0001-01-01T00:00:00"` = app exists, no data.
- `app_get` (`SampleRequestMulti`) → `data.submits[].sizes[]` each with
  `submitStatus`, `submitStatusDate`, `dueDate`, `receivedDate`, `fitDate`; plus
  `data.poms[]` (measurements). A sample app **explodes** like colorways/BOM
  (N submit rounds × M sizes) — do NOT collapse it to one status.
- `p1p7_beproduct_style_sync` enriches `ktb_styles` with **6 JSON-array columns**:
  `{proto,preline,sms,fit,pp,top}_sample_json` — each the full list of submit×size
  records (`submit_id/name, size_id/size, is_sample_size, submit_status,
  submit_status_date, due_date, received_date, fit_date`), `'[]'` when no data.
  Stored RAW (POMs excluded); flattening/selection is **delegated to the Step-2
  transform** (mirrors `colorways_json`) so we don't pre-bake an unspecified format.
  One write — enrichment runs in parallel (`app_max_workers`, default 10) BEFORE the
  single DataFrame build. SSOT title→prefix map = `SAMPLE_APPS` (duplicated in the
  sync + init notebooks; keep aligned).
- **Daily job runs Step 1 in FULL** (set in `scripts/deploy_job.py`) precisely
  because app edits don't bump `style.modifiedAt`; INCREMENTAL would miss them.
  INCREMENTAL stays for ad-hoc developer runs via the ADB portal.
- BeProduct "2 calls/sec" is a **minimum-throughput SLA, not a cap** — 10 workers
  sustained ~7 calls/sec, no throttling. ~1.5 s genuine latency per `app_get`.
  146 KTB styles × 6 apps = 876 calls ≈ ~120 s at 10 workers (the full-scan cost
  added to Step 1). Only ~15 of 146 styles currently hold any sample data.
- Good verification candidates (rich sample data): `Iris - Test- Top-111` (Proto
  Approved + Fit Requested + TOP Approved), `Boy  Short Sleeve Tee` (Proto+PP),
  `LFBP-1WTP0003` (Proto+SMS), `HOODED-K263` (Proto, 27 POMs).
- DTC push of sample status is **wired for all 6 apps (Phase 7)**: Proto/PreLine/SMS/Fit/PP/TOP
  → DTC status columns via `sync.samples.format_sample_field` (transform UDF) +
  `phase1.FIELD_MAPPING`. Each DTC cell gets a JSON list of `[submit_name,
  submitStatus, submitStatusDate]` (first size per submit). All 6 DTC columns confirmed in the
  198-field view (2026-07-07). Note: "Pre-line Sample - Status" uses lowercase 'l' and dash.
  **Superseded 2026-08-28**: the DTC WIP doc was restructured (198 → 204 fields) and Fit/PP
  now target different columns — see the "Current direction partition" section above for the
  current mapping (Fit → "2nd Fit Sample Approval Status", PP → "PP Sample Submission Approval
  Status") and `sync/samples.py`'s module docstring for full detail.
- **Phase 7 DTC WIP restructure (live-validated 2026-08-28)**: `get_view_definition` on the
  KTB WIP_ITS_USE view (id `69f04983501f3d9cf4fc379c`) now returns **204 fields** (up from
  198 on 2026-07-03). Both `"1st Fit Sample Approval Status"` and `"2nd Fit Sample Approval
  Status"` still exist side-by-side; only WHICH ONE Phase 7 pushes Fit to changed (now `2nd`,
  was `1st`). The old PP target `"2nd Fit Sample Approval Status"` was reassigned away from PP
  (now used by Fit) — PP's new field is `"PP Sample Submission Approval Status"` (confirmed
  present). The literally-requested name `"PP Sample Approval Status"` does **not** exist in
  the view at all — flagged, not silently guessed past this one plausible match; **confirmed
  correct by the project team** the same day.
- **`format_sample_field()` output format changed 2026-08-28 (two iterations same day)**:
  first from a nested array-of-arrays (`[["name","status","date"]]` — always doubled
  `[[`/`]]` for the very common single-submit case) to a single flat JSON array
  (`["name","status","date", ...]`); then per follow-up request, to a plain
  **non-JSON** format: one quoted, comma-separated line PER submit
  (`"name","status","date"`), multiple submits stacked on separate `\n`-separated
  lines — no `[` `]` brackets at all in either single- or multi-submit cases.
  **Critical companion fix**: `phase1.build_target_payload()` stores `phase1.norm(value)`
  as the actual pushed payload value (not the raw pre-norm value), and `norm()`'s
  original regex (`\s+` → single space) would have silently collapsed the new format's
  intentional `\n` separators into a single space before the value ever reached DTC —
  found and fixed the same day by changing the regex to `[^\S\n]+` (collapses all
  whitespace EXCEPT newlines). No other current field legitimately contains embedded
  newlines, so this is a no-op for every other column.

**DTC FABRIC document (KTB, validated 2026-07-16) — Phase 8a/8b RETIRED 2026-09-01,
kept below for historical reference only (see decisions log):**
- Document "KTB FABRIC", Workspace "KTB". **39 active requests** in two patterns:
  - DEV sheets:  `"KTB <season> <brand> - DEV"` — master development sheet per brand
  - Mill sheets: `"KTB <season> <brand>-<MILLCODE>"` — mill-specific allocation sheets
- **WIP_ITS_USE view**: id `6a0ac943fedfa0ca7ff2bf48`, **120 dynamicFields**
  (updated 2026-07-17 — "LF Material ID" added).
  Same view name as WIP document; different per-request view ID.
- **Adoption (Y/N)**: filter field. 0 adopted rows in UAT as of 2026-07-16 (UAT data
  not yet populated). Phase 8a code filters to Adoption=Y at pull time; logic is ready.
- **Key staging columns** (DTC field → Delta column):
  `"LF Material ID"` → `lf_material_id` (BP Material Master primary key; confirmed 2026-07-17),
  `"ITS_Key"` → `its_key` (DTC system row key),
  `"Mill Fabric Article #"` → `mill_fabric_code`, `"Mill Name"` → `mill_name`,
  `"Material Class"` → `material_class`, `"Fabric Type"` → `fabric_type`,
  `"Fabric Content"` → `fabric_content` (→ BP Material Description),
  `"KB Fabric Code (SAP Code)"` → `kb_fabric_code`.
- **Sheet-type switch** (`include_test_sheets` widget, default `false`):
  `false` = PROD sheets only (clean `<customer> <season> <brand>`, no suffix);
  `true`  = include DEV + MILL sheets (needed in UAT where all current sheets have suffixes).
- **Phase 8a Delta tables**: `dtc_fabric_<customer>` (Adoption=Y rows) +
  `dtc_fabric_registry` (request metadata, same structure as dtc_request_registry).
- **Job task**: `pull_fabric_dtc` gated by `run_phase8a=true`; runs in parallel with
  the WIP chain after `wait_cluster`. Notebook: `dtc/notebooks/p8a_pull_fabric_to_delta.py`.
- **SSOT field mapping**: `docs/beproduct_material_interested_fields.txt`
- Do NOT use view ID `6a0ac943fedfa0ca7ff2bf48` for WIP document operations —
  that ID belongs to FABRIC only.

**DTC LinePlan document (KTB, validated 2026-07-17):**
- Document "KTB LinePlan", Workspace "KTB". **1 active request** in UAT:
  `"FA HO 27 MENS WESTERN TOPS LINEPLAN - FA27"` (396 rows). No standard
  `<customer> <season> <brand>` naming — LinePlan uses its own naming convention.
- **No "LINEPLAN_ITS_USE" view** — only `"Full"` (id `69f0788555010bb745140ac4`,
  30 dynamicFields). `p9a_pull_lineplan_to_delta` tries `"LINEPLAN_ITS_USE"` first then
  falls back to `"Full"`.
- **Key LinePlan fields** (exact DTC column names — all UPPERCASE):
  `"Lineplan Ref #"` → `lineplan_ref` (join key to WIP `"Lineplan Ref #"`),
  `"PROJECTED VOLUME (season)"` → `projected_volume` (Order Qty in costing),
  `"TARGET SAP w/ Tariff impact"` → `target_ldp` (lowercase 'i'; Target LDP),
  `"TARGET FOB"` → `target_fob`,
  `"INTERNAL/ SOURCED"` → `internal_sourced` (→ Costing Supplier Type).
- **WIP join field** is plain `"Lineplan Ref #"` (no `(GC)` suffix in actual
  view — spec description used `(GC)` as annotation, not literal column name).
- **WIP Tariff Rate fields do NOT exist**: `"Main Factory Tariff rate"`,
  `"Factory 1 - Tariff rate"` etc. are all absent from WIP. Tariff Rate column
  in costing_chart is NULL placeholder, to be filled by Phase 9b (NT Orbit).
- **Phase 9a Delta tables**: `dtc_lineplan_ktb` + `dtc_lineplan_registry`.
  `costing_chart` = fully overwritten join result (Style × Color × Vendor/Factory).

**BeProduct `attributes_list` folder scoping bug (found + fixed live, 2026-08-27):**
- The BeProduct SDK's `api.style.attributes_list(folder_id: str = "", ...)` takes
  a `folder_id` kwarg. Passing `folderId=` (wrong casing) is silently absorbed
  into `**kwargs`/`raw_api.post(**kwargs)` and never applied — the call falls
  back to `folder_id=""` and returns EVERY style across the WHOLE account, not
  just the intended folder. Confirmed live: an ad-hoc "list styles in TEST KTB"
  query using `folderId=` returned all 104 account-wide styles (99 SANDBOX +
  Apparel + KTB + TEST KTB combined) instead of the 8 actually in TEST KTB;
  fixed by switching to `folder_id=`, verified against both `TEST KTB` (8
  styles, matches the DTC web portal) and `KTB` (158 styles, all confirmed
  `folder.name == 'KTB'`).
- `beproduct/p1p7_beproduct_style_sync.py` (the daily Phase 1/7 style sync) and
  `beproduct/00_init_style_app_registry.py`'s fallback API scan had the SAME
  class of bug in a different shape: neither passed `folder_id` AT ALL to
  `attributes_list()` — they fetched the **entire account** every run and
  post-filtered client-side by `style.folder.name == folder_name`. This gave
  correct RESULTS (name equality is unambiguous today) but was needlessly slow
  and would silently break if two folders were ever named identically. Fixed
  (2026-08-27): both notebooks now resolve `folder_id` once via
  `api.style.folders()` (raising if the name is missing or ambiguous) and pass
  it to `attributes_list(folder_id=...)` so the API itself scopes the result
  server-side; the old client-side name check is kept only as a defense-in-depth
  sanity assertion (should now always be a no-op).

## Decisions on record

- **Phase 10: segment-coverage decisions scoped PER COLORWAY, 2026-09-10
  (owner-reported gap) — a THIRD distinct bug found the same day as the
  Mill Fabric Article # backfill above, in the same function.**
  Owner-reported: `KTB-00029` (LF Style# `LFBP-1WTP0002`) has 2 BeProduct
  colors ("Early Hours", "Earthy Hours") and a BOM of 3 materials (Main
  Fabric + 2 Fabric) — expected 2×3=6 DTC WIP rows; only 4 existed, with
  the 2nd color ("Early Hours") stuck at just its Main Fabric row.
  **Root cause**: `p10_pull_bom_and_enrich.py` groups WIP rows by
  `bp_style_number` ONLY (never extracted `color`/`"Color / Wash"` into the
  row dicts passed to `plan_style_enrichment()` at all), so a style's
  MULTIPLE colorways were combined into one flat list. The "is this Fabric
  segment already represented?" check (`claimed_target_keys`) then operated
  GLOBALLY across every color combined — once "Earthy Hours"'s rows (which
  existed and got enriched first) satisfied both Fabric segments, "Early
  Hours" (added later as a fresh placeholder row) was wrongly treated as
  "already covered" too, and never got its own 2 Fabric-segment rows
  inserted.
  **Fix**: `dtc/python/sync/bom.py`'s `plan_style_enrichment()` gained a new
  `color_key` parameter (default `"color"`) and now groups `existing_rows`
  by color INTERNALLY, running the entire per-row match/backfill/insert
  decision tree independently per color group — each colorway now gets its
  own full segment coverage regardless of what any other colorway already
  has. Rows without a color value (or a single-color style) collapse into
  one implicit group, identical to the pre-fix behavior — zero regression
  for callers that don't track color.
  `dtc/notebooks/p10_pull_bom_and_enrich.py`'s Step 2 now extracts
  `"color": row_fields.get("Color / Wash")` into each WIP row dict.
  **A second, adjacent fix in the same pass**: `plan_style_enrichment()`'s
  Placement/Content diff was comparing raw values directly (`!=`), so DTC's
  current blank (`None`, via `get_json_object` on a missing cell) vs. the
  source's own blank (`""`, a real empty string in the techpack JSON) were
  treated as "different" — causing a spurious `{"Placement": ""}`-style
  PATCH on every already-blank, otherwise-fully-matched row, every run
  (violates Ground Rule #6's lean-PATCH requirement). Fixed with a new
  `_values_differ()` helper: two blank values (by the existing `_blank()`
  definition) are never considered different, regardless of which blank
  REPRESENTATION each side happens to use.
  **Live-validated**: re-ran the real `plan_style_enrichment()` against
  `KTB-00029`'s exact real data (3-segment BOM, "Earthy Hours" fully
  enriched, "Early Hours" stuck at 1 row) — before the fix: 0 actions for
  "Early Hours" (bug reproduced exactly); after the fix: exactly 2 correct
  INSERT actions for "Early Hours" (`WV-0064`/`WV-0047`, both with their
  real Content values), and zero spurious Placement-blank-diff updates for
  either color. 3 new unit tests added (`dtc/tests/test_bom.py` `[11o]`-
  `[11q]`, including the color-collapse-without-color-key equivalence
  check and the blank-vs-blank diff suppression).

- **Phase 10: blank Mill Fabric Article # backfill added, 2026-09-10 (owner
  spec) — fixes a live-confirmed "frozen row" bug.** Investigating a
  specific reported case (KTB-00025, legacy code `112358013`: real
  `Content`/`Fabric Group`/`Placement` but blank `Mill Fabric Article #`)
  found the root cause: the style's 4 existing WIP rows were first-enriched
  at a point when the source's `**SupplierRefNo` was still blank
  (`mill_fabric_article=None` baked in), and the source has since been
  updated with real values (`WV-0003`, `LTCL6080`, `fb01art`). Because the
  match key is `(Fabric Group, Mill Fabric Article #)` together, a blank
  vs. a real value never matches, so `plan_style_enrichment()` treated
  these rows as "some other real, unrecognized value" and left them
  permanently untouched — Mill Fabric Article # could never self-heal.
  **Live-tested the actual `plan_style_enrichment()` code against this
  exact case and found a SECOND, more urgent consequence**: since none of
  the 4 existing rows' keys matched either real "Fabric" segment
  (`LTCL6080`/LINING, `fb01art`/HEM), the very next Phase 10 run would have
  additionally INSERTED 8 wasteful duplicate rows (4 existing rows x 2
  unmatched segments) — the same row-explosion risk flagged during the
  2026-09-09 "2nd revision" migration, except recurring on an ONGOING basis
  (any row first-enriched while `SupplierRefNo` was blank will explode the
  next time the source fills it in), not a one-time migration cost.
  **Fix** (`dtc/python/sync/bom.py`'s `plan_style_enrichment()`): a row
  with a currently-BLANK Mill Fabric Article # is now matched to a target
  sharing its exact Fabric Group, disambiguated by Placement when more
  than one target shares that Fabric Group (e.g. two "Fabric" segments);
  if still ambiguous after that, NO backfill is attempted (never guess
  wrong) and the row falls through to the pre-existing "leave untouched"
  behavior. A successful backfill match (a) sets Mill Fabric Article # in
  place, one-way only (blank -> real; a row with a REAL but
  non-matching article # is never touched by this path, unchanged from
  before), and (b) is excluded from the "genuinely new -> insert" fan-out,
  so the target is fixed in place instead of ALSO being duplicated.
  **Live-validated**: re-ran the real `plan_style_enrichment()` against
  KTB-00025's actual data — before the fix: 0 fixes + 8 wasted inserts
  would have resulted; after the fix: exactly 4 correct backfill UPDATEs
  (2 Main Fabric rows -> `WV-0003`, 2 Fabric/LINING rows -> `LTCL6080`,
  the LINING match disambiguated via Placement since 2 Fabric targets
  existed) + exactly 4 INSERTs for the genuinely-new `fb01art`/HEM segment
  (down from 8). 4 new unit tests added (`dtc/tests/test_bom.py`
  `[11l]`-`[11n]`, 5 assertions), including the ambiguous-Placement-tie
  case and the one-way-only (never overwrites a real value) guarantee.

- **"(BACKUP)"-named request exclusion extended to the DTC WIP document,
  2026-09-10 (owner spec).** Previously only Phase 0/XTS Master excluded
  `(BACKUP)`-named requests (via an exact-name allow-list — not adaptable to
  WIP's 75+ dynamically-named requests). `phase1.is_in_scope()` — the SINGLE
  shared choke point for all WIP request registry/scoping
  (`registry.build_registry_row()`, `registry.refresh()`'s pre-filter, and
  `p1_dtc_request_manager`'s creation-eligibility check all call it) — now
  also rejects any reference matching `\(backup` (case-insensitive, checked
  against the whole string before token-parsing, not anchored to a specific
  token position). **Deliberately NOT applied to LinePlan**:
  `p9a_pull_lineplan_to_delta.py` has its own independent, unfiltered
  discovery loop and never imports/calls `phase1.is_in_scope()` at all — the
  project team has not decided on a LinePlan naming convention and
  explicitly wants `(BACKUP)`-named LinePlan requests included (2026-09-01
  decision, unchanged).
  **Live-discovered the exact scope of the problem (2026-09-10)**: of 86
  active KTB WIP requests, **81 (94%) were "(BACKUP)"-named** and were
  incidentally parsing as in-scope under the old logic (customer + season
  code tokens still matched even with a BACKUP marker elsewhere in the
  string) — this is the direct root cause of the long-standing
  "~199/227 `dtc_wip_ktb` rows have a null `bp_style_number`" data-quality
  issue noted throughout this log. **A second live-discovered variant, same
  day**: the marker isn't always the exact string `"(BACKUP)"` — a
  second-generation `"(BACKUP 2)"` form also exists (e.g. `"KTB SS28
  (BACKUP 2) Wrangler Collaborations"`), which an exact `"(backup)"`
  substring match would have missed entirely. Fixed by matching the regex
  `\(backup` (open-paren + "backup" prefix only, not requiring an exact
  closing `)`) so every live-observed variant is caught regardless of what
  follows inside the parens or where in the reference string it appears
  (immediately after customer, in the brand portion, prepended before the
  customer token, or appended at the very end — all four positions were
  live-observed).
  **Live-validated end-to-end 2026-09-10**: triggered a fresh full main-job
  run (`run_id 728801909421944`, all 20 tasks `SUCCESS`) immediately after
  deploying the fix, during a live rename the DTC team happened to be
  performing in real time on the two previously-clean request names — this
  incidentally proved the fix catches renames mid-flight too (a request
  that had been clean moments earlier and was then renamed to include
  `(BACKUP)`/`(BACKUP 2)` was correctly excluded on the very next run,
  and `dtc_wip_ktb` correctly settled to containing rows from exactly the
  ONE genuinely-in-scope request, replacing what had been ~49 distinct
  `request_id`s' worth of accumulated stale/BACKUP data from prior runs).
  Note the registry's `request_is_active` column is set to `'N'` by the
  existing `reconcile_inactive()` mechanism for every request excluded by
  scope (not just genuinely-DTC-inactive ones) — this is pre-existing,
  by-design behavior (the reconcile step's `keep_ids` was always the
  in-scope subset, not the full active list), now simply exercised much
  more heavily since far more requests are correctly excluded from scope.
  Added test coverage: `dtc/tests/test_phase1.py` (`[2b]`, 7 new
  assertions covering both marker variants, all 4 positions, and
  case-insensitivity).

- **New Phase 2 field wired up, 2026-09-09 (owner spec): DTC "Factory
  Production Country for Main Factory" → BeProduct "COO"
  (`country_of_origin`), the FIRST Phase 2 field requiring an actual value
  transform rather than a straight copy.** DTC's WIP column stores a plain
  2-char ISO 3166-1 alpha-2 country code (live-confirmed real values in
  `dtc_wip_ktb`: `"US"`, `"BD"`, `"IN"`). BeProduct's `country_of_origin`
  field (display name "COO", `fieldType: "DropDown"`, discovered live via
  `api.style.folder_schema(folder_id="053ed578-6759-4f36-b6df-d1bd89b3b7be")`
  for the real KTB folder — per Ground Rule #1/#2, the fieldId was NOT
  guessed) stores the country NAME instead (e.g. `"United States"`), never
  the code. Both `folder_schema()`'s embedded `properties.Choices` and a
  direct `api.raw_api.get("MasterData/country_of_origin")` call return an
  **identical** 251-entry choice list (249 unique 2-char codes, 2
  placeholder/blank entries) — confirming this field follows the exact same
  MasterData pattern already used for `parent_vendor`/`factory`.
  **Implementation**:
  1. `beproduct/p5utl_beproduct_master_data_sync.py` — added `"coo":
     "country_of_origin"` to `MASTER_DATA_FIELDS`, so the existing
     admin-triggered PULL_ONLY mode syncs the full code→name choice list
     into a new `lft.beproduct.beproduct_master_coo` table (zero other code
     changes needed — the pull loop already iterates the dict generically).
  2. `dtc/python/sync/phase2.py` — added `"Factory Production Country for
     Main Factory": "country_of_origin"` to `REVERSE_HEADER_FIELDS`, plus a
     new pure function `resolve_coo_country_name(dtc_country_code,
     code_to_name)` (case-insensitive, whitespace-trimmed lookup; returns
     `None` — never a raw-code fallback — on blank/unmatched input, so an
     unrecognized code is treated as "no value" rather than corrupting
     BeProduct's DropDown with an invalid string). `build_beproduct_
     updates()` gained a new optional `value_transforms: Dict[str,
     Callable]` parameter (defaults to `None`/`{}`, zero regression for
     every existing caller/field) applied to the raw DTC value BEFORE
     normalization/diffing/writing — this is a general mechanism, not
     COO-specific, so any future field needing a lookup-based transform can
     reuse it the same way.
  3. `dtc/notebooks/p2_push_dtc_to_beproduct.py` — loads
     `beproduct_master_coo` into a `{code: name}` dict (fails OPEN to an
     empty dict with a warning, not a raise, if the admin hasn't run
     `p5utl` yet — every OTHER Phase 2 field keeps working normally), builds
     `value_transforms = {"Factory Production Country for Main Factory":
     lambda v: phase2.resolve_coo_country_name(v, coo_code_to_name)}`, and
     passes it to `build_beproduct_updates()`. `HDR_ID_TO_COL`/
     `ALL_PHASE2_COLUMNS` already pick up the new field automatically
     (derived from `REVERSE_HEADER_FIELDS`) — no other notebook change
     needed; the NOOP diff correctly compares the TRANSFORMED value against
     BeProduct's current (already-a-name) value, not code-vs-name.
  **Live-validated**: the real `resolve_coo_country_name()` against the
  real BeProduct MasterData response — `"US"→"United States"`,
  `"BD"→"Bangladesh"`, `"IN"→"India"`, `"us"`/`" bd "` (case/whitespace)
  normalize correctly, `"ZZ"` (unrecognized) → `None`. 13 new unit tests in
  `dtc/tests/test_phase2.py` (pure-logic; the Spark/SDK notebook wiring
  itself was not live-executed this pass — flagged for a follow-up
  `p2_push_dtc_to_beproduct.py` dry-run once `p5utl` has been run at least
  once in this environment to populate `beproduct_master_coo`).
  Docs updated: `docs/beproduct_style_interested_fields.txt` (new table row
  + fixed a stale "no BeProduct target yet" note on `Main Factory Customer
  ID`'s prose summary that predated its 2026-09-03 wiring), this file's
  "Current direction partition" section.

- **Phase 10 `Mill Fabric Article #`/`Content` mapping corrected, 2026-09-09
  (owner spec) — reverses the 2026-09-03 decision that had Phase 10 write
  `Content` from BOM `material_name`.** Live-discovered 2026-09-08 (user
  report: "Content field now gets the material_no value"): for a newer
  batch of test styles (KTB-00024, 00026-00028, 00031), the raw
  `bom_unified` source JSON itself has `material_name`/`material_no`
  effectively swapped vs. the original 2026-09-02 assumption — e.g.
  `material_name: "WV-0063"` (code-shaped) / `material_no:
  "LF-BD26-000005--TW"` (a different code format) — the OPPOSITE of early
  KTB-00016/00020/00023 data, where `material_name` held a real descriptive
  content string. This had already pushed material-CODE-shaped garbage into
  live DTC "Content" cells for those 5 styles. Owner-issued corrected spec
  (2026-09-09, confirmed against live `bom_unified` for KTB-00029/KTB-00030
  — `material_name` = `"LTCL6080"` / `"WV-0064"` / `"WG24-01706"`,
  consistently article-code-shaped): **`Mill Fabric Article #` now sources
  from `material_name`** (not `material_no`, which is no longer read by
  this module at all), and **`Content` is NEVER written by Phase 10 at
  all**, in any code path (first-time enrichment, upsert, or INSERT) —
  applies "irrespective to active test cases," i.e. regardless of what any
  individual older/inconsistent test style's data happens to look like.
  Implementation: `dtc/python/sync/bom.py` — `extract_enrichment_fields()`
  / `to_wip_fields()` no longer produce a `content`/`Content` key at all;
  `WIP_FIELD_CONTENT` constant removed; `plan_style_enrichment()`'s
  `content_key` parameter and the whole Content-backfill branch removed.
  `dtc/notebooks/p10_pull_bom_and_enrich.py` and `p9a_build_costing_chart.py`
  docstrings updated to match. `dtc/tests/test_bom.py` rewritten (fixtures
  now use the corrected `material_name`-is-a-code-value shape; all
  Content-related assertions removed, replaced with explicit "no Content
  key produced" checks). **Known downstream consequence, accepted**:
  `costing_chart.fabric_content` (sourced from WIP's "Content" column) will
  typically stay blank now unless DTC's own (already confirmed unreliable
  in UAT) Content-population trigger fills it independently — degrades
  gracefully since `fabric_content` is only one of six `duty.
  PRODUCT_DESCRIPTION_COLS` concatenated for NT Orbit, not a hard blocker.
  **Consequent `p9a_build_costing_chart.py` Step 1b filter change (same
  day, owner spec)**: costing-chart formation now explicitly GATES on WIP
  "Content" being non-blank, on top of (not instead of) the existing
  `material_no` non-blank requirement — a "Main Fabric" WIP row whose
  Content is still blank this round (DTC's trigger/an external job hasn't
  filled it in yet) is excluded from `costing_chart` THIS ROUND, consistent
  with the pipeline's existing "never revert, wait for a later round"
  philosophy (not a permanent exclusion). Per owner: "Content is to be
  filled by DTC trigger / job externally before next step (OrbitDuty etc).
  So costing calculation gate on field nullness." Step 1b's single
  aggregate drop-count was also split into four separate per-reason counts
  (blank `material_no`, blank `bp_style_no`, blank/placeholder `Content`,
  `fabric_group != "Main Fabric"`) so this gate's real-world impact is
  visible/auditable going forward, since it is expected to become the
  dominant reason for exclusion.
  **Not yet live-validated**: the actual `costing_chart` row-count impact
  of the new Content gate (requires a live `build_costing_chart` run against
  current WIP data); the already-corrupted live "Content" cells on the 5
  affected styles have NOT been cleaned up yet (flagged, pending explicit
  go-ahead).

- **Lifecycle gating implemented for the `EXCLUDED_STATUSES`/`ktb_styles`
  gap flagged below (2026-09-08, same-day follow-up), live-validated where
  possible.** Resolves the "Found but NOT fixed" item in the conflict-scan
  entry immediately below: `ktb_styles` now always retains every style
  regardless of Product Status (fixed in `p1p7_beproduct_style_sync.py` —
  `styles`, the full unfiltered list, is what's written; a separate
  `styles_for_enrichment` list is used only to skip wasted `app_get()` calls
  for Finalized/Drop styles). The actual Finalized/Drop staging exclusion
  moved to `p1p7_beproduct_to_dtc_transform.py`'s new Step 6b, backed by a
  new pure-Python module `dtc/python/sync/lifecycle.py`
  (`dtc/tests/test_lifecycle.py`, 20 assertions): while a style's Product
  Status is terminal (`Finalized`/`Drop`) and the DTC WIP row's OWN current
  "Product Status" hasn't caught up yet, one last push still goes through
  (so the terminal status itself reaches DTC); once WIP's own value matches,
  every future run leaves that row alone; a REACTIVATED style (moved back to
  a non-terminal status) resumes syncing automatically, no special-casing.
  Step 6b reads last run's `dtc_wip_<customer>` snapshot (this notebook runs
  in PARALLEL with `pull_dtc`, so there's an intentional one-run lag before
  a freshly-pushed terminal status is recognized as "caught up" — not a
  bug). Fails OPEN (skips the filter, doesn't raise) if the WIP table is
  unreadable, e.g. first-ever deployment run.
  **Live-validated** (via a local Databricks SQL warehouse connection, proxy
  connectivity restored the same day): `get_json_object(data_json,
  "$['Product Status']")` matches the table's own flattened
  `col_Product_Status` column exactly on every sampled row; the exact join
  keys (`bp_style_number`, `color`/`color_wash`) achieve a 100% match rate
  against the real, already-populated `beproduct_to_dtc_staging` table (11/11
  rows); 8 real (bp_style_number, color_wash) identities have multiple
  physical WIP rows (Phase 10 material duplicates) and every one agrees on a
  single Product Status value, confirming the `groupBy` + `first(ignorenulls)`
  dedup is safe; running the real `should_include_in_staging()` against all
  11 live staging rows' actual (BeProduct status, WIP status) pairs returns
  `include=True` for 100% of them (expected — no live style currently holds
  a terminal status, so this is a no-regression check, not a terminal-status
  transition check).
  **NOT live-validated** (no real terminal-status data exists yet to exercise
  it): the actual "one last push, then leave WIP alone" transition itself,
  and reactivation. Both are unit-tested only.
  **A second, DTC-only mechanism — a WIP `"Active / Dropped"` column hard-
  stop (`is_wip_row_dropped()`) — was wired in as a deliberate SAFE NO-OP**,
  per owner decision, despite failing live verification: `GET
  /v1/views/{id}` still 403s at the Azure Application Gateway (a
  previously-documented intermittent issue with this specific endpoint); a
  full `sheetData` scan across all 84 active KTB WIP requests (168 unique
  populated columns) found zero column matching `active`/`drop` — the
  closest candidate, a flattened `col_Vendor_Active` column, is 100% NULL
  across all 237 WIP rows and is very likely an unrelated per-vendor field,
  not this flag. `is_wip_row_dropped()` defaults to "not dropped" on a
  missing/blank value, so this mechanism simply never fires today; it
  activates automatically once the real column name is confirmed (correct
  `lifecycle.WIP_FIELD_ACTIVE_DROPPED` first, if it turns out to differ from
  the project-team-supplied `"Active / Dropped"`).
  **Found in passing, NOT fixed (new gap, flag for a follow-up decision)**:
  `lifecycle.py`'s original docstring incorrectly claimed
  `p9a_build_costing_chart.py`'s Step 1b already excludes terminal-status
  WIP rows from `costing_chart` — live-checked, no such filter exists there.
  A Finalized/Drop style's WIP row can therefore still enter `costing_chart`
  today (the docstring has been corrected to note this rather than assert
  it's handled).
  Also fixed in the same pass: `scripts/check_dtc_view.py` and
  `implement_prompts.txt` no longer hardcode the (private) proxy address —
  proxy config is environment-only (`HTTPS_PROXY`/`HTTP_PROXY`), never
  committed to a tracked file.

- **Full-pipeline conflict scan (2026-09-08)** — found and fixed 3 real
  issues, confirmed the rest clean:
  1. **`run_phase3` was a dead job parameter** since the 2026-09-03 3-job
     split — `phase3_images` moved to its own `images` job but never got a
     `run_phase3` widget wired up, so the parameter had zero effect (the
     task always ran unconditionally). Fixed by adding the same
     no-op-exit pattern already used for `run_phase1`/`run_phase10`
     (`beproduct/p3_beproduct_to_dtc_images.py`), wired into
     `build_images_tasks()` in `scripts/deploy_job.py`.
  2. **`push_duty_to_wip` was a genuinely redundant dead parameter** —
     superseded by `run_phase9b`, which already gates the whole
     `push_duty_rates` task (its only job post-split). Removed from
     `JOB_PARAMS`.
  3. **`standalone/beproduct_style_push.py` duplicated Phase 2's write
     targets** — its `INTERESTED_FIELDS` still included `PARENT VENDOR`/
     `FACTORY` (fieldIds `parent_vendor`/`factory`), which are Phase 2's
     EXCLUSIVE DTC→BeProduct write direction (`phase2.REVERSE_HEADER_
     FIELDS`). This directly contradicted the tool's own documented intent
     (`standalone/README.md`: "kept separate so it can't accidentally
     fight the field-ownership partition"). Removed both entries — this
     tool is manual/legacy and not part of the automated pipeline, so this
     was a latent risk (only triggered if someone runs it manually), not
     an active conflict.
  Also **re-verified clean**: no new Phase 1/10/9b DTC WIP column
  ownership conflicts (the one overlap, `Fabric Group`/`Placement` via
  `DEFAULT_FILL_COLS`, is the documented intentional 2026-09-03 design);
  no dangling `deploy_job.py` task dependencies; `p9a_build_costing_chart.py`
  and `p10_pull_bom_and_enrich.py` fully consistent with the latest
  2026-09-07 decisions; no other live notebook duplicates `duty.COSTING_KEY`/
  `PRODUCT_DESCRIPTION_COLS`/`DUTY_CACHE_KEY_COLS` (the retired
  `p9b_fill_duty_rates.py` has its own pre-2026-09-07 stale copy of
  `COSTING_KEY`, but it's explicitly banner-marked superseded and not
  scheduled).
  **Found but NOT fixed (flagged for a decision)**: `EXCLUDED_STATUSES`
  (`{"Finalized", "Drop"}`) is applied to the SAME `styles` list that
  becomes `ktb_styles`'s write payload in `p1p7_beproduct_style_sync.py`
  — meaning a style transitioning to Finalized/Drop actually DISAPPEARS
  from `ktb_styles` entirely on the next FULL sync, contradicting the
  code's own comment ("still written to ktb_styles (full picture), but
  filtered out in the transform") — no such compensating filter exists in
  `p1p7_beproduct_to_dtc_transform.py`. Downstream, `p10_pull_bom_and_
  enrich.py` joins directly against `ktb_styles`, so this also silently
  drops such a style from Phase 10 enrichment. Not fixed pending a design
  decision: should `ktb_styles` genuinely retain the full historical
  picture (requiring the filter to move into the transform step, with a
  separate exclusion list for the app-enrichment API-call-avoidance
  purpose), or is disappearing from `ktb_styles` actually acceptable/
  intended for terminal-status styles?
  Also updated `docs/DIAGRAM.md` comprehensively (see next entries) and
  `scripts/deploy_job.py`'s own top-of-file docstring (stale since the
  3-job split — still described one unified job's DAG).
- **Project team decisions (2026-09-07, same day as the entry below,
  REVERSING part of it): (1) no BeProduct fallback for `Content` after
  all; (2) only "Main Fabric" rows enter `costing_chart`.**
  1. **"Keep DTC WIP true to BOM extraction."** The `fallback_content`
     mechanism added earlier the same day (next entry below) was explicitly
     REVERSED after project team discussion: if techpack
     (`customer_teckpack_style_latest`) has no/null `bom_unified` for a
     style, `Content` is left exactly as-is (typically blank) — even though
     BeProduct's own `core_main_material` header field may hold a plausible
     value. `bom.plan_style_enrichment()`'s `fallback_content` parameter and
     all associated logic were removed; `p10_pull_bom_and_enrich.py`'s Step
     1 join reverted from LEFT back to INNER (a style with no BOM match
     again simply never reaches the per-style loop at all). The Content
     values my earlier fix had pushed to KTB-00016/KTB-00021's live WIP
     rows were manually cleared back to blank to match this decision.
     Clarifying note from the team: as long as a style's Product Status is
     NOT in `("Finalized", "Drop")`, its BOM extraction is expected to keep
     being updated over time — so a style missing techpack data today is
     not a permanent gap, it'll start flowing through again once techpack
     data appears for it. `"Drop"` was added to `EXCLUDED_STATUSES` in
     `p1p7_beproduct_style_sync.py` (previously only `"Finalized"`) to match
     — exact choice-list spelling confirmed live via `folder_schema`
     (`"Drop"`, code `"DR"`).
  2. **Only `Fabric Group == "Main Fabric"` rows enter `costing_chart`.**
     The "Fabric" segment duplicate rows Phase 10 creates (one extra
     physical WIP row per "Fabric" segment — see `sync/bom.py`) are now
     EXCLUDED entirely from `costing_chart` via a new filter in
     `p9a_build_costing_chart.py` Step 1b (`fabric_group` newly extracted
     via `jcol` for this purpose). Consequently, Duty/Tariff rate/HTS Code
     (Phase 9b) are only ever computed and pushed back to WIP for "Main
     Fabric" rows — "Fabric" segment rows never receive them, by design.
  **Live-reverified end-to-end** after both changes: `costing_chart`
  correctly dropped back to only `KTB-00023`'s rows (KTB-00016/KTB-00021
  excluded again, Content confirmed blank on their live WIP rows), and of
  KTB-00023's own two materials (`WV-0063` "Main Fabric", `WV-0047`
  "Fabric"), only the `WV-0063` "Main Fabric" rows remain — the `WV-0047`
  row is gone. The unrelated `tariff_rate` carry-forward fix (see below)
  was reconfirmed still intact through this same rebuild. Test changes:
  removed `test_bom.py [6e2]`-`[6e5]` (the now-reverted fallback_content
  tests), replaced with a single `[6e2]` confirming zero actions with no
  fallback consulted at all.
- **Phase 10 gained a BeProduct-sourced `Content` fallback for styles with
  no current TPM/BOM match (2026-09-07, owner spec: "KTB-00016 should have
  Content = '97% Cotton, 3% Spandex', KTB-00021 = '100% Cotton'").**
  Follow-up to the completeness-filter hardening above: with `fabric_content`
  now REQUIRED to be non-blank, `KTB-00016`/`KTB-00021` (no current
  `bom_unified` match — see earlier entry) were permanently excluded from
  `costing_chart`, even though they'd been correctly enriched by Phase 10 in
  the past (real `Fabric Group`/`Placement`/`Mill Fabric Article #`) and had
  factory data assigned. Live-discovered: BeProduct's OWN header field
  `core_main_material` ("MAIN MATERIAL CONTENT") holds the exact correct
  values for both styles — and was ALREADY being correctly extracted into
  `ktb_styles.bom_material_1` by `p1p7_beproduct_style_sync.py`'s
  `BOM_MATERIAL_FIELDS`, just never consumed by anything downstream. (Also
  discovered in passing: the OTHER entry in that same dict,
  `"main_material_content": "main_material_content"`, references a
  non-existent BeProduct fieldId — always returns null; `bom_material_1`
  via fieldId `core_main_material` is the one that actually works. Left
  as-is, not in scope of this fix.)
  **Implementation**: `bom.plan_style_enrichment()` gained a new
  `fallback_content` parameter — consulted ONLY when `build_target_segments()`
  returns `None` (no current TPM/BOM data at all), and even then ONLY
  backfills a currently-blank `Content` on rows that are ALREADY enriched
  (real `Fabric Group`); never touches a genuinely never-enriched
  (placeholder) row, since first-time enrichment still needs real BOM
  segment data this fallback can't provide. `p10_pull_bom_and_enrich.py`'s
  Step 1 join changed from INNER to LEFT (a style with no BOM match must
  still flow through with `bom_unified=NULL` so this fallback gets a
  chance to run — an INNER JOIN silently dropped it before ever reaching
  the per-style loop) and now selects `ktb_styles.bom_material_1` as
  `fallback_content`, passed through to `plan_style_enrichment()`. 5 new
  unit tests (`test_bom.py [6e2]`-`[6e5]`).
  **Live-validated end-to-end**: `fill_bom_data` correctly pushed
  `Content="97% Cotton, 3% Spandex"` / `"100% Cotton"` to both styles' live
  WIP rows (both physical requests); `repull_dtc_bom` (needed to refresh
  Delta's `dtc_wip_ktb` snapshot before `build_costing_chart` could see the
  live push — a repull is always required between any live WIP write and
  the next chart rebuild) + `build_costing_chart` then correctly produced
  `costing_chart` rows for all 3 styles (`KTB-00016`, `KTB-00021`,
  `KTB-00023`) with the right `fabric_content` on each.
- **Costing chart completeness filters hardened + NT Orbit lookup scope
  widened (2026-09-07, owner spec, live-discovered while validating a
  reported stale `tariff_rate=null` on `KTB-00023`/`WV-0063`/BD).**
  1. **`p9a_build_costing_chart.py` Step 1b gained two more exclusions**
     (in addition to the existing `material_no IS NOT NULL` filter):
     `bp_style_no IS NULL` (legacy "(BACKUP)"-request pollution — confirmed
     4 such rows were reaching `costing_chart` before this fix, now 0) and
     `fabric_content IS NULL OR fabric_content = "Main Fabric"` (guards
     against a WIP row whose `Content` field is still literally the
     `Fabric Group` placeholder-like string — the live-confirmed root cause
     of the original bad classification, see next entries).
  2. **`color_name` added to `duty.PRODUCT_DESCRIPTION_COLS`** — REVERSES
     the earlier design intent (`cache_key()`'s old docstring explicitly
     said multiple colors of the same style/slot should share one NT Orbit
     cache entry, since color doesn't affect HS classification). Owner
     decision: a new row now only reuses a previous NT Orbit result when
     `style_description`, `color_name`, `fabric_content`, `gender`,
     `class_name`, AND `sub_class` all match exactly — different colors
     always get their own lookup/cache entry now. Since
     `PRODUCT_DESCRIPTION_COLS` feeds both the actual NT Orbit request text
     and the persistent cache key (derived from the same string), this one
     change achieves both. New tests: `test_duty.py [2b]`.
  3. **Live-confirmed NT Orbit IS reproducible**: a fresh call with the
     CURRENT (post-`Content`-fix) product description returns identical
     `hts_code=6202407511`/`duty_rate=0.277`/`tariff_rate=0.1` for BOTH
     BD and IN origins — the previously-observed BD/IN asymmetry in
     `costing_chart` was a data-staleness artifact, not an API
     non-determinism issue (see next entry).
  4. **Root cause of the stale BD row — a self-reinforcing loop via the
     WIP "fallback" mechanism**: `p9a_build_costing_chart.py` re-reads
     `hts_code`/`duty_rate_us/ca/mx` FROM THE LIVE WIP ROW'S own per-slot
     columns on every rebuild ("NT Orbit fallback" — whatever was
     previously pushed there persists across rebuilds), while `tariff_rate`
     always resets to `NULL` (no live WIP column exists for it yet,
     `WIP_TARIFF_COLS_LIVE=False`). A pre-`Content`-fix run had pushed a bad
     classification (from a nonsensical `"... Main Fabric ..."` product
     description) onto the live WIP row's "Main Factory HTS Code"/"Main
     Factory Duty Rate (US)" cells. Every subsequent rebuild re-adopted
     those stale values as non-blank, so `duty.markets_needing_lookup()`
     (which only checks whether `duty_rate_us` itself is blank) always
     considered the US market "already done" — the US market was never
     re-queried, so `tariff_rate` (no WIP fallback, always reset to NULL)
     could never be filled again. **Fixed** by manually clearing the stale
     "Main Factory HTS Code"/"Main Factory Duty Rate (US/CA/MX)" and
     "Factory 1 - HTS code"/"Factory 1 - Duty Rate (US/CA/MX)" cells on the
     live WIP row (`patch_rows` with `null` values), breaking the loop.
  5. **Live-discovered a related, more general job-sequencing gap while
     re-validating the fix**: `push_duty_rates` (Step 55) ALWAYS runs
     immediately after its OWN main-job run's `build_costing_chart` (Step
     40) — with nothing in between. Since `costing_chart` is fully
     rebuilt by every `build_costing_chart` run (wiping every field except
     the WIP fallback), any value the SEPARATE `duty_compute` job filled in
     BETWEEN two main-job runs is wiped again the instant the NEXT main-job
     run's `build_costing_chart` executes — `push_duty_rates` (same run,
     immediately after) can only ever see the POST-wipe state. This means a
     freshly-computed value can only survive to be pushed if `duty_compute`
     happens to run in the exact window between a `build_costing_chart` run
     and ITS OWN `push_duty_rates` — which cannot be guaranteed across two
     independently-scheduled jobs. **Workaround used to unstick the BD row**:
     `w.jobs.run_now(job_id=294837488757511, only=["push_duty_rates"])` —
     the Jobs API's task-selective run support, which runs ONLY the named
     task (all others report `DISABLED`, not skipped) against whatever is
     CURRENTLY in `costing_chart`, without triggering `build_costing_chart`
     first. Re-running `duty_compute` then immediately `only=
     ["push_duty_rates"]` (no rebuild in between) is the reliable manual
     recipe until this sequencing gap is addressed structurally (e.g. by
     having `push_duty_rates` itself trigger/wait on a fresh `duty_compute`
     pass, or reordering the two jobs' relationship) — not yet implemented,
     flagged here for a future decision.
  **Live-reverified end-to-end**: after clearing the stale WIP cells,
  `duty_compute` correctly recomputed BD with `tariff_rate=0.1` (matching
  IN and the direct fresh-API-call check), and the task-selective
  `push_duty_rates` run correctly wrote `Main Factory HTS Code=6202407511`,
  `Main Factory Duty Rate (US/CA/MX)=0.277/0.18/0.35` back onto the live
  WIP row — confirmed via a direct `get_sheet()` read.
- **`tariff_rate` was being wiped by every `build_costing_chart` run —
  structurally fixed 2026-09-07 (same-day follow-up; owner-reported "now
  costing_chart.tariff_rate is null across all 5 rows" within hours of the
  fix above).** Confirmed via `DESCRIBE HISTORY`: a routine SCHEDULED
  main-job run (not a manual trigger) had rebuilt `costing_chart` and
  wiped `tariff_rate` back to `NULL` — proving the item 5 workaround above
  (`only=["push_duty_rates"]`) was never going to be durable, since the job
  runs on a real 3x/day cron and every run's OWN `build_costing_chart`
  unconditionally resets `tariff_rate` to `NULL` (line `.withColumn(
  "tariff_rate", F.lit(None))`) with no fallback, unlike `hts_code`/
  `duty_rate_us/ca/mx` which persist via the live-WIP-row fallback.
  **Two structural fixes, both required together**:
  1. `p9a_build_costing_chart.py` Step 4b (new): before the final write,
     reads the EXISTING `costing_chart` table's own prior `tariff_rate`
     (keyed by the newly-shared `duty.COSTING_KEY` — moved out of
     `p9b1_compute_duty_rates.py`'s local copy into `sync/duty.py` so both
     notebooks use the identical definition) and carries it forward via
     `COALESCE(new, old)` — mirroring the WIP-fallback semantics Step 4
     already uses for the other duty fields, without needing a live WIP
     column.
  2. `duty.markets_needing_lookup()` — a SECOND, independent bug this
     exposed: once `duty_rate_us` is filled (which, per the item 4 root
     cause, is ALWAYS true after the very first push, since it persists via
     the WIP fallback), the US market was considered "already done"
     forever, so `duty_compute` would never even attempt a fresh lookup to
     backfill a genuinely-blank `tariff_rate` — confirmed live: a
     `duty_compute` run reported "Rows needing at least one NT Orbit
     lookup: 0" immediately after fix #1 alone, despite `tariff_rate`
     being blank on every row. Fixed: `markets_needing_lookup()` now ALSO
     adds `"US"` to the needed-lookups list whenever `tariff_rate` itself
     is blank, independent of whether `duty_rate_us` already has a value.
  **Live-reverified end-to-end** after both fixes: `duty_compute` correctly
  re-queried US (hit the persistent cache, no new API cost) and filled
  `tariff_rate=0.1` on all 5 rows; an ISOLATED `build_costing_chart`-only
  run (`only=["build_costing_chart"]`) confirmed `tariff_rate` now SURVIVES
  a full rebuild; a subsequent full main-job run (all 20 tasks `SUCCESS`)
  reconfirmed it stays intact end-to-end. New tests: `test_duty.py [3b]`
  (US re-queried on blank `tariff_rate` alone) and `[6b]` (shared
  `COSTING_KEY`).
- **Phase 3 sibling-copy: two live-discovered bugs, both fixed 2026-09-04
  (owner-reported: "KTB SS28 Wrangler Collaborations" rows stayed blank
  after a phase3_images run despite sibling rows already having images).**
  1. **Silent dedup skip.** `compute_image_uploads()` had a "only act once
     per (BP Style#, Color / Wash) key" dedup predating Phase 10 — Phase 10
     can create MULTIPLE physical WIP rows sharing the exact same (style,
     color) key (one "Main Fabric" row + one duplicate per "Fabric"
     segment). The dedup treated the SECOND such row as "already handled"
     the instant it saw the FIRST row's key — even when the first had a
     real image and the second was genuinely blank — silently skipping it
     with NO upload and NO skip record at all (invisible in every
     summary/log; this is why rows 9/10 of the affected request never even
     appeared as attempted). **Fixed** by removing the key-based dedup
     entirely — every physical row is now evaluated independently by its
     own `rowId`/`rowIndex`, which the "already imaged" / sibling-copy /
     missing-`rowIndex` checks already operate on anyway. 2 new unit tests
     (`test_phase3.py` `[15e]`-`[15f]`).
  2. **401 Unauthorized downloading a sibling-copy source URL.** Unlike
     BeProduct CDN URLs (self-contained SAS token in the query string),
     DTC's own image-hosting URLs (returned after our own earlier uploads,
     used as the sibling-copy source) require the SAME `x-api-key` auth as
     every other DTC API call — `download_image()`'s anonymous
     `requests.get` returned 401 for every sibling-copy attempt, live
     visible as `sync_log.reason='download_failed'`. **Fixed** in
     `p3_beproduct_to_dtc_images.py`: `download_image()` now attaches
     `x-api-key` when-and-only-when the URL's host matches the DTC
     connector's own `base_url` host (BeProduct CDN downloads stay
     anonymous/unchanged).
  **Live-reverified same day** against the exact reported request
  (`6a8fb9fed0062ee92c1f7d31`, "KTB SS28 Wrangler Collaborations"): before
  the fix, the plan showed only 3 of 5 blank rows even attempted (2 hidden
  by the dedup bug) and all 3 attempted uploads failed (401); after the
  fix, the plan correctly showed all 5 blank rows, and all 5 uploads
  succeeded — every row in the request now has a Style Image.
- **`brands_multi` ("BRANDS") removed entirely from `ktb_styles`/staging
  (2026-09-03, owner spec: "take out 'brands' from BP tables — use 'brand'
  (brand_hk) only now").** Since Phase 6, `brand` (field_id `brand_hk`,
  single-value) has been the composite-key Brand field; `brands_multi`
  (`BRANDS`, multi-select) was kept alongside it purely as non-key metadata.
  It is now removed from extraction entirely — `INTERESTED_FIELDS` in
  `beproduct/p1p7_beproduct_style_sync.py` no longer includes `"BRANDS":
  "brands"`, so `ktb_styles` stops gaining a `brands` column going forward
  (existing historical rows/column are NOT retroactively dropped from
  Delta — this only stops future population). `beproduct/
  p1p7_beproduct_to_dtc_transform.py`'s staging `select(...)` and debug
  `.show(...)` sample print both had their own `col("brands")` references
  removed (the `.show()` one would otherwise crash once the column stops
  existing on fresh rows). `standalone/beproduct_style_push.py` (a
  manual/legacy push-back utility with its own independent, already-stale
  copy of the field list) had its `"BRANDS": "brands"` entry removed too,
  for consistency. `beproduct/p5utl_beproduct_master_data_sync.py`'s
  `MASTER_DATA_FIELDS["brands"] = "brands_multi"` entry (a DIFFERENT
  concept — the Brands MasterData CHOICE LIST, not a per-style value) and
  `dtc_request_registry.brands` (parsed from the DTC request NAME string,
  also unrelated) were deliberately left untouched — neither is the
  per-style `brands_multi` field this change targets.
- **BeProduct "Customer Factory Code" now the DTC->BeProduct target for
  "Main Factory Customer ID" (2026-09-03, owner spec).** Previously
  UNSUPPORTED (no BeProduct field had been identified — see the
  `implement_prompts.txt` history: "Skip it for now"). Live-discovered
  2026-09-03: BeProduct's `customer_factory_code` (display name "Customer
  Factory Code", `fieldType: "Text"`, `MaxValueCharacters: 240`, tooltip
  "Customer Factory SAP Code") is a real header-level field, present in
  BOTH the "KTB" and "TEST KTB" folders (confirmed via `folder_schema` on
  both, and present on live style records in both). Its `LockField: true`
  UI property does **NOT** block API writes — live-tested via a
  write-then-revert round-trip against a real KTB style
  (`api.style.attributes_update(style_id, fields={"customer_factory_code":
  ...})`): the write succeeded and persisted, then was cleanly reverted to
  `None`. This is the same "declared read-only/locked flag is unreliable"
  pattern already seen with DTC's `isReadOnly` (see `compute_non_writable_
  cols`'s docstring in `sync/bom.py`) — BeProduct's `LockField` is
  evidently also a UI-only lock, not an API-enforced one. Wired into
  `sync/phase2.py`'s `REVERSE_HEADER_FIELDS` (moved out of the now-empty
  `UNSUPPORTED_FIELDS` tuple, kept as an empty tuple rather than removed so
  future unsupported columns have an obvious place to land). Live-verified
  end-to-end via `phase2_push`'s existing generic mechanism (`HDR_ID_TO_COL`
  is derived from `REVERSE_HEADER_FIELDS`, so no notebook code changes were
  needed at all — the SAME run that added this field also correctly pushed
  `parent_vendor`/`factory` for `KTB-00023`, proving the shared code path
  works). **Important discovery while attempting to manufacture a live
  non-null test value**: DTC's own "Main Factory Customer ID" field is
  `type: "lookup"` (`GET /v1/views/{id}` dynamicFields) — a COMPUTED field
  looked up from the "XTS Factory Master" request based on the row's
  selected "Main Factory (Sampling)", NOT a plain user-editable text
  column. A direct `PATCH` to this field returns `204` but the value is
  silently ignored/never persists (same non-obvious-rejection class as the
  formula/image fields found earlier — `isReadOnly: false` here too,
  equally unreliable). This does not affect correctness of the Phase 2
   read-and-forward logic (reading a DTC-computed lookup value and pushing
   it elsewhere is fine, mechanically identical to `parent_vendor`/
   `factory`) — it only means a genuine non-null value can't be manufactured
   by hand for testing; it only appears once a row's assigned factory has a
   populated "Customer Factory ID" in the real XTS Factory Master data.
   **Correction**: NOT the same data Phase 0 syncs into `beproduct_directory`
   — `xts_master.py` deliberately maps only Factory Code -> `directory_id`;
   "Customer Factory ID" is a distinct XTS field left unmapped (available in
   `dtc_xts_master_ktb.data_json` only), so `beproduct_directory` cannot be
   used to manufacture/inspect a test value either.
   **Live-validated end-to-end 2026-09-04** (owner-reported: DTC showed
   "132" for `KTB-00016` / factory "SUPPLIER HSDJHK" but a live re-read
   initially returned `None`) — confirmed via `dtc_xts_master_ktb.data_json`
   that "132" is genuinely present at the XTS Factory Master source; a
   second live re-read minutes later returned `"132"` on the WIP row itself
   (the DTC lookup had simply not resolved yet on the first read — same
   latency class as `Content`/`Fabric Type`, not a pipeline bug). Re-running
   `phase2_push` immediately picked it up: sync log payload
   `{"customer_factory_code": "132", ...}`, and `api.style.attributes_get`
   confirmed BeProduct's `KTB-00016` header now genuinely holds
   `customer_factory_code = 132`. No code change was needed — the mapping
   works correctly the moment DTC's lookup has resolved.
- **Costing chart key corrected to `material_no`, not `fabric_content`
  (2026-09-03, same-day owner correction).** An initial iteration keyed
  `COSTING_KEY` and the WIP-row disambiguation on `fabric_content`
  ("Content"); owner corrected this same day: "multiple material_no can
  have same content, and multiple style could share a lineplan" — so
  neither `fabric_content` (free text, not guaranteed unique per material)
  nor `lineplan_ref` alone (can be shared across styles) is a reliable
  material-level discriminator. `material_no` (Phase 10's own "Mill Fabric
  Article #") is the real, unambiguous one. Changed: `COSTING_KEY` in
  `p9b1_compute_duty_rates.py` (now `[..., lineplan_ref, material_no,
  supplier_type, supplier, factory]`), `p9b2_push_duty_to_wip.py`'s
  WIP-row lookup index (now `(bp_style_number, color_wash, "Mill Fabric
  Article #")`), and `p9a_build_costing_chart.py` — added a NEW
  `costing_chart.material_no` column (extracted from WIP "Mill Fabric
  Article #") and changed the Step 1b completeness filter from gating on
  `Content`/`Fabric Type` to gating on `material_no` being non-blank (the
  real signal now that Phase 10 always sets it together with Content —
  see next entry). Live-validated: `costing_chart` correctly carries
  `material_no` per row, and `KTB-00023` (which has BOTH a "Main Fabric"
  and a "Fabric" segment, each a different `material_no`) correctly
  produces separate, non-colliding costing rows per material.
- **Phase 10 now writes `Content` directly from BOM `material_name`,
  removing the DTC-trigger dependency that was blocking Phase 9a
  (2026-09-03, owner spec).** "Content" is normally populated by a
  DTC-internal trigger polling "Mill Fabric Article #", but that trigger's
  timing/conditions in UAT are unreliable (every KTB test row stayed blank
  for days after Mill Fabric Article # was set) — Phase 9a's original
  Content/Fabric Type completeness filter was permanently blocked as a
  result. Fixed at the source: `sync/bom.py` gained `WIP_FIELD_CONTENT`
  ("Content") and `extract_enrichment_fields()`/`to_wip_fields()` now
  include it (sourced from the BOM segment's `material_name`, same segment
  `fabric_group`/`mill_fabric_article` already come from) — an
  intentional, ACCEPTED dual-write with DTC's own trigger (per explicit
  owner instruction), unlike the earlier Phase 1/Phase 10 `Fabric Group`
  conflict, which was an unintentional bug. **Backfill for
  already-enriched rows**: a row enriched by an EARLIER version of this
  notebook (before `Content` existed as a target field) already satisfies
  `plan_style_enrichment()`'s "matches a current segment" branch and would
  otherwise never get `Content` populated (first-time enrichment is the
  only OTHER place it's written, and that's for un-enriched rows only) —
  so that branch now ALSO backfills `Content` whenever the row's current
  value is blank (never overwrites a real existing value, whether ours or
  DTC-trigger-written). New `content_key` parameter on
  `plan_style_enrichment()` (default `"content"`, pass `None` to disable).
  6 new/changed unit tests (`test_bom.py` `[6g]`-`[6h4]`). Live-validated:
  after redeploying, a run correctly backfilled `Content` on all
  previously-enriched styles that have real BOM data (17 updates, 0
  errors); `KTB-00016`/`KTB-00021` correctly remain blank throughout (their
  `bom_unified` is genuinely `None` in the source table — confirmed via a
  local replay of `plan_style_enrichment()` against live data, not a bug).
- **Phase 9a/9b/3 clarifications (2026-09-03, owner spec).**
  - **`fabric_content` was reading the WRONG WIP column** (`"Fabric Group"`
    instead of `"Content"`) since Phase 9a was first built — corrected.
    `"Content"` and `"Fabric Type"` (new `costing_chart.fabric_type` column)
    are DTC-internal-trigger-populated columns (the trigger polls "Mill
    Fabric Article #", which Phase 10 sets from TPM/BOM data) — NOT written
    by this pipeline directly. A WIP row missing either one now gets DROPPED
    from `costing_chart` entirely (`p9a_build_costing_chart.py` Step 1b,
    same treatment as a blank "Lineplan Ref #"), since incomplete material
    data would otherwise silently reach Phase 9b's NT Orbit classification.
    **Live-confirmed same day**: every current KTB test WIP row (231 total)
    still has both columns blank even where Mill Fabric Article # has been
    set for days — the DTC trigger's latency/conditions in UAT aren't fully
    understood. Filter live-validated: 203/231 dropped, 0 of the remaining
    28 matched a Lineplan Ref# (they're unrelated non-test WIP rows),
    `costing_chart` correctly ends up empty for our Lineplan-linked test
    styles — `build_costing_chart`/`push_duty_rates` both complete with 0
    rows and 0 errors, confirming the empty-result path is handled cleanly.
  - **`COSTING_KEY` (the MERGE key Phase 9b writes NT Orbit results back
    onto `costing_chart` with) gained `fabric_content`.** Phase 10 can
    produce MULTIPLE WIP rows per style×color (one "Main Fabric" row + one
    duplicate per "Fabric" segment — see `sync/bom.py`), which otherwise
    share an IDENTICAL key (customer/season/brand/style/color/lineplan_ref/
    supplier_type/supplier/factory) and would ambiguously MERGE-match
    against each other once each carries its own real `Content` value.
    `p9b2_push_duty_to_wip.py`'s WIP-row lookup index was fixed the same
    way: keyed on `(bp_style_number, color_wash, Content)` instead of
    `(bp_style_number, color_wash)` alone — otherwise it would silently push
    one material's duty data onto the WRONG physical WIP row (dict
    last-write-wins on the ambiguous key). `docs/costing_interested_fields.txt`
    updated to match.
  - **Phase 9b's "55" staleness safeguard: rejected timestamp-based check,
    push unconditionally instead.** Originally proposed: compare DTC's
    per-row "modified date" against the NT Orbit cache's `looked_up_at`
    before pushing, skipping if DTC looks newer. Investigation found DTC's
    API only exposes a per-REQUEST `updatedDat` (via `GET /v1/requests/
    {id}`) — confirmed live, individual `sheetData` rows carry NO timestamp
    at all — and that timestamp is touched by every OTHER phase's push too
    (Phase 1/2/7 all write to the same request), making it untrustworthy as
    a "did duty data specifically change" signal and prone to false-positive
    skips within the same job run. Owner decision: since HTS/Duty/Tariff
    values are sourced from `costing_chart` (our own Delta table, not
    independently edited by anyone else in DTC), it's safe to just push
    them every run ("should be a few merge ops") — no staleness check
    needed. The value-diff check already implemented in
    `p9b2_push_duty_to_wip.py` (only PATCH a column if it actually differs
    from the WIP row's current value) is kept as a cheap, non-fragile
    optimization; it isn't a staleness/freshness mechanism, just avoids
    redundant identical PATCHes.
  - **Phase 3 (`phase3_images`) now checks for a sibling row's image before
    doing a full BeProduct extraction.** A style's front image is a
    HEADER-level BeProduct attribute (one per style, not per colorway), so
    every colorway row of the same BP Style# within one request (fixed
    Brand+Season) is expected to carry the SAME image. For a blank-image
    row, `phase3.compute_image_uploads` now first checks whether ANY OTHER
    row with the same BP Style# in this request already has a real Style
    Image — if so, it reuses THAT row's own already-uploaded DTC-hosted
    image URL as the upload source (`ImageUploadOp.source="sibling_copy"`)
    instead of downloading+transcoding from BeProduct's CDN again. Falls
    back to the original full-extraction path (`source=
    "beproduct_extract"`) when no sibling has an image yet. No notebook
    changes needed — the upload mechanics (download-the-URL + classify/
    transcode + multipart POST) are unchanged, only WHICH url is
    downloaded differs. 4 new unit tests (`test_phase3.py` `[15a]`-`[15d]`):
    copy-from-sibling, fallback when no sibling image exists, style-scoped
    isolation (a DIFFERENT style's image is never copied), and
    missing-rowIndex still blocks even a copy upload.
- **Pipeline split into 3 independent Databricks jobs (2026-09-03, owner
  design decision).** Motivated by DTC's known concurrent-edit limitation: a
  browser user's 'save' is silently rejected (or the user's in-progress edit
  is silently lost) if the request's server-side `last_read` timestamp has
  moved past what the browser last loaded — including when this pipeline
  edits the same DTC request while a user has it open (DTC team is working
  on real concurrent-edit support, not yet shipped). Minimizing which jobs
  touch live DTC, and for how long, reduces that contention surface; splitting
  also removes Phase 9b's NT Orbit compute (~30s/call, serial) from blocking
  everything else.
  - **`BeProduct_DTC_sync_dag`** (job `294837488757511`, unchanged job ID) —
    the MAIN job: 00 (Phase 0) → 10 (Phase 1+4+7) → 20 (Phase 2) → 30
    (Phase 10) → 40 (Phase 9a) → 55 (Phase 9b's DTC WIP push only, renamed
    task `push_duty_rates`, notebook `p9b2_push_duty_to_wip.py`). No longer
    contains `gate_phase3`/`phase3_images` (moved out) or the NT Orbit
    compute step (moved out).
  - **`BeProduct_DTC_sync_duty_compute`** (new job `1026599988408090`, "50")
    — single task `compute_duty_rates` running the NEW
    `p9b1_compute_duty_rates.py` notebook (steps 1-4 of the original
    `p9b_fill_duty_rates.py`): NT Orbit lookups → `costing_chart` ONLY.
    Zero DTC dependency of any kind (no API key, no DTC read/write) —
    fully independent, can run on its own cadence without ever touching
    live DTC.
  - **`BeProduct_DTC_sync_images`** (new job `847087837807970`, "60") —
    single task `phase3_images` running the existing
    `p3_beproduct_to_dtc_images.py` unchanged. Confirmed safe to decouple:
    it needs nothing from the SAME main-job run to be correct — it reads
    `dtc_request_mapping`/`beproduct_to_dtc_staging` (left behind by
    whichever main-job run most recently populated them) and does its own
    live `DTCConnector.get_sheet()` read for the freshest rowIndex/Style
    Image state immediately before writing.
  - **`p9b_fill_duty_rates.py` split into two notebooks**:
    `p9b1_compute_duty_rates.py` (steps 1-4, unchanged logic, just the
    DTC-push widgets/imports removed) and `p9b2_push_duty_to_wip.py`
    (rewritten Step 5 — since it's now a genuinely separate job run with no
    shared in-memory state from part 1, it independently re-reads
    `costing_chart` for ANY row with a filled `hts_code`/`duty_rate_*`/
    `tariff_rate`, computes target WIP fields via the unchanged
    `duty.build_wip_patch_fields`, and — new — diffs each target field
    against the WIP row's CURRENT `data_json` value before including it in
    the PATCH, so a `costing_chart` row that already matches its WIP cell
    produces NO API call at all; this avoids re-pushing identical values on
    every run now that this step can no longer rely on "only rows this
    exact run just filled"). Original `p9b_fill_duty_rates.py` kept in
    place as a superseded/manual-fallback artifact (banner added), same
    retirement pattern as Phase 8a's notebook.
  - **Shared Instance Pool** (`INSTANCE_POOL_ID` in `scripts/deploy_job.py`)
    across all 3 jobs: keeps each job's cluster fully independent
    (separate ephemeral job cluster per run, no shared running cluster) but
    cuts cold-start from ~5-7 min to ~1-2 min by drawing from pre-warmed
    pooled VMs — owner explicitly confirmed job independence over sharing
    one literal cluster. `enable_elastic_disk` is REJECTED by Databricks
    when `instance_pool_id` is set (`InvalidParameterValue`, live-discovered
    deploying) — removed from `CLUSTER_EXTRA`.
  - **Node type changed same day**: `Standard_D4s_v3` → `Standard_D4as_v5`
    (owner request). Instance pools are immutable on `node_type_id`, so this
    required deleting the original pool and creating a new one (old pool
    `0903-044952-son1-pool-us7wrezv` deleted, new pool
    `0903-055346-hose1-pool-cia9e7xn` created) rather than an in-place edit.
  - `scripts/deploy_job.py` restructured around a `JOB_SPECS` dict
    (`main`/`duty_compute`/`images`, each with its own `build_*_tasks()`
    function) and a `--job {main,duty_compute,images,all}` CLI flag
    (default `main` for backward compatibility; `all` previews all 3,
    dry-run only). All 3 jobs share the same `JOB_PARAMS` definitions for
    simplicity (each job's tasks only reference the subset they need via
    `P(...)`; unused parameters are harmless).
  - **Live-validated same day**: all 3 jobs deployed and triggered
    independently — `duty_compute` (0 lookups needed, everything already
    cached/filled, 0 errors), `images` (0 new uploads needed, already
    fully imaged, 0 errors), `main` (all 20 tasks `SUCCESS`, including the
    new `push_duty_rates` task correctly diffing against WIP and finding
    nothing to push since `duty_compute`'s prior run had already filled
    everything). One transient issue during testing (deleting the OLD
    instance pool while a main-job run from BEFORE the redeploy was still
    mid-flight caused that one run's `wait_cluster`/`push_duty_rates` tasks
    to fail with `RESOURCE_DOES_NOT_EXIST` on the deleted pool) was a
    self-inflicted testing-methodology artifact, not a real bug — a fresh
    run afterward completed cleanly with 0 errors.
- **Phase 1 was silently reverting Phase 10's Fabric Group/Placement
  enrichment on every scheduled run — live-discovered and fixed
  2026-09-03.** `phase1.FIELD_MAPPING` had `"fabric_group": "Fabric Group"`
  and `"placement": "Placement"` as REGULAR (always-overwrite) entries —
  predating Phase 10, from when `p1p7_beproduct_to_dtc_transform.py`
  hardcoded a `"MAIN MATERIAL CONTENT"` placeholder into every staging row
  (`df_denormalized.withColumn("fabric_group", lit("MAIN MATERIAL
  CONTENT"))`) as a stopgap before any real BOM source existed. This
  violated the repo's own "one field, one direction" rule the moment Phase
  10 started actually writing real values to those same two DTC columns.
  **Live-confirmed real damage**: `beproduct_to_dtc_sync_log` shows a
  scheduled `phase1_push` run at `2026-09-03 00:07:58 UTC` (the 07:55 HKT
  cron trigger) pushed `{"Fabric Group": "MAIN MATERIAL CONTENT"}` onto a
  KTB-00023 WIP row that Phase 10 had correctly enriched to `"Fabric"` in
  the previous day's testing — silently destroying it, confirming this was
  not theoretical but actively happening on the live 3x/day schedule.
  **Fixed** by adding `"Fabric Group"` and `"Placement"` to
  `DEFAULT_FILL_COLS` (same write-once pattern already used for
  `"Supplier"`) rather than removing them from `FIELD_MAPPING` outright —
  this preserves the existing INSERT-time placeholder behavior (still
  needed: `bom.is_unenriched()` treats the placeholder as equivalent to
  blank, so this is not strictly required, but keeps the established
  "MAIN MATERIAL CONTENT" placeholder visible to DTC users on brand-new
  rows rather than silently leaving the cell blank) while ensuring
  `diff_updatable_fields()` NEVER re-pushes either column on UPDATE once
  the DTC cell holds ANY value (placeholder or Phase-10-enriched) — see
  `diff_updatable_fields()`'s existing `DEFAULT_FILL_COLS` skip logic,
  unchanged. Updated: `dtc/python/sync/phase1.py` (`FIELD_MAPPING` comment
  + `DEFAULT_FILL_COLS`), `dtc/tests/test_phase1.py` (new case `[14]`,
  6 assertions), `docs/beproduct_style_interested_fields.txt`, this file's
  "Current direction partition" section. The corrupted live KTB-00023 WIP
  row was manually restored (`Fabric Group` set back to `"Fabric"`) so
  Phase 10's upsert logic doesn't misinterpret it as a fresh placeholder row
  on its next run (which would have inserted yet another duplicate).
- **Phase 10 source table changed to `customer_teckpack_style_latest`, and
  enrichment logic redesigned to UPSERT semantics (owner spec, 2026-09-03).**
  `customer_teckpack_style_log` required this notebook to dedupe multiple
  versions itself (`current_version` DESC / `timestamp_lf_captured`
  tie-break); `customer_teckpack_style_latest` pre-resolves that, guaranteeing
  at most one row per (`style_no`, `customer_name`, `customer_department`,
  `style_season`) — live-confirmed 0 duplicate groups for `customer_name=
  'KONTOOR'` on that full 4-column key (2026-09-03). `customer_department`
  IS part of the real uniqueness key (a constant, non-null value for KONTOOR,
  `"Wrangler Collaborations"`, but genuinely varies for other customers in
  this shared table) — a defensive `row_number()` dedup keyed on it plus
  style_no/style_season is kept as a near-zero-cost safety net even though
  it's a no-op for KONTOOR today. Separately, the enrichment decision logic
  was redesigned from the original all-or-nothing `style_already_enriched`
  gate (a single real value on ANY row short-circuited the WHOLE style to a
  permanent no-op) to genuine per-row UPSERT semantics: the match key
  between a BOM segment and an existing WIP row is (Fabric Group, Mill
  Fabric Article #) together (`Placement` excluded from the key since it's
  the one field expected to still legitimately drift for an otherwise-
  unchanged assignment); a matching row gets `Placement` upserted (only if
  changed); an un-enriched row gets Main Fabric's full field set (first-time
  enrichment, unchanged). A row holding
  some OTHER real, unrecognized combination (a "Fabric" segment that's since
  disappeared from the techpack, or hand-edited DTC data) is left
  COMPLETELY UNTOUCHED; a style with no "Main Fabric" segment at all this
  run (BOM missing entirely, or Main Fabric itself vanished) takes ZERO
  actions for the whole style. This last rule has a REAL, live-confirmed
  trigger: switching to `customer_teckpack_style_latest` left `bom_unified`
  NULL for two previously-BOM-bearing KONTOOR test styles (`KTB-00016`,
  `KTB-00021`) that had already been correctly enriched by an earlier run —
  without this rule, the table switch alone would have silently reverted
  their already-correct DTC data on the next scheduled run. Implementation:
  `sync/bom.py` gained `segment_key()`, `is_unenriched()`, and
  `build_target_segments()`; `style_already_enriched`/`RowEnrichmentPlan`/
  `plan_row_enrichment` were removed (superseded); `plan_style_enrichment()`
  was rewritten around the new per-row match logic (same `RowAction`
  output contract, so `p10_pull_bom_and_enrich.py`'s Step 4 PATCH/INSERT
  push code is unaffected). `dtc/tests/test_bom.py` cases `[4]`-`[6j]`
  rewritten for the new semantics (11 new/changed cases, including the
  never-revert/never-re-insert/idempotent-no-op scenarios). Step 2 of the
  notebook now also extracts each WIP row's current `Mill Fabric Article #`
  / `Placement` values (previously only `Fabric Group`), required for the
  new match key.
- **Phase 10 DAG-level EXCLUDED cascade broke Phase 9a/9b on every scheduled
  run (live-discovered and fixed 2026-09-02).** `fill_bom_data` was
  originally gated by a `gate_phase10` condition task
  (`dep("gate_phase10", outcome="true")`), mirroring every other phase's
  pattern. But with `run_phase10` defaulting to `false`, this made
  `fill_bom_data` become `EXCLUDED` (not merely `SKIPPED`) — and Databricks
  propagates `EXCLUDED` to every downstream dependent UNCONDITIONALLY,
  ignoring `run_if` entirely (`run_if=ALL_DONE` only tolerates a dependency
  that actually ran and then skipped/failed, NOT one excluded via an
  untaken condition branch). Since `repull_dtc_bom` → `build_costing_chart`
  → `gate_phase9b` → `fill_duty_rates` all transitively depended on
  `fill_bom_data`, this silently excluded the ENTIRE Phase 9a/9b chain on
  every scheduled run (confirmed live: the 2026-09-02 15:57 HKT periodic
  run showed `build_costing_chart`/`fill_duty_rates` etc. all `EXCLUDED`).
  **Fixed** by removing `gate_phase10` from the DAG entirely — `fill_bom_data`
  now always runs (originally `depends=[dep("pull_master_dtc")]`, no
  condition; changed again 2026-09-02, see next entry), and checks
  `run_phase10` INSIDE the notebook instead (matching the existing
  `dry_run` pattern elsewhere), calling `dbutils.notebook.exit(...)`
  immediately as a genuine SUCCESS no-op when disabled — so nothing
  downstream is ever excluded. Live-reverified after the fix: `fill_bom_data`
  → `repull_dtc_bom` → `build_costing_chart` all completed `SUCCESS` with
  `run_phase10=false` (the exit value confirms the no-op path was taken:
  `"SKIPPED_run_phase10_false"`).
- **`fill_bom_data` re-pointed from `pull_master_dtc` to `repull_dtc`, and
  `gate_phase1` removed the same way as `gate_phase10` (2026-09-02, owner
  clarification of intended lineage).** Owner clarified the intended data
  flow: Phase 1 completes the style×color WIP master chart first (image
  upload, Phase 3, is optional and not a blocker); Phase 10 then reads that
  COMPLETE style×color state to fill in the material dimension
  (style×color×material); Phase 9 then builds `costing_chart` from that.
  `fill_bom_data` previously depended on `pull_master_dtc` — the WIP
  snapshot taken BEFORE `phase1_push` runs — so it could enrich a style×
  color state missing rows `phase1_push` had just created in DTC this same
  run. Fixed: `fill_bom_data` now depends on `repull_dtc` instead (still
  `run_if=ALL_DONE`), the task that already exists specifically to reflect
  `phase1_push`'s newly-created rows back into Delta. This exposed a second,
  identical-class landmine: `repull_dtc` was gated by
  `dep("gate_phase3", outcome="true")`, so `run_phase3=false` would have
  `EXCLUDED`-cascaded through `repull_dtc` into `fill_bom_data` and the
  entire Phase 9/10 chain behind it — the same bug as the `gate_phase10`
  entry above, reintroduced through `gate_phase3`. Fixed by making
  `repull_dtc` an unconditional prerequisite (`depends=[dep("phase1_push")],
  run_if=ALL_DONE`, no gate) shared by both `phase3_images` and
  `fill_bom_data`; the `run_phase3` check moved onto `phase3_images` itself
  (`depends=[dep("gate_phase3", outcome="true"), dep("repull_dtc")]`). This
  in turn exposed a THIRD instance of the same class: `phase1_push` itself
  was gated by `gate_phase1`, and with the whole Phase 3/9/10 chain now
  transitively depending on it via `repull_dtc`, `run_phase1=false` would
  have `EXCLUDED`-cascaded the same way. Fixed identically — `gate_phase1`
  removed from the DAG, `phase1_push` (`beproduct/p1p7_beproduct_to_dtc_push.py`)
  now takes a `run_phase1` widget (default `"true"`) checked INSIDE the
  notebook, `dbutils.notebook.exit("SKIPPED_run_phase1_false")` as a no-op
  (also explicitly zeroing its `taskValues` `inserted_ids`/`inserts` outputs
  so `repull_dtc`'s `{{tasks.phase1_push.values.inserted_ids}}` reference
  still resolves cleanly). `phase3_images` and `fill_duty_rates` (Phase 9b's
  WIP PATCH) remain safe to run in parallel off this shared prerequisite:
  they write through disjoint DTC surfaces (binary `/images` endpoint keyed
  by `rowindex` vs. JSON `sheetData` PATCH keyed by `rowId`, disjoint
  columns), and `phase3_images` re-reads the live sheet itself immediately
  before writing rather than trusting a Delta snapshot. `python
  scripts/deploy_job.py --dry-run` re-verified after the fix: 22 tasks, no
  `gate_phase1`/`gate_phase10`, `phase1_push <- request_manager`,
  `repull_dtc <- phase1_push`, `phase3_images <- gate_phase3[true],
  repull_dtc`, `fill_bom_data <- repull_dtc`. **Deployed live** the same day
  (`python scripts/upload_notebooks.py` + `python scripts/deploy_job.py
  --reset-existing 294837488757511`) and triggered with
  `run_phase10=true, dry_run=false` for a real end-to-end validation — see
  next entry for what that run uncovered.
- **Phase 10 INSERT payload copied non-writable DTC columns (Style Image +
  formula fields), causing 100% of live INSERTs to fail (live-discovered
  and fixed 2026-09-02, same deployment as the entry above).** The
  triggered live run (`run_phase10=true, dry_run=false`) surfaced a
  previously-undetected bug (dry-run mode never actually calls the DTC
  PATCH endpoint, so this could only ever be caught by a real push): all 17
  UPDATEs succeeded but all 4 INSERTs failed with HTTP 400 `"'Style Image'
  is an image field and cannot have data added to it"`. Root cause:
  `p10_pull_bom_and_enrich.py`'s INSERT path (duplicating a row for each
  "Fabric" segment) copied the ENTIRE original row's fields from
  `data_json` forward into the new row, including "Style Image" — DTC's
  sheetData PATCH/INSERT endpoint rejects ANY write to an image-type column
  outright, even a mere copy-forward of an existing value (images can ONLY
  be set via Phase 3's separate multipart `/images` endpoint). Excluding
  just `"Style Image"` and re-testing (via a direct, isolated replay of the
  exact failing payload against the live sheet, using
  `dtc.get_max_row_index`/`dtc.patch_rows` the same way the notebook does)
  immediately hit a SECOND, different-shaped 400: `"'Fabric Article' is a
  formula field and cannot have data added to it"` — a computed/derived
  column, not an image one. Checking DTC's own `isReadOnly` flag on both
  fields (`GET /v1/views/{id}` dynamicFields) found it `false` on BOTH —
  **`isReadOnly` is not a reliable signal for this at all**. Live-scanning
  all 204 fields on the KTB `WIP_ITS_USE` view found the two signals that
  DO reliably predict a rejection: `type == "contact"` (exactly one field,
  "Style Image") and a truthy `formula` key (6 fields: "Fabric Article",
  "Fabric Mill", and 4 "`<app>` - Target Sample Ready Date" fields).
  **Fixed** generally, not just for these two named fields:
  `sync/bom.py` gained `compute_non_writable_cols(dynamic_fields)` (derives
  the exclusion set from a view's own `dynamicFields` metadata using the
  `type`/`formula` signals) and `build_insert_row_payload(base_fields,
  wip_fields, exclude_cols=...)` (the actual row-copy-plus-override logic,
  now unit-tested); `p10_pull_bom_and_enrich.py` calls
  `dtc.get_view_definition(view_id)` once per distinct `view_id` (cached),
  falling back to the static `INSERT_EXCLUDE_COLS` (rowId/rowIndex/Style
  Image) alone if that call fails (live-observed once, a transient 403 from
  the Azure Application Gateway fronting the DTC API — the fallback still
  produced 0 errors for this run's specific payloads). **Re-validated live**
  after the fix: the 2 styles that actually carry a "Fabric" BOM segment
  (`KTB-00020`, `KTB-00023` — confirmed directly against
  `alb_tpm_uat.public.customer_teckpack_style_log`; the OTHER 6 matched
  KONTOOR test styles have Main Fabric only, no "Fabric" segment, so were
  never going to exercise the INSERT path at all) were reset back to the
  `MAIN MATERIAL CONTENT` placeholder (undoing just enough of the prior
  run's partial UPDATE so `style_already_enriched()` would treat them as
  un-enriched again — `style_already_enriched()` short-circuits a whole
  style to a no-op the moment ANY of its rows carries real Fabric Group
  data, so a partially-failed run cannot be silently "completed" by a
  plain re-run without this reset) and the job re-triggered end-to-end:
  `fill_bom_data` reported `Pushed updates: 4  Pushed inserts: 4  errors:
  0`, and the resulting `dtc_wip_ktb` rows confirm both new "Fabric" rows
  landed correctly (`KTB-00020`/`KTB-00023` each now appear twice — once
  `Fabric Group="Main Fabric"`, once `Fabric Group="Fabric"` — across both
  physical KTB WIP requests in this test data). A stray manually-inserted
  validation row (used to isolate-reproduce the original failure before the
  fix, with placeholder test values) was deleted from the live UAT sheet
  afterward via `DTCConnector.delete_rows` so no test artifacts were left
  behind. New tests: `dtc/tests/test_bom.py` cases `[7]`/`[8]`.
- **Phase 10 BOM enrichment: "Body" → "Fabric" and `material_name` →
  `bom_detail_name` correction (owner amendment, 2026-09-02).** Initial spec
  used `bom_detail_name` values "Main Fabric"/"Body" and read `Fabric Group`
  from the segment's `material_name`. Live data check across all 16 KONTOOR
  rows found "Body" NEVER appears at all, but "Fabric" genuinely does (3/16
  styles, e.g. KTB-00020/KTB-00023/CB-S28003/s234160). Corrected: the two
  interesting segments are "Main Fabric" (exactly 1 by construction) and
  "Fabric" (0 or more, not just 0 or 1 — `dtc/python/sync/bom.py`'s
  `ParsedBomSegments.fabric_list` handles any count), and `Fabric Group` is
  now assigned from the segment's own `bom_detail_name` (i.e. literally
  "Main Fabric" or "Fabric"), NOT `material_name`. `Placement`/`Mill Fabric
  Article #` are unaffected (`placement`/`material_no`, unchanged). Live
  dry-run re-validated after the fix: 8/8 matched styles, 17 updates + 4
  inserts (matching exactly the 4 styles confirmed to have a "Fabric"
  segment), 0 errors.
- **Phase 10 `alb_tpm_*` requires SERVERLESS compute (live-discovered
  2026-09-02).** `alb_tpm_uat`/`alb_tpm_prd` are Lakebase databases
  registered in Unity Catalog, not plain Delta-backed catalogs — the
  classic shared job cluster (`Standard_D4s_v3`, used by every other task)
  fails with `UnauthorizedAccessException: ... requires serverless
  compute` on any `spark.table("alb_tpm_uat...")` call. This is why an
  earlier local SQL-warehouse validation of the same query succeeded (that
  warehouse happens to be serverless) while the actual job task failed.
  Fixed by adding a `serverless` flag to `scripts/deploy_job.py`'s
  `nb_task()` helper (omits `job_cluster_key` entirely, which Databricks
  Jobs then runs on serverless compute) and setting it for `fill_bom_data`
  only — every other task remains on the shared classic cluster. Serverless
  compute can still read ordinary Unity Catalog Delta tables fine (the
  constraint is one-directional: Lakebase needs serverless, but serverless
  isn't restricted from anything classic compute can already do), so this
  required no other code changes.
- **Phase 9b WIP push: "Duplicate rowId found" 400 (live-fixed 2026-09-01).**
  `costing_chart` transposes ONE WIP row into up to 4 rows (Main/1/2/3 vendor
  slots) — all 4 map to the SAME underlying WIP `rowId`, differing only in
  which columns they target (e.g. `"Main Factory HTS Code"` vs `"Factory 1 -
  HTS code"`). Step 5's push previously appended one `sheetData` object PER
  costing_chart row, so a style with multiple filled slots produced multiple
  objects sharing the same `rowId` in one PATCH call — DTC's API rejects
  this outright (`400 "Duplicate rowId found."`). Fixed by merging all
  slots' patch fields into a single dict PER ACTUAL WIP ROW (keyed on
  `(sheet_id, view_id, row_id)`) before batching into `sheetData` — safe
  because each slot's `build_wip_patch_fields()` output always targets
  disjoint column names, so `dict.update()` across slots never collides.
- **Phase 9b cache staleness check: naive-vs-aware datetime crash (live-fixed
  2026-09-01).** `duty.is_cache_entry_stale()` did `now - looked_up_at`
  directly; Spark's TIMESTAMP columns come back as NAIVE `datetime` objects
  via `.asDict()`/`collect()`, while the notebook's `now =
  datetime.now(timezone.utc)` is AWARE — mixing the two raises `TypeError:
  can't subtract offset-naive and offset-aware datetimes`. Fixed with a
  `_as_naive_utc()` normalization helper (strips tzinfo, converting to UTC
  first if aware) applied to both sides before subtracting, so the
  comparison works regardless of which side (if either) happens to carry
  tzinfo. Unit-tested in `dtc/tests/test_duty.py` with the exact
  naive/aware combinations that crashed in production.
- **WIP↔LinePlan join changed to INNER (2026-09-01), owner decision.**
  `p9a_build_costing_chart.py` previously used a LEFT JOIN on "Lineplan Ref
  #", so a WIP row with a blank/unmatched ref# still surfaced in
  `costing_chart` with null `order_quantity`/`target_ldp`/`target_fob`.
  Owner confirmed all live WIP styles now have `BP Style#` populated
  (verified: 100% for every non-`(BACKUP)`-named WIP request; the
  `(BACKUP)`-named requests are a separate, pre-existing pollution source —
  199 of 227 `dtc_wip_ktb` rows with null `bp_style_number` all trace to
  legacy `(BACKUP)` requests, not real production data) and that
  `costing_chart` should only ever contain rows with a REAL LinePlan match —
  changed to an INNER JOIN; unmatched WIP rows are now dropped entirely
  rather than surfacing with null LinePlan fields.
  **Live-verified same day**: of the 8 WIP rows with a real Lineplan Ref#
  (`WC-S8001`..`WC-S8008`, all in the non-backup `"KTB SS28 Wrangler
  Collaborations"` request, all matching 1:1 to `dtc_lineplan_ktb`), only 2
  (`WC-S8001` with 4 populated vendor slots, `WC-S8002` with 1) have ANY
  vendor assigned yet — the pre-existing, independent per-slot
  vendor-presence filter (spec: "transpose ... into 4 rows", blank-vendor
  slots dropped) still applies after the join and correctly produces
  **5 rows total** (4 + 1), not 8 — confirmed by the owner as correct,
  not a bug. `WC-S8003`..`WC-S8008` have zero vendor slots assigned in
  WIP/BeProduct and correctly produce zero costing rows each.
- **LinePlan request naming — no filter, human-enforced uniqueness (project
  team decision, 2026-09-01).** The project team will maintain MULTIPLE DTC
  LinePlan requests with NO specific naming convention for now (this
  includes `(BACKUP)`-named requests — unlike Phase 0/XTS Master, which
  explicitly excludes those). `p9a_pull_lineplan_to_delta.py` therefore has
  and keeps NO name-pattern filter; it pulls every active request in the
  document regardless of name. Uniqueness of `"Lineplan Ref #"` ACROSS ALL
  requests is a HUMAN-enforced invariant (human-in-the-loop), not something
  this pipeline validates or enforces. `p9a_build_costing_chart.py`'s
  LinePlan aggregation step (`F.first(ignorenulls=True)` per ref) adds a
  best-effort conflict DETECTOR that prints a warning (does not block) when
  a ref's `order_quantity`/`target_ldp`/`target_fob` actually disagree
  across rows/requests, so violations of the human-enforced invariant are at
  least surfaced rather than silently resolved to an arbitrary value.
- **`costing_chart` is a FULL OVERWRITE every `build_costing_chart` run, not
  an upsert (clarified 2026-09-01).** If a style initially has only "Main"
  factory assigned and a later run adds Factory 2/3, the later run does NOT
  duplicate the Main row — the entire table is rebuilt from current WIP +
  LinePlan state on every run via `.mode("overwrite")`. Manually truncating
  `costing_chart` before a run is therefore redundant (the overwrite already
  clears prior content) but harmless. This is also why Phase 9b's
  `nt_orbit_duty_cache` must be a SEPARATE, never-overwritten table (see
  Phase 9b entry above) — any HTS/Duty/Tariff values Phase 9b fills into
  `costing_chart` itself are wiped by the next Phase 9a rebuild.
- **`factory_slot` renamed to `supplier_type` in `costing_chart` — RESOLVED
  (2026-09-01), traced back to the original spec.** Initial implementation
  wrongly mapped `costing_chart.supplier_type` to LinePlan's
  `"INTERNAL/ SOURCED"` field. Checking `implement_prompts.txt`'s original
  Phase 9a spec: `"Supplier Type - Generated from Master Chart data"` — i.e.
  ONE flag, GENERATED from WIP structure (which of the 4 vendor/factory
  column-pairs a row came from — `"Main"|"1"|"2"|"3"`), NOT the LinePlan
  business classification. Fixed: `p9a_build_costing_chart.py`'s `factory_slot`
  column is renamed to `supplier_type` (same values, same derivation — no
  functional change to Phase 9b's WIP-push routing, which still keys on this
  same value, just reads it from the renamed column); LinePlan's
  `"INTERNAL/ SOURCED"` is still captured into `dtc_lineplan_ktb.internal_sourced`
  (harmless raw data) but is no longer joined into `costing_chart` at all.
  `dtc/python/sync/duty.py`'s `build_wip_patch_fields()` / `WIP_HTS_COL` /
  `WIP_DUTY_COL` are UNCHANGED (they only ever took a slot-value string
  positionally, independent of the caller's column name) — only the caller
  in `p9b_fill_duty_rates.py` and `COSTING_KEY` were updated to read
  `supplier_type` instead of `factory_slot`.
- **Phase 8a/8b retired (2026-09-01), confirmed by the project team.** DTC
  FABRIC → Delta → BeProduct Material Master is superseded by a separate
  "MaterialLib" application. Removed from `scripts/deploy_job.py`'s DAG
  entirely — the `gate_phase8a` condition task, the `pull_fabric_dtc` task,
  and the `run_phase8a` / `include_test_sheets` / `fabric_document` job
  parameters are all gone from the deployed job (`BeProduct_DTC_sync_dag`,
  reset 2026-09-01: task count 23 → 21). Verified nothing else in the DAG
  depended on `pull_fabric_dtc`/`gate_phase8a`, so removal was a clean,
  self-contained deletion — Phase 9a/9b and the WIP chain were unaffected.
  Left in place (NOT deleted) as historical/manual-fallback artifacts:
  `dtc/notebooks/p8a_pull_fabric_to_delta.py`, and the SSOT doc
  `docs/beproduct_material_interested_fields.txt` (marked superseded).
  Docs updated: `README.md`, `docs/ARCHITECTURE.md`, `docs/DTC_GUIDE.md`,
  `docs/DIAGRAM.md`, `QUICK_START.md`.
- **`lft.beproduct` table cleanup (2026-09-01), owner-confirmed.** Live-schema
  audit (via `databricks-sql-connector`, not just code grep) found 6 drop
  candidates beyond the already-known Phase 8a tables; owner triaged each:
  - **Dropped**: `dtc_fabric_ktb` (0 rows), `dtc_fabric_registry` (81 rows —
    Phase 8a outputs, retired same day, see above), `ktb_styles_push_log`
    (5 rows, last written 2026-05-26, zero code references anywhere —
    superseded by `beproduct_to_dtc_sync_log`, which is what's actually
    written today).
  - **Kept, NOT dropped**: `wmt_styles` / `wmt_styles_sync_meta` (75 + 21
    rows, last written 2026-06-17, zero current code references) — owner
    confirmed these are an intentional artifact demonstrating the pipeline
    generalizes beyond `KTB` to another customer folder (`WMT`); leave as-is,
    do not treat as orphaned/stale in future audits. `costing_chart_kei`
    (job param `costing_chart_table` test override) — owner's own active
    Phase 9b DAG-testing table; **`costing_chart` itself must stay stable**
    since it has real downstream readers, which is exactly why the test
    override table exists — keep both.
  - Already gone before this audit (per the 2026-06-17 decision above,
    confirmed still absent): `dtc_master_chart_uat`,
    `dtc_master_chart_uat_change_log`. Confirmed never created:
    `beproduct_directory_contacts` (0 contacts org-wide, `fetch_contacts`
    defaults `false`).
- **NT Orbit / Entra token endpoint AADSTS700025 (live-validated 2026-08-31).**
  Registering `http://localhost:8765/callback` under the app's "Mobile and
  desktop applications" platform in the Entra portal makes Entra treat EVERY
  token request using that redirect_uri as a public client — it rejects
  `client_secret` with `AADSTS700025` ("Client is public so neither
  'client_assertion' nor 'client_secret' should be presented"), even though
  the app registration (`c486611e-9bfc-49d5-8930-7d1943884b03`) has a secret
  configured. This is determined by the redirect_uri's PLATFORM TYPE, not by
  whether the app has a secret. Fix: register the redirect_uri under "Web"
  instead. `entra_auth.exchange_code_for_tokens`/`refresh_access_token` also
  auto-detect this specific error (`error_codes` contains `700025`) and
  retry once without the secret, so a misconfigured platform type degrades
  to a public-client exchange instead of hard-failing the login (unit-tested
  in `dtc/tests/test_entra_auth.py` with a mocked 401 response).
  **Resolved 2026-08-31**: rather than fix the platform type on
  `c486611e-...`'s existing redirect_uri, a fresh dedicated app registration
  (client_id `d270069e-20cc-4e63-ba38-156fb0ee9296`) was created with
  `http://localhost:8765/callback` registered correctly under "Web" from the
  start. `--flow authcode` against it completed the one-time login
  successfully (health check confirmed `auchunkei@lifung.com` is authorized
  against NT Orbit). `c486611e-...` is abandoned/unused going forward —
  `d270069e-...` is the live Phase 9b client_id.
- **`create_sheet` payload fix (2026-06-17): `requestName` → `requestReference`.**
  The original `POST /v1/sheets` payload used the key `requestName`; the DTC API
  uses `requestReference` everywhere (GET responses, search filters, `get_request`
  return values). Sending the wrong key caused 400 on all create attempts. Fixed in
  `DTCConnector.create_sheet` (`dtc/python/connectors/dtc.py`). Also added
  `RestClient._log_error_body` which logs the API response body on any HTTP error so
  future 4xx failures are self-diagnosable without a debugger.
  Status: code fix applied; UAT validation still required.
- **Phase 3 image sync is a separate post-Phase-1 step (2026-06-17).** Style Image
  is binary and cannot ride the Phase 1 JSON sheetData PATCH, and its endpoint
  targets an EXISTING row by `rowindex`, so it cannot run at insert time. New
  notebook `beproduct/p3_beproduct_to_dtc_images.py` runs AFTER `p1p7_beproduct_to_dtc_push`:
  it re-reads each in-scope sheet live (freshest rowIndex + Style Image state),
  and for every row whose cell is blank AND whose BeProduct staging row has a
  valid `front_image_url`, downloads the CDN image and POSTs it to the multipart
  images endpoint (`columnname="Style Image"`). Pure decision logic in
  `dtc/python/sync/phase3.py` (`compute_image_uploads`, unit-tested in
  `test_phase3.py`); idempotent (already-imaged rows skipped). The orchestrator
  (`orchestrate_sync.py`) runs it as Step 8, gated by `run_phase3`, preceded by a
  Step 7 re-pull of `dtc_wip_<customer>` so the table reflects Phase 1 inserts.
  **Validated live 2026-06-17** (UAT full run): 41 uploads succeeded; open
  follow-ups are DTC rejecting `webp` (400) and a subset of BeProduct CDN URLs
  failing download with 403 (SAS auth) — both data/CDN-side, not core-logic bugs.
  webp is now auto-transcoded to PNG (`phase3.classify_image_type` + Pillow).
- **Phase 1 now CREATES missing in-scope DTC requests (2026-06-17, reverses prior
  scope).** The earlier rule "Phase 1 does NOT create requests; the project team
  pre-creates them" is superseded. `p1_dtc_request_manager` creates missing **in-scope**
  requests (`connector.create_sheet` → `POST /v1/sheets`) in `dtc_document`, then
  re-scans + resolves. Guardrails: only in-scope names (`<customer> <seasonCode>
  <brand>`; brand-less names → `NOT_IN_SCOPE`, never created); gated by `dry_run`
  (default true = preview only). The registry scan is the shared
  `sync.registry.refresh` (discover → enrich → merge), invoked automatically by
  `p1_pull_masters_to_delta` and `p1_dtc_request_manager` (default `refresh_registry=true`)
  and standalone by `00_init_request_registry`. NOTE: `create_sheet` is a live,
  not-easily-reversible write; there is no delete-request in the connector.
  **`POST /v1/sheets` body shape VALIDATED LIVE 2026-06-18 (HTTP 201)** after the
  first prod attempt 400'd on all 4 requests. The correct body is
  `{workspaceName, documentName, requestReference (NOT requestName),
  requestDescription (MUST be non-empty), viewName,
  requestAssigneeSharingViewNames:[], sheetData:[]}`. Omitting the two arrays
  crashes the server with 400 "Cannot read properties of undefined (reading
  'map')"; empty arrays are accepted. Optional: `requestStatusName`
  (e.g. "Factory Allocation"), `requestAssigneeEmail`. Success response nests ids
  under `data` with a CAPITAL-S `SheetId`: `{"data":{"requestId":..,"SheetId":..}}`
  — `create_sheet` now normalises this to flat `{requestId, sheetId, raw}`.
- **Request sharing VALIDATED LIVE 2026-06-18 (HTTP 201).** A freshly created
  request grants FULL rights to its CREATOR (the API identity) ONLY; the data is
  invisible to the team until SHARED. `POST /v1/requests/{requestId}/shares/{userEmail}`
  and `POST /v1/requests/{requestId}/shares/usergroups/{userGroupName}` (path
  segments URL-encoded — group names have spaces), body
  `{"viewNames":[...],"message":"...","sendEmail":"Y"|"N"}` → 201 (empty body).
  Read current shares via `GET .../shares` and `.../shares/usergroups` (used for
  idempotency). Connector: `share_request_with_user`, `share_request_with_usergroup`,
  `get_request_shares`, `get_request_share_usergroups`. Project policy: share ALL
  views with `aiagentwip@lifung.com` (AI Agent WIP) and the **Full Version** view
  with the **Kontoor Project Team** user group (**changed 2026-08-27**, was
  "Fabric Group" — do not confuse with the unrelated DTC data column also named
  "Fabric Group" in `phase1.FIELD_MAPPING`). Applied automatically by
  `p1_dtc_request_manager` at create time (`share_on_create=true`, `send_email=N`)
  and backfillable via the idempotent `beproduct/p1utl_dtc_share_requests` notebook.
- **Legacy change-tracking pipeline removed (2026-06-17).** The old single-table
  `dtc_master_chart_uat` snapshot/change-log flow (`pull_dtc_to_delta`,
  `01_create_sync_tables`, `02_create_snapshot`, `03_detect_changes`,
  `04_push_changes`, `sync/snapshot.py`, `sync/change_detection.py`,
  `CHANGE_TRACKING_DESIGN.md`) was deleted from the repo and the Databricks
  workspace. Current model: registry-driven pull of the `WIP_ITS_USE` view into
  `dtc_wip_<customer>` + Phase 1/2. The season mapping is forward-only (Phase 2
  doesn't reverse-map season). `.kilo/skill/dtc-integration/SKILL.md` still contains
  disclaimed legacy examples behind a "superseded" banner.
- Moved-key orphan (style's key changed → row belongs to a different request): mark the
  stale DTC row `Product Status = "(removed)"` (invalid BeProduct value, user signal);
  do not delete. Only mark keys that moved to another request.
- `Main Factory Customer ID` has no BeProduct target → skipped + logged until a
  fieldId is provided. Live schema check (2026-06-18): `factory_id_no` is a
  read-only LabelText formula (`[factory]`); `customer_supplier_id` is a DropDown
  with only 2 predefined choices — neither is a suitable writable target.
  **RESOLVED 2026-09-03**: wired up to `customer_factory_code` ("Customer
  Factory Code") instead — a real, writable header field found live; see
  decisions log for the full write-then-revert validation.
- DTC blanks do not clear BeProduct unless `push_blanks=true`.
- `customer_style_number` / DTC "Legacy Code" direction: **CHANGED in Phase 6
  (2026-06-26)** to **BeProduct → DTC** (optional; populated from BP's
  `customer_style_number` when non-blank). Previously "Legacy Code" was DTC→BP;
  that role now belongs to the new DTC column **"Customer Style#"**. There is no
  loop: "Legacy Code" (BP→DTC) and "Customer Style#" (DTC→BP) are different DTC
  columns. **Note:** There is NO new BeProduct "Legacy Code" field — the existing
  `customer_style_number` (CUSTOMER STYLE NUMBER / PLM #) is the source. Both
  `lf_style_number` and `customer_style_number` will be populated by upstream
  integration before flowing to DTC. **DTC "Customer Style#" column is NOT being
  created (decision 2026-07-02)**. Removed from `phase2.REVERSE_HEADER_FIELDS`.
- BeProduct `header_number` for style `127-WM2FF-K26` has a leading space
  (`' 127-WM2FF-K26'`). `phase1.norm()` strips it for matching so the sync
  functions correctly; the cosmetic issue should be fixed in BeProduct directly.

## Pipeline performance (validated 2026-06-19)

Full history in `docs/PERFORMANCE.md`. Key facts for agents:

**Current production jobs (split into 3, 2026-09-03 — see decisions log):**
`BeProduct_DTC_sync_dag` (job 294837488757511, MAIN — 00/10/20/30/40/55),
`BeProduct_DTC_sync_duty_compute` (job 1026599988408090, "50" — NT Orbit
compute only, no DTC dependency), `BeProduct_DTC_sync_images` (job
847087837807970, "60" — Phase 3 image upload only). Each has its own
single-node non-Photon cluster, drawing from a SHARED Instance Pool
(`Standard_D4as_v5`) for fast warm-up while remaining fully independent
(separate schedules, separate cluster instances). Defined in
`scripts/deploy_job.py`; deploy a specific job with
`python scripts/deploy_job.py --job {main,duty_compute,images}`. The old
single-notebook orchestrator (job 22324120218492, `orchestrate_sync.py`) is
retired.

**Parallel DAG:** Steps 1→2 (BeProduct chain) run in parallel with Step 3 (DTC
pull), converging at Step 4. Steps 1-2 ≈ 110 s; Step 3 ≈ 84 s — they overlap.

**Current typical execution (single-node cold start ~290 s):**

| Step | Task | Typical exec |
|------|------|-------------|
| 1 | `bp_style_sync` (BeProduct API → ktb_styles) | ~75–130 s |
| 2 | `transform` (ktb_styles → staging) | ~30–50 s |
| **3** | **`pull_dtc`** (DTC API → dtc_wip + registry) | **~84 s** |
| 4 | `request_manager` | ~13 s |
| 5 | `phase1_push` (BeProduct → DTC upsert) | ~14–60 s |
| 6 | `phase2_push` (DTC → BeProduct) | ~20–30 s |
| 7 | `repull_dtc` (targeted: only INSERT'd requests) | ~17 s full / ~5 s targeted |
| 8 | `phase3_images` (front image upload) | ~25–130 s |
| **Total execution** | | **~320–450 s** |
| **Total wall (cold)** | | **~610–740 s (~10–12 min)** |

**What was actually slow and why (intra-step cell-level profiling, 2026-06-19):**
The original hypothesis (serial `get_sheet` HTTP calls = 396 s) was wrong. The real
costs in `p1_pull_masters_to_delta` were 100% Spark overhead:
- Cell 5: 66 per-request DataFrames → `reduce(unionByName)` → Delta overwrite +
  redundant `count()` = **277 s** for 422 rows. Fixed: one flat list → one DF → one
  write → `len()`. Now **8.8 s**.
- Cell 6: 66 serial `spark.sql("UPDATE registry …")` = **179 s**. Fixed: single
  batched `MERGE INTO` from a temp view. Now **5.5–8 s**.
- DTC API (registry refresh + all `get_sheet`s combined) = **~24 s** and was never
  the bottleneck. Opt A (parallel `get_sheet`) therefore gave nothing.

**Per-step log access (no hidden child runs):** Because the pipeline is now a
multi-task job, each step's logs are at `runs/get → tasks[].run_id`. Export any
step directly with `runs/export?run_id=<task_run_id>`. No WORKFLOW_RUN hunting.

**INCREMENTAL mode upsert bug fixed (2026-06-18):** `p1p7_beproduct_style_sync.py`
was using `mode="append"` for INCREMENTAL writes, causing duplicates when the
BeProduct `FolderModifiedAt` filter (folder-scoped, not style-scoped) returned
styles that were already in `ktb_styles`. Fixed to `DeltaTable.merge` (keyed on
BeProduct style `id`) so INCREMENTAL correctly upserts — matched rows UPDATE,
new rows INSERT, unrelated rows are untouched. FULL mode remains `overwrite`.
NOTE: the `FolderModifiedAt` filter is folder-scoped (any change in the KTB folder
re-qualifies all styles), so INCREMENTAL is NOT reliably faster than FULL for Step 1
— leave `refresh_mode=INCREMENTAL` (default) but do not expect it to save time.

**BeProduct SDK install (~10 s/run):** `p1p7_beproduct_style_sync.py` installs the SDK
via `subprocess.check_call(pip install)` on every run. Isolated in its own cell so
the cost is measured. To eliminate, bake `beproduct` into the cluster init script.

**Remaining opportunity:** parallelize `registry.refresh()` inner loop
(`registry.py:308`, serial `get_request_scope` by-id reads) — now ~40 s, the last
remaining lever in Step 3. Same `ThreadPoolExecutor(max_workers=4)` pattern.
Pre-warmed cluster pool also eliminates the ~290 s cold start.

## Commands

```bash
python3 dtc/tests/test_phase1.py        # Phase 1 core unit tests
python3 dtc/tests/test_phase2.py        # Phase 2 core unit tests
python3 dtc/tests/test_phase3.py        # Phase 3 image-upload core unit tests
python3 dtc/tests/test_samples.py       # Phase 7 sample formatter unit tests
python3 dtc/tests/test_registry.py      # request-registry pure-function unit tests
python3 dtc/tests/test_xts_master.py    # Phase 0 XTS Master pure-function unit tests
python3 dtc/tests/test_duty.py          # Phase 9b NT Orbit Duty Tools pure-function unit tests (fixture-based)
python3 dtc/tests/test_bom.py           # Phase 10 BOM enrichment pure-function unit tests (upsert semantics)
python3 dtc/tests/test_lifecycle.py     # Style/WIP-row lifecycle gating pure-function unit tests (Finalized/Drop + Active/Dropped)
python3 dtc/tests/test_entra_auth.py    # Phase 9b Entra OAuth2 URL-building / callback-parsing pure-function unit tests
python3 dtc/tests/test_phase1_live.py   # live reversible DTC write test (needs UAT)
python3 dtc/tests/test_nt_orbit_live.py # LIVE Entra OAuth2 + real NT Orbit API call (needs NT_ORBIT_* env + RUN_NT_ORBIT_LIVE_TEST=true; skips cleanly otherwise)
python scripts/nt_orbit_oauth_setup.py  # ONE-TIME Entra ID interactive login for NT Orbit (Phase 9b)
python scripts/check_dtc_view.py        # DTC WIP_ITS_USE column readiness check
python scripts/upload_notebooks.py --dry-run   # preview Databricks notebook upload
python scripts/upload_notebooks.py             # deploy notebooks to Databricks
python scripts/deploy_job.py --job all                            # preview all 3 job definitions (dry-run only)
python scripts/deploy_job.py --job main --dry-run                  # preview one job's task graph
python scripts/deploy_job.py --job main                            # create a job (main/duty_compute/images)
python scripts/deploy_job.py --job main --reset-existing <JOB_ID>   # update an existing job in place
```

Run `beproduct/00_init_style_app_registry` (ADB) once per folder, and again whenever
the folder's BeProduct application setup changes, to refresh the cached sample-app IDs.

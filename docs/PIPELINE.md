# Pipeline (v2) — stages, tasks, and gates

**Branch `v2`.** This is the single reference for *what runs, in what order, and
what a row must satisfy to progress*. It consolidates four v1 documents:

| v1 document | Fate |
|---|---|
| `PHASE0/1/2/3/5/7/9/10_WORKFLOW.md` | Per-stage sections below; originals archived in [docs/v1/](v1/) |
| `DIAGRAM.md` | "The DAG" below; original archived |
| `PIPELINE_GATES.md` | "Gates" subsections below; original archived |

Field-level mapping is **not** here — see [SYNC_CONTRACT.md](SYNC_CONTRACT.md).
Why v2 exists and how it differs from v1 — see [MIGRATION_V1_V2.md](MIGRATION_V1_V2.md).
Systems, repo layout and the Delta data model — see [ARCHITECTURE.md](ARCHITECTURE.md).

> **Implementation status.** The stage table marks each task `reused` (v1
> notebook, unchanged) or `NEW` (not yet written). The DAG and this document are
> the specification the v2 notebooks are being built against; `BeProduct_DTC_sync_v2`
> will not run end-to-end until every `NEW` task exists.

---

## The one thing v2 is built around

DTC uses **permissive optimistic locking at request granularity**. Any successful
API write moves the request's server-side `last_read`; every browser session that
loaded earlier is then refused on save ("reload please") and loses in-progress
edits. Confirmed with the DTC developer:

- **Only writes** move the timestamp. Reads are free.
- **Scope is the whole request**, not the view or the row. A write through
  `WIP_ITS_USE` blocks a user editing a different row through a different view.
- DTC is building websocket change-propagation with client-side merge to lift
  this. Not shipped; v2 does not assume it.

So the design objective is **not** "fewer API calls" — it is **the fewest
distinct moments at which a given request is written, packed into the shortest
possible window**. v1 writes each request at up to 5 moments scattered across the
whole DAG. v2 writes it at exactly one point, in ≤2 back-to-back calls.

This is what makes the target cadence (a run every ~2 hours during active style
development) viable at all:

| | Write windows/day | User-visible exposure |
|---|---|---|
| v1 @ 3 runs/day | 9 | ~2 h/day |
| v1 @ 12 runs/day | 36 | ~6–8 h/day — unusable |
| v2 @ 12 runs/day | 12 | minutes |

---

## Design rules

1. **One write window per request per run.** Every DTC WIP write in the main job
   happens in `wip_push` (Stage 40) and nowhere else. Adding a second
   DTC-writing task to this job needs an explicit decision recorded in AGENTS.md.
2. **A run that changes nothing must write nothing.** Not an emergent property of
   per-field diffing — an asserted, unit-tested invariant of the plan builder. At
   12 runs/day this is the difference between safe and intolerable.
3. **≤2 PATCH calls per request.** The floor is 2, not 1: `DTCConnector.patch_rows`
   rejects a body mixing `rowId` (update) and `rowIndex` (insert). One updates
   call, one inserts call, back to back.
4. **No condition tasks.** Every `run_*` flag is read as a plain widget inside its
   own notebook, which exits as a SUCCESS no-op when disabled. Databricks
   propagates a condition task's `EXCLUDED` outcome to every downstream dependent
   *unconditionally, ignoring `run_if`* — and v2's chain is linear enough that one
   gate evaluating false would silently excise everything behind it. v1 learned
   this twice (`gate_phase1`, `gate_phase10`, both removed 2026-09-02); v2 adopts
   it as a blanket rule rather than case-by-case.
5. **Serverless everywhere.** No job clusters, no instance pool, no `wait_cluster`.
6. **Plan against live, not against Delta.** `wip_push` does one live `get_sheet()`
   per request and diffs all contributions against that. v1's Phase 10 and 9b
   planned against a Delta snapshot and needed two full re-pulls to stay honest.

---

## The DAG

```
p0_pull ─► p0_upsert ─► p0_push ─┬─► bp_style_sync ─────► transform ─┬─► request_manager ─┐
                                 │                                   │                    │
                                 ├─► pull_master_dtc ────────────────┼────────────────────┤
                                 │           │                       │                    │
                                 │           └─► phase2_push         │                    │
                                 │                                   │                    │
                                 └─► pull_lineplan_dtc ──────────────┴─► build_costing ───┤
                                                                                          ▼
                                                                                      wip_push
                                                                      (the only DTC write in this job)
```

Companion jobs, unchanged from v1 and deliberately outside this DAG:

```
BeProduct_DTC_sync_duty_compute   compute_duty_rates    NT Orbit → costing_chart. Zero DTC contact.
BeProduct_DTC_sync_images         phase3_images         Style Image multipart upload. Own write window.
```

---

## Stages

| # | Task | Notebook | Status | Depends on |
|---|---|---|---|---|
| 00 | `phase0_pull` | `p0_pull_xts_master_to_delta` | reused | — |
| 00 | `phase0_upsert` | `p0_xts_master_to_directory_upsert` | reused | `phase0_pull` |
| 00 | `phase0_push` | `p5utl_beproduct_master_data_sync` | reused | `phase0_upsert` |
| 10 | `bp_style_sync` | `p1p7_beproduct_style_sync` | reused | `phase0_push` |
| 10 | `pull_master_dtc` | `p1_pull_masters_to_delta` | reused | `phase0_push` |
| 10 | `pull_lineplan_dtc` | `p9a_pull_lineplan_to_delta` | reused | `phase0_push` |
| 20 | `transform` | `v2_build_wip_staging` | **NEW** | `bp_style_sync` |
| 25 | `request_manager` | `p1_dtc_request_manager` | reused | `transform`, `pull_master_dtc` |
| 30 | `build_costing` | `v2_build_costing_chart` | **NEW** | `transform`, `pull_master_dtc`, `pull_lineplan_dtc` |
| 40 | `wip_push` | `v2_wip_push` | **NEW** | `request_manager`, `build_costing` |
| 50 | `phase2_push` | `p2_push_dtc_to_beproduct` | reused | `transform`, `pull_master_dtc` |

Every dependency edge carries `run_if=ALL_DONE`. A stage that fails should degrade
the run, not abort it — notably `wip_push` still runs (and still pushes style, BOM
and sample data) if `build_costing` failed; it simply contributes no duty fields
that round.

---

### Stage 00 — DTC XTS Master → BeProduct Directory

Unchanged from v1. Pulls the DTC `XTS Master` document (requests
`XTS Supplier Master` / `XTS Factory Master`; `XTS Mill Master` is out of scope)
into `dtc_xts_master_ktb`, upserts into `beproduct_directory` matched on
**`name` + `partner_type` together** — BeProduct's real Directory key, not `id`
and not `name` alone — then pushes rows where
`id IS NULL OR extracted_at IS NULL OR modified_at > extracted_at`.

Flag: `run_phase0`. Writes to **BeProduct**, never to DTC.

**Gates** (`sync/xts_master.py`):
1. Exact `requestReference` match against the 2-name allow-list `XTS_REQUESTS`.
2. Not a brand-config row — a Supplier-view row with `Type == "Brand"` is access
   config, not a company. Factory view has no such exclusion.
3. Non-blank name, else the row is dropped.
4. Dedup on `(name, partner_type)`; a true collision prefers a non-null
   `directory_id`, then the lowest `row_index`.

---

### Stage 10 — Source pulls (parallel)

Three independent roots.

**`bp_style_sync`** — BeProduct styles → `ktb_styles`, including the 6 sample
applications (Proto / PreLine / SMS / Fit / PP / TOP).

> **Cadence warning.** Sample-app enrichment is one `app_get` per (style × app) —
> ~876 calls, ~120 s for KTB — and `refresh_mode` is pinned to `FULL` because app
> changes do not bump `style.modifiedAt`, so INCREMENTAL would miss app-only
> edits. At 12 runs/day that is ~10,500 BeProduct calls/day and it will be the
> largest single runtime item once the DTC passes are merged. Splitting sample-app
> enrichment onto its own slower schedule is tracked as an open item in
> [MIGRATION_V1_V2.md](MIGRATION_V1_V2.md).

**`pull_master_dtc`** — in-scope DTC WIP requests → `dtc_wip_<customer>`. This is
the **only** WIP read that feeds planning; v1's `repull_dtc` and `repull_dtc_bom`
are gone.

**`pull_lineplan_dtc`** — the `KTB LinePlan` document → `dtc_lineplan_<customer>`.

**Gates — request-level scoping** (`phase1.is_in_scope()`, the shared choke point
for `registry.build_registry_row()`, `registry.refresh()`'s pre-filter and
request-creation eligibility). The most upstream gate in the pipeline: excluding a
request here removes its rows from every stage below.

1. **Not `(BACKUP)`-named** — case-insensitive regex on `\(backup`, anywhere in
   the reference, any variant (`(BACKUP 2)` included). 81 of 86 active KTB WIP
   requests were backup-named and were being pulled before this fix
   (2026-09-10) — the root cause of the long-standing "~199/227 rows have a null
   `bp_style_number`" problem.
2. **Parses as `<customer> <seasonCode> <brand>`** — ≥3 whitespace-delimited
   tokens with the 2nd matching `[A-Za-z]{2}\d{2}`.
3. **Customer token matches** (case-insensitive) — `KON …` developer requests out.

**Exception:** `pull_lineplan_dtc` has its own independent, *unfiltered* discovery
loop and never calls `is_in_scope()` — the project team has not settled a LinePlan
naming convention and explicitly wants backup-named LinePlan requests included
(2026-09-01). Uniqueness of `Lineplan Ref #` across LinePlan requests is therefore
a human-enforced invariant; Stage 30 only warns on conflict, never blocks.

---

### Stage 20 — `transform` → style × color × material staging  **NEW**

The structural heart of v2. v1's transform produced **style × color**; Phase 10
later fanned that out to **style × color × material** by pushing to DTC, re-pulling,
and planning a second time. v2's transform produces the final grain in one pass by
joining the techpack BOM directly.

Writes `beproduct_to_dtc_staging`. Touches no DTC.

**BOM source** (two-hop, unchanged semantics from v1 Phase 10's "2nd revision"):

```
alb_tpm_<env>.public.customer_teckpack_style_latest   -- resolves WHICH log row is current
   .latest_techpack_style_log_id
      → customer_teckpack_style_log.teckpack_style_log_id
        .custom_fields → xts_data → TECH_PACK_EXTRACTION → Table[Type="BOM"]
```

Joined onto `ktb_styles` on `(bp_style_number = style_no,
season || ' - ' || year = style_season)`, INNER throughout both hops.
`customer_name` is pre-filtered to `bom_customer_name` for scoping/performance
only — the join keys are already customer-correct.

`alb_tpm_*` are Lakebase databases registered in Unity Catalog and are queryable
**only from serverless compute**. In v1 this forced Phase 10 onto a serverless task
of its own; in v2 the whole job is serverless, so the constraint costs nothing and
the BOM read collapses into the transform.

Only two `**MaterialCategory` values are used: **"Main Fabric"** (exactly one per
style by construction) and **"Fabric"** (zero or more). `ColumnHeader` may contain
dict entries (e.g. `{"Colorway": [...]}`) for per-colorway breakdowns this stage
does not use; a plain-string column lookup never matches them, so no special-casing
is needed. If more than one `Type == "BOM"` entry exists, all their `Data` rows are
concatenated.

**Gates — staging eligibility** (`sync/lifecycle.py`, `sync/bom.py`):

1. **Colorway presence is not a gate.** A style with zero colorways gets one row
   with `color = DUMMY_COLOR` ("NO BP COLORWAY") rather than being dropped.
   Stage 40 upgrades that row in place the first time a real colorway appears.
2. **BOM presence is not a gate.** A style with no resolvable Main Fabric segment
   stages with `DUMMY_FABRIC_GROUP` / `DUMMY_FABRIC_ARTICLE` ("NO TPM BOM") and is
   enriched on a later run. Never an error.
3. **Lifecycle** (`should_include_in_staging()`) — non-terminal `Product Status`
   always included; terminal (`Finalized` / `Drop`) included only until the DTC
   row's own `Product Status` has caught up, then excluded until reactivation.
   Fails **open** if the WIP snapshot can't be read.
4. **DTC-marked-Dropped** (`is_wip_row_dropped()`, WIP `"Active / Dropped"`) — a
   safe no-op today: the column's real name was never live-verified (the view
   endpoint 403s; a full scan of 84 active requests found no match). Activates
   automatically once confirmed.
5. **Required non-null** — `bp_style_number`, `season_code`, `brand`, `color`.
   A violation **raises and aborts the run** rather than skipping the row: a null
   here means an upstream data problem (usually a missing `dtc_seasoncode_mapping`
   entry) that needs fixing, not something to work around.
6. **Request name format** — `^[A-Z]+ [A-Z]{2}[0-9]{2} .+$`; also a raise.

Flag: `run_bom` (v1's `run_phase10`) disables only the BOM join, leaving
style × color staging intact.

---

### Stage 25 — `request_manager`

Unchanged from v1. Resolves each pending staging request name against
**active + in-scope** registry rows, writing `dtc_request_mapping`; creates and
shares any missing in-scope request.

- Only in-scope names are created; a brand-less `KTB SS26` logs `NOT_IN_SCOPE`.
- Creation is gated by `dry_run`.
- Sharing (`share_on_create`, default true): all views → `aiagentwip@lifung.com`;
  the Full Version view → the Kontoor Project Team group.
- **Recreate-on-inactive**: a name that previously resolved to a now-inactive
  request falls through to "missing" and is recreated under the same name — there
  is no separate recreate path.
- **Duplicate-name guard**: DTC permits two concurrently-active requests with
  identical names, distinguished only by `requestId`.
  `registry.find_duplicate_active_names()` logs `DUPLICATE_ACTIVE_NAME` and the
  colliding name is neither resolved nor treated as missing, so it is never
  auto-created a third time.

`POST /v1/sheets` body (HTTP 201): `requestReference` (not `requestName`),
non-empty `requestDescription`, `viewName`, and `requestAssigneeSharingViewNames` /
`sheetData` present as arrays (empty `[]` accepted).

Creating a request is a write, but to a request no user has open yet — it does not
violate the one-window rule.

---

### Stage 30 — `build_costing` → `costing_chart`  **NEW**

Same output table and key as v1, different inputs. v1 read the twice-re-pulled
`dtc_wip_<customer>`; v2 reads **staging** (for the material dimension we own) ⋈
**`pull_master_dtc`** (for the DTC-owned dimension) ⋈ **LinePlan**.

That substitution is sound because the split is clean:

| Input | Source in v2 | Why |
|---|---|---|
| `material_no`, `fabric_content`, `fabric_group` | staging | We write these; our plan is authoritative |
| `lineplan_ref`, 4 vendor/factory slots, production country | `pull_master_dtc` | DTC-owned, never written by us — the start-of-run pull is current by definition |

The one v1 dependency that would have blocked this is already gone: `Content` used
to be populated by a DTC-internal trigger polling `Mill Fabric Article #`, which
was confirmed unreliable in UAT; since 2026-09-09 Phase 10 writes it directly, so
staging knows the value without a round-trip. `fabric_type` remains
DTC-trigger-only but is traceability-only, never a filter or key.

Fully overwritten every run, no incremental.

**Key** (`duty.COSTING_KEY`): `[customer, season_code, brand, bp_style_no,
lf_style_no, color_name, lineplan_ref, material_no, supplier_type, supplier,
factory]`. `material_no` is in the key because one style × color fans out to
multiple physical rows that would otherwise collide.

> **`COSTING_KEY` joins and MERGEs must use null-safe equality.** `lf_style_no`
> can legitimately be `NULL`. Spark's `t.c = s.c` and the `.join(other, on=[list])`
> shorthand both evaluate `NULL = NULL` to `NULL`, never `TRUE`, so a row with a
> null key column silently never matches itself — no error, no log line. Use `<=>`
> in SQL and `.eqNullSafe()` in PySpark. This bit both call sites simultaneously
> in v1 (fixed 2026-09-10).

**Gates.** A WIP row must clear three independent gates to produce any
`costing_chart` row, and failing any one produces zero rows with no distinguishing
error — this is the most common source of "why isn't my style in `costing_chart`"
(see the `KTB-00030` case in AGENTS.md, 2026-09-10).

*Completeness filter — all four ANDed:*
1. `material_no` (`Mill Fabric Article #`) non-blank.
2. `bp_style_no` non-blank.
3. `fabric_content` (`Content`) non-blank **and** not literally `"Main Fabric"`
   (guards an old placeholder leak).
4. `fabric_group` is **exactly** `"Main Fabric"` — the "Fabric" segment duplicate
   rows are excluded from costing by design, so duty is only ever computed for
   Main Fabric.

*LinePlan INNER JOIN:*
5. `lineplan_ref` non-blank on the row itself — dropped before the join if blank.
6. That ref must actually match a `dtc_lineplan_<customer>` row; an unmatched ref
   is dropped, not carried through with nulls.

*Per vendor slot (Main/1/2/3), independently:*
7. The slot's vendor must be non-blank. Zero populated slots → zero rows; two of
   four → exactly two rows, transposed.

*Duty back-fill, in order:*
8. **Carry-forward** — the table's own prior `tariff_rate` (keyed by
   `COSTING_KEY`) is `COALESCE`d back in, since a rebuild always resets it to
   `NULL` and no live WIP fallback column exists.
9. **Cache fill** — every duty field is filled directly from
   `nt_orbit_duty_cache`, read-only, zero API calls. The cache is keyed purely on
   `(product_description, origin_country, import_country)` with no style/color/
   vendor identity, so even a brand-new row is filled the instant its exact
   product+origin+market combination has ever been looked up — including for a
   different style. Reuses the same pure functions the NT Orbit job uses to decide
   whether to call (`markets_needing_lookup`, `cache_key`, `is_cache_entry_stale`,
   `merge_lookup_into_row`), with the API call simply never made. Write-once; a
   miss leaves the field `NULL` for `duty_compute` to fill later. This strictly
   supersedes gate 8, which is kept only as a redundant safety net.

Flag: `run_costing`. Checked inside the notebook — **never** as a condition task,
since `wip_push` depends on this stage.

---

### Stage 40 — `wip_push` → the single DTC write window  **NEW**

Everything that writes to a DTC WIP request. Replaces v1's `phase1_push` (Phases
1/4/7), `fill_bom_data` (Phase 10) and `push_duty_rates` (Phase 9b's push half).

Per request, in order:

1. One live `connector.get_sheet(sheet_id, view_id)`.
2. Re-validate the request is still active **by `request_id`** against the current
   registry snapshot — a request can go inactive between resolver and push, and
   DTC's "inactive" means hidden from users, i.e. deleted. Refuse to push if so.
3. Build one combined plan (`sync/wip_plan.py`, **NEW**) composing three
   contributions against that single live snapshot:
   - **style fields** — `phase1.compute_upsert` / `build_target_payload`
   - **material fields** — `bom.plan_style_enrichment`, fanned out against the
     *planned* row set rather than a re-pulled one
   - **duty fields** — `duty.build_wip_patch_fields`, joined on
     `(bp_style_number, color_wash, Mill Fabric Article #)`
4. If the combined plan is empty → **send nothing at all.** No GET-to-PATCH path,
   zero calls, zero user disruption. Design rule 2.
5. Otherwise: one PATCH of updates keyed by `rowId`, then one PATCH of inserts
   keyed by `rowIndex`. Back to back.

`sync/wip_plan.py` composes; it does not re-implement. `phase1.py`, `bom.py` and
`duty.py` keep their decision logic and their existing unit tests unchanged.

**Failure containment.** A failing BOM or duty contribution must degrade to
*omitting those keys*, never to failing the row or the request. In v1 a Phase 10
bug could only corrupt material fields; merging removes that natural blast-radius
limit, so it has to be re-established explicitly in the composition layer.

**Gates — style fields** (`sync/phase1.py`):
1. Value non-blank after `norm()` — `null`, empty, whitespace-only, `"n/a"`,
   `"none"`, `"nan"` all normalize to `None` and are never pushed.
2. Field present in `allowed_cols` (the live view's `dynamicFields`, unioned with
   a static fallback — the view endpoint 403s for this API key, so the data-scan
   fallback alone would miss columns blank in every current row).
3. `DEFAULT_FILL_COLS` (`Supplier`, `Fabric Group`, `Placement`) are **write-once**
   — never overwritten on UPDATE if DTC already holds any non-blank value. This is
   what protects the material contribution's ownership from the style contribution.
4. On UPDATE, a field is sent only if `norm(current) != norm(new)`.
5. Row-level exceptions (skipped and logged, never raised): `missing_bp_style`,
   `season_mismatch` / `brand_mismatch` (when `enforce_scope`), `duplicate_bp_key`,
   `missing_row_id`, `empty_payload`.

**Match key and upsert semantics:**
- In-request key is `(BP Style#, Color / Wash)`. Season and brand are fixed per
  request.
- That key matches **multiple physical rows** — one per material segment. Style
  field updates broadcast to all of them.
- **Dummy colorway upgrade**: the first real colorway UPDATEs the style's
  unclaimed `DUMMY_COLOR` row(s) in place — never insert-then-delete. A second
  real colorway gets a genuine INSERT.
- **INSERT** uses `rowIndex = max(rowIndex)+1` within the request, sparse-aware.
- **Moved-key orphans**: when BP Style#, brand or season changes, the row's
  request changes. The new request gets an INSERT; the row stranded in the old
  request is marked `Product Status = "(removed)"` — an invalid BeProduct value
  that signals the DTC user. Never deleted. Only rows whose key now lives under a
  different request are marked. These marks are part of the *old* request's own
  single write window.

**Gates — material fields** (`sync/bom.py`, `plan_style_enrichment()`):
1. Style must have at least one row (existing or planned) — else no-op.
2. **A "Main Fabric" segment must exist this run.** If not — missing, wrong JSON
   shape, or genuinely absent — **zero actions for the whole style**, regardless
   of how many "Fabric" segments exist. Never reverts.
3. Match key is the **pair** `(Fabric Group, Mill Fabric Article #)`.
   `Placement` / `Content` are deliberately excluded — they are the fields
   expected to legitimately drift for an otherwise-unchanged assignment. Per row:
   - **Exact key match** → upsert `Placement` and/or `Content` independently, each
     only if actually changed **and** the new value is itself non-blank.
     **A blank target value is never pushed** — `KTB-00024`/`KTB-00026` carry
     real, hand-entered `Content` while the source segment is genuinely blank;
     without this guard the next run would silently PATCH `Content: ""`.
   - **Blank `Mill Fabric Article #`** → matched to the target sharing its exact
     Fabric Group, disambiguated by Placement if several share it; if still
     ambiguous, nothing is guessed. A match backfills the article in place
     (one-way, blank → real only) and counts as represented for the fan-out below,
     so a row is never both backfilled and duplicated. Fixes the "frozen row"
     case (`KTB-00025`) where a row first enriched while `**SupplierRefNo` was
     blank could never satisfy the exact-match key once the source filled in.
   - **Un-enriched** (blank, or the `DUMMY_FABRIC_GROUP` sentinel `"NO TPM BOM"`)
     → full first-time enrichment from the Main Fabric segment.
   - **Any other real value** → left completely untouched. Never reverted, never
     blanked, whatever this run's BOM snapshot says.
4. **Fan-out**: for each "Fabric" segment whose key is not already represented —
   by exact match or blank-article backfill — duplicate every existing row of that
   colorway once per such segment.
5. **Coverage is scoped per colorway, not per style.** `KTB-00029` had 2 colors ×
   3 segments = 6 expected rows but only 4, because one color's rows had claimed
   both Fabric segments globally and permanently starved the other. Rows with no
   colorway value collapse into one implicit group.
6. **Blank-vs-blank is never a diff** — DTC's `None` and the source's `""` are
   equivalent when diffing Placement/Content, avoiding a spurious `{"Placement": ""}`
   PATCH on every already-blank matched row.
7. **INSERT payload exclusions** (`build_insert_row_payload()` +
   `compute_non_writable_cols()`): never copies `rowId`/`rowIndex`, never
   `"Style Image"`, never a column the live view marks `type=="contact"` or
   carrying a truthy `formula`.

> **Known hazard, carried over from v1.** Any change to which BOM segments or keys
> count as "current" can leave already-enriched rows whose old key no longer
> matches — correctly left untouched — while the unmatched segments are still
> treated as genuinely new and fan out N×M inserts. This multiplies fast for
> styles with many pre-existing rows. Re-read this before changing the BOM source
> or field mapping again; see AGENTS.md 2026-09-09 for the live case study.

**Gates — duty fields** (`sync/duty.py`):
1. The `costing_chart` row needs at least one non-null of
   `hts_code` / `duty_rate_us|ca|mx` / `tariff_rate`.
2. A target row must be found on `(bp_style_number, color_wash,
   Mill Fabric Article #)` — style+color alone would silently collapse a style's
   multiple material rows into one and push a material's duty onto the wrong row.
3. Only fields that actually differ from the row's current value are included.
4. Multiple `costing_chart` rows (one per vendor slot) map to the **same** WIP
   row, differing only in which columns they target. They must be **merged into
   one object** — two `sheetData` entries sharing a `rowId` in one call is
   rejected with `400 Duplicate rowId found` (confirmed live 2026-09-01). Merging
   is safe because slots target disjoint column names.
5. Tariff Rate columns are defined (`duty.WIP_TARIFF_COL`) but **not live** in the
   WIP view (`WIP_TARIFF_COLS_LIVE = False`, confirmed 2026-07-17). A computed
   tariff stays `costing_chart`-only and the skip is logged, not silently dropped.
   Flip the flag when DTC adds the columns; no other change needed.

Flags: `run_wip_push` (whole stage), `run_duty_push` (duty contribution only).

**DTC write contract** (validated live):
- Upsert — `PATCH /v1/sheets/{sheetId}/views/{viewId}`,
  body `{"sheetData":[{…,"rowId"|"rowIndex":…}]}` → 204
- Delete — `DELETE /v1/sheets/{sheetId}/views/{viewId}/rows`,
  body `{"rowIndexes":[…]}` → 204
- Create — `POST /v1/sheets` → 201, ids nested under `data` with a capital-S `SheetId`
- Share — `POST /v1/requests/{requestId}/shares/{userEmail}` and
  `…/shares/usergroups/{userGroupName}`,
  body `{"viewNames":[…],"message":"…","sendEmail":"Y|N"}` → 201

---

### Stage 50 — `phase2_push` → DTC → BeProduct

Unchanged from v1. Reads the DTC snapshot, writes **BeProduct**. No DTC writes, so
it is outside the write-window constraint entirely and runs in parallel with
Stages 20–40.

Flag: `run_phase2`.

**Gates** (`sync/phase2.py`, `build_beproduct_updates()`):
1. Identity required — no `beproduct_style_id` → `missing_style_id`; a
   colorway-level field also needs `colorway_id` → `missing_colorway_id`.
2. Value transforms apply **before** blank/diff checks — e.g. COO's
   `resolve_coo_country_name()` (2-char code → BeProduct country name). A
   transform returning `None` is treated exactly like a blank DTC value.
3. Blank handling — `push_blanks=false` (default) skips blanks entirely and never
   clears BeProduct; `push_blanks=true` writes an explicit empty string.
4. NOOP when `norm(current_bp) == norm(new_dtc)`.
5. `header_value_conflict` — two DTC rows for one style disagreeing on a
   header-level field is an exception, not an arbitrary pick.
6. `UNSUPPORTED_FIELDS` (currently empty) is skipped *and logged*, never silently
   dropped.

---

## Companion jobs

### `BeProduct_DTC_sync_duty_compute` — NT Orbit lookups

Single task `compute_duty_rates` (`p9b1_compute_duty_rates`). NT Orbit →
`costing_chart` + `nt_orbit_duty_cache`. **Zero DTC contact** — no API key, no
read, no write — so it is free to run on its own schedule with its own latency.
Unchanged in v2.

Auth is Microsoft Entra ID **delegated** OAuth2 (a signed-in person, never
client-credentials), not DTC's `x-api-key`. Entra rotates the refresh token on most
uses; the notebook persists the rotated value to `nt_orbit_oauth_state` each run and
prefers it over the static secret, because `dbutils.secrets` is read-only. Seed once
via `scripts/nt_orbit_oauth_setup.py`; automatic thereafter as long as the job runs
at least every ~90 days.

**Gates — which markets need a call** (`markets_needing_lookup()`):
1. `production_country` non-blank, else zero lookups for the row.
2. Each blank `duty_rate_us|ca|mx` adds its market.
3. `US` is added **even if `duty_rate_us` is filled** when `tariff_rate` is blank
   — `tariff_rate` has no WIP fallback and always resets on rebuild, so keying
   solely on `duty_rate_us` would mean it is never recomputed once filled.
4. If nothing above triggered a lookup but `hts_code` is blank, one `US` lookup is
   forced as a backfill.
5. **Cache short-circuit** — skipped entirely on a `nt_orbit_duty_cache` hit
   younger than `cache_ttl_days` (default 180; tariff policy does change). A
   missing `looked_up_at` is always treated as stale.

Values are written **write-once** from the response's "General Duty" detailed_line
(`duty_rate_xx`) — *not* `data.duty_rate`, which also folds in tariff and fees —
and `tariff_rate` from the sum of other `type="duty"` lines, only ever from a US
call. Calls are serial by default (`orbit_parallel_calls=false`); each takes ~30 s
and sometimes exceeds it (HTTP timeout raised 30 s → 60 s on 2026-09-01).

> **Sequencing gap, unchanged from v1.** This job and the main job run on
> independent schedules. Because every main-job run's own `build_costing` rebuilds
> `costing_chart` before `wip_push` reads it, a value filled between two main-job
> runs only survives if it lands in that window. Stage 30's cache fill makes this
> far less damaging than in v1 — a cached combination is refilled on every rebuild
> regardless of timing — but it is not structurally closed. Manual unstick:
> re-run `duty_compute`, then `w.jobs.run_now(job_id=…, only=["wip_push"])`.

### `BeProduct_DTC_sync_images` — Style Image upload

Single task `phase3_images` (`p3_beproduct_to_dtc_images`). Unchanged in v2.

**This is a second DTC write stream and therefore a second write window.** It
cannot join `wip_push`'s PATCH: image cells are writable *only* through the
multipart `/images` endpoint, and DTC rejects any `sheetData` write to
`Style Image`. Consolidating or co-scheduling the two windows is an open item in
[MIGRATION_V1_V2.md](MIGRATION_V1_V2.md).

Needs nothing from the main job's same run — it reads `dtc_request_mapping` /
`beproduct_to_dtc_staging` left by whichever run last populated them and does its
own live `get_sheet()` immediately before writing.

**Gates** (`sync/phase3.py`, `compute_image_uploads()`):
1. Row has a resolvable match key.
2. Style Image cell is currently blank — idempotent, never re-uploads.
3. Row has a `rowIndex` — the multipart endpoint cannot target a row without one
   (`missing_row_index`).
4. Source, in priority order: **(a) sibling copy** — any other row for the same BP
   Style# in this request that already has a real image, reusing that DTC-hosted
   URL rather than re-downloading and re-transcoding; **(b)** BeProduct's
   `front_image_url`, which must be non-blank and `http(s)://`; **(c)** neither →
   left alone, no exception.
5. Content type (`classify_image_type()`): `jpeg`/`png` as-is; `webp`/`gif`/`bmp`/
   `tiff` transcoded to PNG; `svg+xml` skipped (`unsupported_vector_image`);
   anything else skipped (`unsupported_image_type`).

---

## Cross-cutting

- **"Never revert" is the dominant philosophy.** Style fields
  (`DEFAULT_FILL_COLS`), Phase 2 (blank-skip unless `push_blanks`) and especially
  the material contribution (an entire style takes zero actions rather than risk
  blanking real data). A row failing a gate almost always means "wait for a later
  run", not "this data is lost".
- **Lean PATCH is a real gate, not a style preference.** It appears as
  `diff_updatable_fields`, NOOP-on-match, blank-vs-blank equality and
  `push_already_correct`. An unchanged field must never generate network traffic —
  see design rule 2 and Ground Rule #6 in AGENTS.md.
- **Normalization is shared but not universal.** `phase1.norm()` handles blank,
  `"n/a"`, `"none"`, `"nan"` identically nearly everywhere. Stage 30's
  completeness filter is the exception — it uses direct
  `isNotNull()` / `trim() != ""` Spark conditions, so a `norm()`-recognized
  sentinel in a WIP cell would **not** be treated as blank there.
- **Every gate is a per-run decision** unless stated otherwise. A row failing this
  round is re-evaluated fresh next round once its data changes.

---

## v1 → v2 task map

| v1 task | v2 |
|---|---|
| `wait_cluster` | removed (serverless) |
| `gate_phase0/2/9a/9b` | removed (in-notebook flags, design rule 4) |
| `phase0_pull` / `phase0_upsert` / `phase0_push` | unchanged |
| `bp_style_sync` | unchanged |
| `pull_master_dtc` | unchanged |
| `pull_lineplan_dtc` | unchanged; no longer behind `gate_phase9a` |
| `transform` | → `transform` (Stage 20), now joins BOM and emits style × color × material |
| `request_manager` | unchanged |
| `phase1_push` | → folded into `wip_push` |
| `repull_dtc` | removed |
| `fill_bom_data` | → folded into `transform` (read) + `wip_push` (write) |
| `repull_dtc_bom` | removed |
| `build_costing_chart` | → `build_costing` (Stage 30), reads staging instead of the re-pull |
| `push_duty_rates` | → folded into `wip_push` |
| `phase2_push` | unchanged |
| `compute_duty_rates` (own job) | unchanged |
| `phase3_images` (own job) | unchanged |

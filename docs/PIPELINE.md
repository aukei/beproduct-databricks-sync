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

1. **One write *period* per request per run.** Every `sheetData` write happens
   in `wip_push` (Stage 40). `phase3_images` (Stage 45) is the one other DTC
   writer and is irreducibly separate — image cells are writable only through
   the multipart `/images` endpoint, and DTC rejects any `sheetData` write to
   `Style Image`. It runs immediately after `wip_push` so the two windows are
   **adjacent**, not scattered. In steady state it uploads nothing and opens no
   window at all. Adding any further DTC-writing task needs an explicit
   decision recorded in AGENTS.md.
2. **A run that changes nothing must write nothing.** Not an emergent property of
   per-field diffing — an asserted, unit-tested invariant of the plan builder. At
   12 runs/day this is the difference between safe and intolerable.
3. **One write *window* per request — not a fixed call count.** The floor is 2
   calls, not 1: updates go out as a `patch_rows` PATCH keyed by `rowId`, and
   inserts as an `append_rows` POST to `.../rows`, back to back.
   > **Changed 2026-09-23.** Inserts used to be a second PATCH keyed by a
   > CLIENT-COMPUTED `rowIndex`, and the 2-call floor existed because
   > `patch_rows` rejects a body mixing `rowId` and `rowIndex`. They now use the
   > append endpoint, where the SERVER assigns `rowId`+`rowIndex` and a body
   > carrying either is rejected — so no index is computed anywhere, and the
   > stale-read race it carried is gone. Still two calls, still one window; the
   > 201 returns the new rowIds in send order, so inserted rows are logged with
   > the id DTC actually gave them.
   > **This becomes more than 2 calls at scale.** `batch_size` (default 100)
   > chunks each side, so a request with 250 changed rows issues 3 update
   > calls, not 1. They are still consecutive, so it remains **one write
   > window** — which is the property that actually matters to users — but do
   > not quote "≤2 calls" once the real `KTB` folder is in play. See the
   > go-live checklist in [MIGRATION_V1_V2.md](MIGRATION_V1_V2.md).
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
p0_pull ─► p0_upsert ─► p0_push ─┬─► bp_style_sync ─┬─► transform ─┬─► request_manager ─┐
                                 │                  │              │                    │
                                 │                  └─► pull_bom ──┤                    │
                                 │                                 │                    │
                                 ├─► pull_master_dtc ──────────────┼────────────────────┤
                                 │           │                     │                    │
                                 │           └─► phase2_push       │                    │
                                 │                                 │                    │
                                 └─► pull_lineplan_dtc ────────────┴─► build_costing ───┤
                                                                                        ▼
                                                                                    wip_push
                                                                                        │
                                                                                        ▼
                                                                                  phase3_images
```

**Two jobs, not four** (2026-09-15). `phase3_images` moved INTO this DAG; the
standalone images job is paused and superseded. Only one companion job remains:

```
BeProduct_DTC_sync_duty_compute   compute_duty_rates   NT Orbit → nt_orbit_duty_cache + costing_chart.
                                                       Zero DTC contact. Serverless.
                                                       10:00 and 15:00 HKT.
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
| 20 | `transform` | `p1p7_beproduct_to_dtc_transform` | reused | `bp_style_sync` |
| 20b | `pull_bom` | `v2_pull_bom_segments` (Lakebase techpack) | **NEW** | `bp_style_sync` |
| 25 | `request_manager` | `p1_dtc_request_manager` | reused | `transform`, `pull_master_dtc` |
| 30 | `build_costing` | `p9a_build_costing_chart` (`wip_effective_mode=intent`) | reused + Step 1a | `transform`, `pull_bom`, `pull_master_dtc`, `pull_lineplan_dtc` |
| 40 | `wip_push` | `v2_wip_push` | **NEW** | `request_manager`, `build_costing` |
| 45 | `phase3_images` | `p3_beproduct_to_dtc_images` | reused, moved in | `wip_push` |
| 50 | `phase2_push` | `p2_push_dtc_to_beproduct` | reused | `transform`, `pull_master_dtc` |
| 55 | `push_customer_code` | `v2_push_customer_code` | **NEW** | `pull_bom`, `pull_master_dtc` |

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

1. **No lifecycle marker word** — case-insensitive, matched at a word boundary
   with any suffix: **cancel / backup / archive / delete**
   (`phase1.EXCLUDED_NAME_WORDS`; the stems are `cancel`, `backup`, `archiv`,
   `delet`, so `cancelled`, `archival` and `deletion` match too). Began as a
   `\(backup`-only rule on 2026-09-10 — 81 of 86 active KTB WIP requests were
   backup-named and were being pulled, the root cause of the long-standing
   "~199/227 rows have a null `bp_style_number`" problem — and was widened to
   the full word list on 2026-09-15 after a live inventory turned up
   `Cancel`-named requests and five named simply `DELETED`. The widening also
   catches a **bare** `BACKUP` with no parentheses, which the original rule
   missed.
2. **No `-SUPPLIER` marker** — case-insensitive `-supplier\b`. DTC generates
   supplier-scoped artifact requests named
   `<customer> <seasonCode> <brand>-SUPPLIER <xxx>`; they are DTC's own
   artifacts, never sync targets. Without this they parse as perfectly valid
   in-scope requests, since the parser takes everything after the season code
   as the brand — four appeared in a single afternoon in UAT.
3. **Parses as `<customer> <seasonCode> <brand>`** — ≥3 whitespace-delimited
   tokens with the 2nd matching `[A-Za-z]{2}\d{2}`.
4. **Customer token matches** (case-insensitive) — `KON …` developer requests out.

> **Combined live effect (2026-09-15):** of 109 requests in the `KTB WIP`
> document, **3** are in scope. Before the two 2026-09-15 rules it was 10. The
> `(BACKUP)`-era duplicate in-scope name
> (`KTB FW26 Cancel Wrangler Global TALISMAN LTD`, present twice with different
> `request_id`s) disappears as a side effect — both copies are `Cancel`-named.
> Read-only inventory: `dtc/notebooks/v2_inspect_requests.py`.

**Exception:** `pull_lineplan_dtc` has its own independent, *unfiltered* discovery
loop and never calls `is_in_scope()` — the project team has not settled a LinePlan
naming convention and explicitly wants backup-named LinePlan requests included
(2026-09-01). Uniqueness of `Lineplan Ref #` across LinePlan requests is therefore
a human-enforced invariant; Stage 30 only warns on conflict, never blocks.

---

### Stage 20 — `transform` → style × color staging  (reused, unchanged)

The v1 transform already produces `beproduct_to_dtc_staging` correctly and needs
no change for v2. Writes Delta only; touches no DTC.

> **Correction to an earlier draft of this document.** It said the v2 transform
> would emit the final **style × color × material** grain. It cannot, and should
> not:
>
> 1. The material fan-out depends on which segments a colorway is **already
>    represented by in live DTC**. The transform has no live DTC state, so it
>    cannot compute it — only `wip_plan.compute_request_plan()`, which sees the
>    live rows, can.
> 2. `phase1.compute_upsert()` treats a repeated `(BP Style#, Color / Wash)` as
>    a `duplicate_bp_key` exception, so material-grain rows would make every
>    multi-material style raise.
>
> Staging therefore stays at **style × color**, the BOM becomes a separate
> style-keyed table (Stage 20b), and the material dimension is resolved at
> **plan time** in Stage 40. `repull_dtc` still disappears — planning against
> intent is what removed it, not the staging grain.

### Stage 20b — `pull_bom` → `bom_segments`  **NEW**

Reads the techpack BOM from the **`alb_tpm_*` Lakebase tables** and writes the
raw payload to Delta. Runs in **parallel** with `transform`; touches no DTC, and
writes nothing back to BeProduct.

> **SOURCE WALKBACK 2026-09-22 — this reverses the 2026-09-16 switch below.**
> Owner decision. The BOM read is back on
> `customer_teckpack_style_latest` + `customer_teckpack_style_log`, via the same
> two-hop join v1 used. `run_bom` and the `bom_catalog` / `bom_schema` /
> `bom_table` / `bom_log_table` / `bom_customer_name` parameters are live again.
>
> The table **keeps the name `bom_segments`**; only its column shape reverts
> (`custom_fields` / `parse_error`, not `segments_json` / `error`). All three
> consumers sniff which shape they were handed — via
> `bom.segments_table_mode()` / `segments_from_delta_value()` — so none of them
> changed. Switching the BOM source is a **one-notebook** redeploy in either
> direction, and the PageBomVariation parser stays dormant but unit-tested in
> `sync/bom.py` ("SOURCE 2"). A full snapshot of the BeProduct-sourced pipeline
> is on branch `v2-bomvariation`.
>
> Two things the walkback gives up, both known and accepted:
> - `KTB-00029`'s placements are blank in Lakebase where BeProduct gave
>   `BODICE` / `LINING` / `HEM`. The one-way blank guard in
>   `plan_style_enrichment()` means the values already in DTC survive — this
>   loses future corrections, it does not revert live data.
> - `Content` notation returns to `"Cotton 97%, Spandex 3%"`. See Stage 40's
>   Content note — **write-once must stay on**.
>
> It also breaks Stage 55's material resolution outright; see that stage.

> **Historical — source replaced 2026-09-16, reverted 2026-09-22.** For one
> week BOM came straight from BeProduct's PageBomVariation API. That removed an
> intermediate system, its two-hop join, and the serverless-only access
> constraint that originally forced Phase 10 onto its own task (moot in v2 —
> the whole job is serverless).
>
> Validated against the Lakebase source across all 8 styles at the time:
> identical counts, **7 of 8 byte-identical** on `(Fabric Group, Mill Fabric
> Article #, Placement)`, and the 8th *better* — `KTB-00029`. Switching
> produced **zero** DTC writes. The equivalence evidence is kept because it
> applies symmetrically: walking back should also produce zero writes, and
> anything beyond the two known deltas above is a bug rather than the walkback.
>
> The access pattern below is retained for whoever re-switches.

```
style.app_list(header_id)          → the "BOMVariations" page. Its pageId is
                                     FOLDER-CONSTANT, so it is discovered once
                                     per run (pin it with `bom_page_id`).
style.app_get(header_id, page_id)  → the VARIATION LIST. There is no separate
                                     list endpoint.
GET Style/{h}/PageBomVariation/{p}/Variation/{v}
                                   → {metadata, id, …, rows[]}
```

Field mapping lives in `sync/bom.py`, not the notebook, so it keeps its unit
tests. Two traps worth knowing:

- **`rows[].group` is a GUID, not the group name.** The name is
  `fields["Group"]`, whose values are exactly the two segments the decision tree
  already knows: `"Main Fabric"` and `"Fabric"`.
- **Field objects carry a stable `id`** (`placement`,
  `vendor_material_reference_no`, `fabric_content`, `customer_material_code`)
  alongside the display `name`. Prefer `id` — display names are precisely what
  the original spec got wrong.

`FACE FABRIC/MATERIAL CONTENT` is **structured**
(`[{"value": 97.0, "code": "Cotton"}, …]`), so `bom.render_material_content()`
chooses the rendering: `"97% Cotton / 3% Spandex"`. That matches DTC's dominant
notation but **not all of it** — see Stage 40's Content note; write-once stays on.

Colorway affinity is deliberately ignored (owner decision): variations carry
`syncColorways` / `selectedVariationColorways`, but every variation applies to
all colorways. Per-colorway coverage stays with `plan_style_enrichment()`.

> **Note on writing BOM rows.** The Update DTO takes `rows[].rowFields` — NOT
> `fields`, which is what the GET response uses and which the endpoint silently
> discards (200, nothing applied). `placement` and `Size` write and restore
> cleanly. `CUSTOMER MATERIAL CODE` is the exception: it is not editable on a
> material-linked row, because the row only *displays* it. That push therefore
> targets the Material master — see Stage 55.

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

Parses each payload eagerly and reports per-style diagnostics — how many styles
have a Main Fabric segment, which do not, and which payloads failed to parse — so
a malformed BOM surfaces in **this** task, attributed to a specific style, rather
than silently degrading a contribution two stages later.

The raw `custom_fields` is stored verbatim rather than pre-parsed into segments,
so `sync/bom.py` stays the single source of truth for BOM parsing: Stage 40
re-parses it with the very same function the unit tests cover.

Flag: `run_bom` (v1's `run_phase10`). Disabling it leaves whatever the table
already holds; downstream never reverts on missing BOM data.

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

### Stage 30 — `build_costing` → `costing_chart`  (shared notebook, "intent" mode)

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
8. **WIP fallback** — all five duty fields (`hts_code`, `duty_rate_us|ca|mx`,
   `tariff_rate`) are re-read from the live DTC WIP row's own per-slot columns,
   which is how a value survives this table being fully overwritten every run.
   `tariff_rate` joined this on **2026-09-17**, when its DTC columns went live;
   before that it was reset to `NULL` and restored by a separate Step 4b
   carry-forward keyed on `COSTING_KEY`, now **removed** as redundant.
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
- **Stranded rows** (reported, never written): a row whose `(BP Style#,
  Color / Wash)` key exists **nowhere** in BeProduct — e.g. a colorway deleted
  from BeProduct, or one created directly in DTC. No stage touches these:
  `compute_orphan_marks` only handles keys that moved to a *different* request,
  and the "exists nowhere" case shares a branch with genuinely user-entered
  rows, which must be protected. They are **not inert** — they still feed
  Stage 30, so a stranded colorway can consume NT Orbit lookups and carry duty
  values. `wip_push` surfaces them as `stranded_rows` in its exit JSON and as a
  `STRANDED_ROWS` row in the sync log; deciding what to do with one is a
  data-policy call.

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
   - **`LF Fabric ID`** (added 2026-09-22) is proposed on an exact key match as
     well as at first-time enrichment — a row enriched before the column existed
     only ever reaches the matched branch again, so it would otherwise stay blank
     forever. A **blank is never written**, but it is a **normal upsert, not
     write-once**: the techpack extraction can only run *after* a style reaches
     DTC, so it arrives late and progressively, and a corrected
     `LF_Material_ID` must be able to propagate. Safe because this pipeline is
     its only writer — unlike `Content`, which DTC's own trigger contests.
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
5. Tariff columns are **live for all four slots** and written unconditionally
   (verified against the view definition 2026-09-17; the `WIP_TARIFF_COLS_LIVE`
   switch was removed on owner instruction — tariff overwrites whatever DTC
   holds). The names are **not** symmetric with the HTS/duty ones:
   `Main Factory Tariff`, `Factory 1 - Tariff`, `Factory 2 - Tariff`,
   `Factory 3 - Tariff` — no `rate` suffix, and ` - ` for the numbered slots.
   DTC types them `string` while duty rates are `number`; `values_equal()`
   normalises across that, so a float `0.1` and a stored `"0.1"` are not a diff.

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
3. `US` is added **even if `duty_rate_us` is filled** when `tariff_rate` is blank.
   The original reason (tariff had no WIP fallback, so it reset on every rebuild)
   went away on 2026-09-17, but the rule is still correct and now rarely fires.
4. If nothing above triggered a lookup but `hts_code` is blank, one `US` lookup is
   forced as a backfill.
5. **Cache short-circuit** — skipped entirely on a `nt_orbit_duty_cache` hit
   younger than `cache_ttl_days` (default 180; tariff policy does change). A
   missing `looked_up_at` is always treated as stale.

Values are written **write-once** (unless `force_refresh_duty=true` — see below,
and note that write-once is also what makes `cache_ttl_days` unreachable for an
already-populated row) from the response's "General Duty" detailed_line
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

#### Refreshing a duty rate that has CHANGED — `force_refresh_duty`

Everything above is **fill-blank-only**. An already-populated duty value is
therefore never corrected, at four independent layers:

| # | Layer | Where |
|---|-------|-------|
| 1 | a market is queried only when its `duty_rate_*` cell is blank | `duty.markets_needing_lookup()` |
| 2 | only blank columns are filled | `duty.merge_lookup_into_row()` |
| 3 | `t.c = COALESCE(t.c, s.c)` | `p9b1` Step 4 MERGE |
| 4 | `hts_code`/`duty_rate_*` re-read from the **live DTC WIP columns** | `p9a` Step 4 |

Layer 1 also makes `cache_ttl_days` unreachable in practice: a filled market is
never requested, so its cache entry's age is never examined. The 180-day TTL has
never fired for a populated row. And layers 1+4 form a closed loop — WIP →
`costing_chart` → WIP — in which each side only re-learns what the other holds.

`force_refresh_duty=true` (default `false`) inverts all four plus the staleness
check. Run **both jobs, in this order**:

```python
w.jobs.run_now(job_id=<duty_compute>, job_parameters={"force_refresh_duty": "true"})
# … wait for it to finish, then:
w.jobs.run_now(job_id=<v2>,           job_parameters={"force_refresh_duty": "true"})
```

1. `duty_compute` re-queries every market **live**, refreshes the cache, and
   overwrites `costing_chart`.
2. the main job's `build_costing` lets that fresh cache outrank the WIP fallback,
   and `wip_push` carries the corrected value out to DTC by itself.

**Nothing has to be cleared by hand** — not the WIP duty columns, not
`costing_chart`, not the cache. Order matters: `p9a` Step 4c never calls NT
Orbit, so running the main job alone only re-applies what the cache already has.

Preserved under force: an identical answer still writes nothing (rates compare
numerically, `hts_code` as text — a leading zero is a different code); a failed
market never blanks a stored value; and Step 4c still honours the TTL, so a
years-old entry cannot override a human's correction in WIP.

Cost: ~30 s per (row × market) — 3 live calls per row. Never leave it on for a
scheduled run.

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
| `transform` | unchanged (Stage 20); staging stays style × color |
| `request_manager` | unchanged |
| `phase1_push` | → folded into `wip_push` |
| `repull_dtc` | removed |
| `fill_bom_data` | → split: the BOM read becomes `pull_bom` (Stage 20b, now BeProduct not Lakebase); the DTC write folds into `wip_push` |
| `repull_dtc_bom` | removed |
| `build_costing_chart` | → `build_costing` (Stage 30), reads staging instead of the re-pull |
| `push_duty_rates` | → folded into `wip_push` |
| `phase2_push` | unchanged |
| `compute_duty_rates` (own job) | unchanged |
| `phase3_images` (own job) | unchanged |


---

### Stage 55 — `push_customer_code` → BeProduct **material master**  **NEW**

DTC `"Fabric Customer # or SAP #"` → the material's `customer_material_code`.
Writes **BeProduct only**, so it opens no DTC write window and runs alongside
anything.

**The target is the material, not the BOM row.** `CUSTOMER MATERIAL CODE` is not
editable on a material-linked row — the row only *displays* it, read through from
the linked material. Writing the material makes the BOM row show the new value
immediately (verified live).

```
DTC row --(Fabric Group, Mill Fabric Article #)--> BOM segment
        --materialId-->                            material master
```

That pair is unique within a style and is the same `bom.segment_key()` the
enrichment direction uses, so both directions agree on what "the same fabric
assignment" means. The write always uses the GUID `materialId`.

> **DISARMED 2026-09-22 (`run_customer_code_push=false`) by the Stage 20b source
> walkback.** This is the one stage that did *not* walk back cleanly: only the
> PageBomVariation payload carried `materialId`, and the Lakebase BOM has no
> such column.
>
> Note the failure mode, because it is quiet rather than loud: with Lakebase
> segments every `material_id` is `None`, so `bom_push`'s
> `is_ad_hoc or not material_id` guard funnels **every** row into
> `ad_hoc_skipped` and the stage reports a clean `writes: 0`. That reads as
> success. Hence the flag, rather than trusting a green run.
>
> **The replacement route**: Lakebase carries `**MaterialCode`, which *is* the
> material master's `headerNumber` (e.g. `LF-BD26-000002--SH` — the suffix is
> part of the key). `bom.extract_enrichment_fields()` now carries it as
> `lf_material_id`, and the notebook's `resolve_material_ids()` turns codes into
> GUIDs before planning. It **refuses rather than guesses**: `attributes_list`
> is used directly (never `attributes_get_by_number`, which hides a second match
> behind `next(..., None)`), the server's `Eq` result is post-filtered to an
> exact `headerNumber`, and a code matching zero or several materials goes to
> the `unresolved` bucket. Lookups are cached per code and only fetched for rows
> that actually carry a value — with the DTC column blank, that is **zero API
> calls**.
>
> **Cost of the route**: the GUID was chosen precisely so the planned
> reorganisation of material master into per-customer folders could not break
> this stage. Resolving through `headerNumber` puts that exposure back — it
> depends on `headerNumber` staying globally unique, and the lookup is
> deliberately folder-agnostic.
>
> Re-enable only after `v2_probe_material_code` returns `PROVEN` and the live
> end-to-end proof is repeated.

**Gates** (`sync/bom_push.py`, all pure and unit-tested):
1. A **blank** DTC value is never pushed — this stage can set or change a code,
   never clear one.
2. **Conflicting materials are never written.** Materials are *shared*: the 60
   live DTC rows resolve to just 8 distinct materials, up to 10 rows each. If two
   rows disagree about one material's code, "last row wins" would corrupt a
   record other styles depend on — so the material is skipped and the conflict
   reported.
3. An already-correct material is a no-op.
4. An unmatched `(group, article)`, or an ad-hoc row with no linked material, is
   reported — never guessed.
4b. An LF material code that resolves to **zero or several** materials, or is
   blank, goes to the `unresolved` bucket — reported, never guessed. Kept
   separate from `unmatched` and `ad_hoc_skipped` on purpose: the three need
   different fixes ("fix the DTC fabric assignment", "this row has no material
   by design", "fix or re-key the material master"). A run with anything
   unresolved exits `COMPLETED_WITH_UNRESOLVED`, never `OK` — work was silently
   not done.
5. **Every write is read back and verified.** A 200 from a vendor API means
   accepted, not stored; this pipeline has been burned by that twice.

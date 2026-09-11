# Pipeline Gates — Criteria to Fulfill for a Style/Row to Reach the Next Step

This is a cross-cutting reference, not a per-phase workflow doc (see
`PHASE0_WORKFLOW.md` .. `PHASE10_WORKFLOW.md` / `ARCHITECTURE.md` for those). It
lists every **gate** in the pipeline — the exact criteria a style, colorway, or
material row must satisfy to progress from one stage to the next — in pipeline
order, with the governing code location for each.

**How to use this doc**: when a style/row unexpectedly doesn't show up
downstream (e.g. "why isn't this row in `costing_chart`?"), walk the relevant
phase's gates top-to-bottom and check each condition against the row's actual
data (`dtc_wip_<customer>.data_json`, `ktb_styles`, etc.) — mirrors exactly how
the `costing_chart` KTB-00030/"KTB SS28 Collaborations" investigation (see
AGENTS.md decisions log, 2026-09-10) was diagnosed.

Every gate below is a **per-run** decision unless stated otherwise — a row that
fails a gate this run is simply not processed this round; it is re-evaluated
fresh on the next run once its underlying data changes (this pipeline's
long-standing "never revert, wait for a later round" philosophy applies almost
everywhere below).

---

## Phase 0 — DTC XTS Master → BeProduct Directory

Governs which XTS Master rows become `beproduct_directory` records.
Code: `dtc/python/sync/xts_master.py`.

1. **In-scope request** — only exact `requestReference` matches for
   `"XTS Supplier Master"` and `"XTS Factory Master"` are pulled
   (`XTS_REQUESTS`). `"XTS Mill Master"` and any `(BACKUP)`-named sibling
   request are excluded entirely, regardless of content. (This works via an
   exact-name allow-list — only 2 valid names exist for this document. The
   DTC WIP document, with 75+ dynamically-named requests, needs a genuine
   pattern-based exclusion instead — see "Request-level scoping" below.)
2. **Not a brand-config row** — `is_brand_row()`: a Supplier-view row with
   `Type == "Brand"` (access-sharing config, not a real company) is excluded.
   Factory-view rows have no such exclusion (`EXCLUDE_TYPE_VALUES["FACTORY"] =
   None`).
3. **Non-blank name** — `extract_directory_row()` returns `None` (row
   dropped) if the row's name column is blank.
4. **Dedup key** — `(name, partner_type)` together is the real match key (NOT
   `name` alone — the same name under two different `partner_type`s is two
   legitimate separate records). A true collision (same `(name,
   partner_type)` pair repeated) is tie-broken: prefer the row with a
   non-null `directory_id`, then lowest `row_index`.
5. **Upsert MERGE key into `beproduct_directory`**: `name = name AND
   partner_type = partner_type`.

---

## Request-level scoping — DTC WIP document discovery

The most upstream gate of all: if an ENTIRE DTC request is excluded here,
none of its rows ever reach `dtc_wip_<customer>` at all, which affects
every single phase below that reads from it (Phase 1 diffing, Phase 2,
Phase 3, Phase 9a, Phase 9b, Phase 10). Code: `phase1.is_in_scope()`
(`dtc/python/sync/phase1.py`), the single shared choke point for
`registry.build_registry_row()`, `registry.refresh()`'s pre-filter, and
`p1_dtc_request_manager`'s request-creation eligibility check.

1. **Not `"(BACKUP)"`-named** (added 2026-09-10, owner spec) — a
   case-insensitive regex match on `\(backup` (open-paren + "backup" prefix,
   not requiring an exact `"(BACKUP)"` closing) anywhere in the reference
   string excludes it, regardless of position (immediately after customer,
   in the brand portion, prepended before the customer token, or appended
   at the end — all four observed live) and regardless of variant (e.g. a
   second-generation `"(BACKUP 2)"` marker). Live-confirmed 2026-09-10: 81
   of 86 active KTB WIP requests (94%) were `(BACKUP)`-named and were
   incidentally being pulled before this fix — the direct root cause of the
   long-standing "~199/227 `dtc_wip_ktb` rows have a null `bp_style_number`"
   issue.
2. **Parses as `"<customer> <seasonCode> <brand>"`** — at least 3
   whitespace-delimited tokens, with the 2nd token matching `[A-Za-z]{2}\d{2}`
   (e.g. `"FW26"`). A reference that doesn't parse this way is out of scope.
3. **Customer token matches** the target customer (case-insensitive) — e.g.
   with customer=`KTB`, a `"KON ..."` request (developer test data) is
   excluded.

**Exception: the DTC LinePlan document reads ALL active requests, no name
filtering at all** (`p9a_pull_lineplan_to_delta.py` has its own independent
discovery loop; it never imports or calls `phase1.is_in_scope()`) — the
project team has not decided on a LinePlan naming convention and explicitly
wants `(BACKUP)`-named LinePlan requests included (2026-09-01 decision).
Uniqueness of `"Lineplan Ref #"` across all LinePlan requests is a
human-enforced invariant this pipeline does not itself validate (beyond a
best-effort conflict warning in Phase 9a Step 2).

---

## Phase 1/7 — BeProduct → DTC staging eligibility

Governs which style×colorway rows reach `beproduct_to_dtc_staging` and are
therefore eligible for the Phase 1 (field push) / Phase 7 (sample-history
push) upsert. Code: `beproduct/p1p7_beproduct_to_dtc_transform.py` +
`dtc/python/sync/lifecycle.py`.

1. **Colorway presence is NO LONGER a gate (fixed 2026-09-11, "WIP = style x
   color x material")** — a style with zero colorways used to be silently
   dropped entirely at the colorway-explosion step (Cell 3); it now gets
   exactly one staging row with `color = DUMMY_COLOR` ("NO BP COLORWAY")
   instead, so every style reaches DTC regardless of colorway state. The
   first time a real colorway appears, `phase1.compute_upsert()` upgrades
   that dummy row in place (see `docs/PHASE1_WORKFLOW.md`'s "Match key,
   rowIndex & upsert" section) rather than it ever being a gate again.
2. **Lifecycle gating (Cell 7b, `lifecycle.should_include_in_staging()`)** —
   the ACTIVE terminal-status filter (superseded the older, now-dormant
   `EXCLUDED_STATUSES` filter in `p1p7_beproduct_style_sync.py`, which only
   gates sample-app-enrichment API calls today, not staging):
   - Non-terminal BeProduct `Product Status` (anything except `"Finalized"`/
     `"Drop"`) → **always included**.
   - Terminal `Product Status` → included **only if** the DTC WIP row's own
     current `"Product Status"` hasn't caught up to it yet (one last push
     through); once WIP matches, the row is excluded from every future run
     until the style reactivates (moves back to a non-terminal status).
   - Fails **open** (skips this whole filter, doesn't block anything) if the
     WIP snapshot table can't be read (e.g. very first deployment run).
3. **Not DTC-marked-Dropped** (`is_wip_row_dropped()`, WIP column
   `"Active / Dropped"`) — currently a **safe no-op**: this column's real
   name was never live-verified (view-definition endpoint 403s persist; a
   full data-scan of all 84 active KTB WIP requests found zero matching
   column), so this condition never actually excludes anything today. It
   activates automatically once the real column name is confirmed.
4. **Required non-null fields** (Cell 8 validation — `bp_style_number`,
   `season_code`, `brand`, `color`) — technically a data-quality assertion
   that **raises and aborts the whole run** if violated, not a per-row silent
   skip; a row with a `NULL` here signals an upstream data problem (e.g. no
   `dtc_seasoncode_mapping` entry for the style's `(customer, season)`) that
   needs fixing, not something the pipeline quietly works around. `color`
   should never actually trigger this anymore since gate #1's `DUMMY_COLOR`
   fallback guarantees a non-null value even for a colorless style.
5. **DTC request name format** — must match `^[A-Z]+ [A-Z]{2}[0-9]{2} .+$`
   (also a validation-FAIL/raise, not a silent skip).

---

## Phase 1 — BeProduct → DTC push (field-level)

Governs which individual FIELDS actually get written once a style×colorway
row is in staging. Code: `dtc/python/sync/phase1.py`.

1. **Value must be non-blank after `norm()`** — `null`/empty/whitespace-only
   /`"n/a"`/`"none"`/`"nan"` (case-insensitive) all normalize to `None` and
   are never pushed as a fresh value (a blank BeProduct value never appears
   in a PATCH payload at all, insert or update).
2. **Field must be in `allowed_cols`** (the DTC view's actual live column
   set — `get_view_definition()`'s `dynamicFields`, unioned with a static
   fallback list) — a field not present in the DTC view is silently dropped
   from the payload.
3. **`DEFAULT_FILL_COLS` (`Supplier`, `Fabric Group`, `Placement`) are
   write-once** — on an UPDATE, if DTC's current cell already has ANY
   non-blank value, these 3 columns are never overwritten again, regardless
   of what BeProduct now has (protects Phase 10's Fabric Group/Placement
   ownership and the Supplier default-fill placeholder from being clobbered
   by Phase 1).
4. **On UPDATE, a field is only sent if the normalized value actually
   changed** (`norm(dtc_current) != norm(bp_new)`) — an unchanged field never
   appears in the PATCH body (lean-PATCH requirement, Ground Rule #6).
5. **Pre-upsert exceptions (row completely skipped, logged, not raised)**:
   - `missing_bp_style` — no BP Style# to match on.
   - `season_mismatch` / `brand_mismatch` (when `enforce_scope=True`) — the
     row's season/brand doesn't match the target request's own
     season/brand.
   - `duplicate_bp_key` — this (BP Style#, Color/Wash) key was already seen
     earlier in this same push.
   - `missing_row_id` (matched-but-no-rowId) / `empty_payload` (nothing to
     insert) also short-circuit to an exception, not a push.

---

## Phase 2 — DTC → BeProduct push

Governs which DTC values get written back to BeProduct. Code:
`dtc/python/sync/phase2.py`, `build_beproduct_updates()`.

1. **Identity match required** — no `beproduct_style_id` →
   `missing_style_id` exception, row skipped. A colorway-level field also
   needs a non-null `colorway_id` (`missing_colorway_id` otherwise).
2. **Value transform, if any, applied BEFORE blank/diff checks** — e.g. COO's
   `resolve_coo_country_name()` (2-char code → BeProduct's country-name
   string). A transform that returns `None` (unresolved code) is treated
   exactly like a blank DTC value.
3. **Blank handling**: `push_blanks=False` (default) → a blank/None DTC value
   is skipped entirely, never clears BeProduct. `push_blanks=True` → blank
   values propagate as an explicit empty-string write.
4. **NOOP if BeProduct already matches** — `norm(current_bp) ==
   norm(new_dtc_value)` → counted as a noop, no write attempted.
5. **`header_value_conflict`** — if two different DTC rows for the same
   style disagree on a header-level (style-wide) field's value, the
   conflicting write is rejected as an exception rather than picking one
   arbitrarily.
6. **`UNSUPPORTED_FIELDS`** — any DTC column listed here (currently empty —
   the last entry, `Main Factory Customer ID`, was wired up 2026-09-03) is
   explicitly skipped and logged, never silently dropped without a trace.

---

## Phase 3 — Style Image upload

Governs which DTC WIP rows get a Style Image pushed. Code:
`dtc/python/sync/phase3.py`, `compute_image_uploads()`.

1. **DTC row has a valid match key** — a row with no resolvable (LF Style#,
   Color) identity is skipped outright.
2. **Style Image cell is currently blank** — `is_image_populated()`; an
   already-imaged row is left alone (idempotent, never re-uploads).
3. **Row has a `rowIndex`** — a matched row missing `rowIndex` cannot be
   targeted by the multipart upload endpoint; skipped with reason
   `missing_row_index`.
4. **Image source, in priority order**:
   a. **Sibling copy** — if ANY other row for the same BP Style# in this
      request already has a real image, that image's DTC-hosted URL is
      reused (per-style, no per-colorway dedup — every physical row is
      evaluated independently, fixed 2026-09-04).
   b. Otherwise, BeProduct's own `front_image_url` — must be non-blank and
      start with `http://`/`https://` (`is_valid_image_url()`); if invalid,
      skipped as `no_source_image`.
   c. If neither is available, the row is simply left alone (no exception —
      not every DTC row necessarily has a matching BeProduct source row).
5. **Downloaded content type must be supported** (`classify_image_type()`):
   - `image/jpeg`/`image/png` → uploaded as-is.
   - `image/webp`/`gif`/`bmp`/`tiff` → transcoded to PNG before upload.
   - `image/svg+xml` (vector) → skipped (`unsupported_vector_image`).
   - Anything else/undetected → skipped (`unsupported_image_type`).

---

## Phase 9a — Costing chart formation

Governs which WIP rows produce a `costing_chart` row. Code:
`dtc/notebooks/p9a_build_costing_chart.py`. **This is the gate most often
responsible for "why isn't my style in costing_chart" surprises** — a row
must pass BOTH the Step 1b completeness filter AND the Step 3 LinePlan join
AND have at least one non-blank vendor slot (Step 4); failing any one of the
three silently produces zero rows for that WIP row, with no single error
message pointing at the specific missing piece.

**Step 1b — completeness filter (all 4 required, ANDed together):**
1. `material_no` (WIP `"Mill Fabric Article #"`) non-blank.
2. `bp_style_no` non-blank (guards against legacy `(BACKUP)`-request
   pollution, which is otherwise blank here).
3. `fabric_content` (WIP `"Content"`) non-blank AND not literally the string
   `"Main Fabric"` (guards against an old placeholder-leak bug).
4. `fabric_group` (WIP `"Fabric Group"`) is **exactly** `"Main Fabric"` —
   Phase 10's "Fabric" segment duplicate rows are excluded from
   `costing_chart` entirely by design.

**Step 3 — LinePlan INNER JOIN on `"Lineplan Ref #"`:**
5. `lineplan_ref` must be non-blank on the WIP row itself — dropped before
   the join even runs if blank (this is the gate that caused the
   KTB-00030/"KTB SS28 Collaborations" case: real Main Fabric + Content +
   Main Factory, but a blank WIP `"Lineplan Ref #"` cell dropped both rows
   here even though a matching LinePlan record for that exact style already
   existed under a different key).
6. The non-blank ref must actually MATCH a row in `dtc_lineplan_ktb` — an
   unmatched ref is also dropped by the inner join (not carried through with
   nulls).

**Step 4 — per-vendor-slot (Main/1/2/3), independently for each:**
7. That slot's vendor (`supplier`) must be non-blank — a WIP row with zero
   vendor slots assigned produces **zero** `costing_chart` rows; a row with
   e.g. 2 of 4 slots assigned produces exactly 2 rows (one per populated
   slot), transposed.

**Step 4b — `tariff_rate` carry-forward** (not a gate on inclusion, but on
value preservation): since Step 4 always resets `tariff_rate` to `NULL`,
existing rows' prior `tariff_rate` (keyed by `duty.COSTING_KEY`) is
`COALESCE`d back in so a full `costing_chart` rebuild doesn't silently wipe
previously-computed NT Orbit tariff values.

---

## Phase 9b — NT Orbit duty/HTS lookup and WIP push

Governs which `costing_chart` rows get a fresh NT Orbit API call, and which
computed values actually get pushed back to DTC. Code:
`dtc/python/sync/duty.py`, `dtc/notebooks/p9b2_push_duty_to_wip.py`.

**Which markets need a lookup (`markets_needing_lookup()`):**
1. `production_country` must be non-blank — otherwise **zero** lookups for
   this row (NT Orbit requires an origin/export country; nothing to call
   with).
2. Each of `duty_rate_us`/`duty_rate_ca`/`duty_rate_mx` that is currently
   blank is added to the needed-lookups list.
3. **Even if `duty_rate_us` is already filled**, `"US"` is ALSO added if
   `tariff_rate` is blank (fixes a real bug: `tariff_rate` has no WIP
   fallback and always resets to null on rebuild, so relying solely on
   `duty_rate_us` being blank would mean it's never recomputed once
   `duty_rate_us` is filled even once).
4. If nothing above triggered a lookup but `hts_code` is still blank, one
   `"US"` lookup is forced anyway (backfill side-effect).

**Cache short-circuit (`is_cache_entry_stale()`):** a market otherwise
needing a lookup is skipped (cache hit) if a persistent
`nt_orbit_duty_cache` entry exists for the same `(product_description,
origin_country, import_country)` key AND is younger than `cache_ttl_days`
(default 180). A missing `looked_up_at` timestamp is always treated as
stale (forces a fresh call).

**Pushing a computed value back to WIP (`p9b2_push_duty_to_wip.py`):**
5. The `costing_chart` row must have at least one of
   `hts_code`/`duty_rate_us/ca/mx`/`tariff_rate` non-null to be considered
   at all.
6. A matching live WIP row must be found via `(bp_style_number, color_wash,
   "Mill Fabric Article #")` (NOT just style+color — disambiguates Phase
   10's multiple physical rows per style×color); no match → skipped,
   counted as `push_no_match`.
7. **Only fields that actually DIFFER from the WIP row's current value are
   included in the PATCH** — an already-correct value produces zero API
   calls for that row (`push_already_correct`), matching Ground Rule #6's
   lean-PATCH requirement.

---

## Phase 10 — BOM enrichment from techpack extraction

Governs which DTC WIP rows get Fabric Group/Placement/Mill Fabric Article #/
Content written from techpack data. Code: `dtc/python/sync/bom.py`,
`plan_style_enrichment()`. Source (2026-09-09, "2nd revision"):
`customer_teckpack_style_log.custom_fields` → `xts_data` →
`TECH_PACK_EXTRACTION` → `Table[Type="BOM"]`, joined via
`customer_teckpack_style_latest.latest_techpack_style_log_id`.

1. **Style must have at least one existing WIP row** — `existing_rows`
   empty → no-op (nothing to enrich).
2. **A "Main Fabric" segment must exist this run** (`build_target_segments()`
   returns `None` otherwise) — no Main Fabric anywhere in the current
   `custom_fields` (missing entirely, wrong JSON shape, or genuinely absent
   from the techpack) → **zero actions for the whole style**, regardless of
   how many "Fabric" segments exist. Never reverts already-enriched data.
3. **Per existing row, the decision tree** (match key = `(Fabric Group, Mill
   Fabric Article #)`; `Placement`/`Content` deliberately excluded from the
   key since they're expected to legitimately drift):
   - Row's current key matches a CURRENT segment exactly → upsert
     `Placement` and/or `Content` **independently**, each only if it
     actually changed AND the new value is itself non-blank (never re-writes
     Fabric Group/Mill Fabric Article # once matched). **A blank target
     value is NEVER pushed** (added 2026-09-10, owner spec — fixes a live
     bug where a genuinely-blank source segment would have silently wiped a
     real, manually-entered `Content` value already in DTC). The identical
     guard applies during first-time enrichment too.
   - Row's Mill Fabric Article # is currently **blank** (added 2026-09-10 —
     fixes a live "frozen row" case, `KTB-00025`/legacy `112358013`: a row
     first-enriched while the source's `**SupplierRefNo` was still blank
     can never satisfy the exact-match key above once the source is later
     filled in) → matched to the target sharing its exact Fabric Group,
     disambiguated by Placement if more than one target shares that Fabric
     Group; if still ambiguous, no backfill is guessed. A successful match
     backfills Mill Fabric Article # in place (one-way: blank → real only)
     and is excluded from the insert fan-out below.
   - Row is still un-enriched (blank, or Phase 1's INSERT-time
     `DUMMY_FABRIC_GROUP` sentinel `"NO TPM BOM"`, 2026-09-11 — supersedes
     the old `"MAIN MATERIAL CONTENT"` placeholder) → apply the Main Fabric
     segment's FULL field set (first-time enrichment).
   - Row holds some other real, recognized value not in the current BOM
     data (e.g. a vanished "Fabric" segment, or hand-edited DTC data) →
     **left completely untouched**, never reverted.
4. **New "Fabric" segment insertion** — for each "Fabric" segment whose key
   isn't already represented by ANY existing row **of the SAME colorway**
   for the style, it's genuinely new: duplicate every existing row of that
   colorway once per such segment (fan-out: N rows of one color × M new
   segments = N×M new INSERT rows for that color — this can multiply
   quickly for styles with many pre-existing physical rows; see AGENTS.md's
   2026-09-09 decisions log for a live case study of this fan-out's actual
   real-world impact). **Scoped per colorway, not globally across the whole
   style** (fixed 2026-09-10 — a live gap, `KTB-00029`/LF Style#
   `LFBP-1WTP0002`: one color already had all 3 segments, so the OTHER
   color was wrongly treated as "already covered" too and never got its
   own copies inserted — 2 colors × 3 materials should be 6 DTC rows, only
   4 existed). Rows without a color/colorway value all collapse into one
   implicit group, matching the pre-fix behavior for any caller that
   doesn't track color.
5. **INSERT payload exclusions** (`build_insert_row_payload()` +
   `compute_non_writable_cols()`) — a duplicated row never copies forward
   `rowId`/`rowIndex` (identity), `"Style Image"` (image-type field, DTC
   rejects any sheetData write to it), or any column flagged `formula`-typed
   in the live view definition (computed/derived columns DTC also rejects
   writes to).

---

## Cross-cutting notes

- **"Never revert" is the dominant philosophy** across Phase 1 (
  `DEFAULT_FILL_COLS`), Phase 2 (blank-skip unless `push_blanks`), and
  especially Phase 10 (an entire style/row gets zero actions rather than any
  risk of blanking real data) — a row failing a gate almost always means
  "wait for a later run", not "this data is lost."
- **Lean-PATCH (Ground Rule #6)** shows up as a real gate in Phase 1
  (`diff_updatable_fields`), Phase 2 (NOOP-on-match), and Phase 9b
  (`push_already_correct`) — an unchanged field never generates network
  traffic.
- **Blank-string vs. `None` vs. sentinel values** (`"n/a"`, `"none"`, etc.)
  are normalized identically almost everywhere via `phase1.norm()` — the one
  notable exception is Phase 9a's Step 1b, which uses direct
  `isNotNull()`/`trim() != ""` Spark conditions rather than the shared
  `norm()` helper (worth keeping in mind if a `norm()`-recognized sentinel
  like `"n/a"` ever appears in a WIP cell — it would currently NOT be
  treated as blank by Phase 9a's filter).

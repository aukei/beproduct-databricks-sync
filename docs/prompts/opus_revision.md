# An Exemplar Prompt — BeProduct ⇄ DTC Sync

This file is a **teaching artifact**: it shows how one might have written the
*initial prompt* to a coding agent to produce this repository. The raw,
stream-of-consciousness brief that actually drove the work is preserved at
`/implement_prompts.txt`; this is the "cleaned-up, how-you-should-have-asked"
version, annotated so a human can learn to prompt effectively.

It is **not** project documentation. For what the system actually is, read
`../ARCHITECTURE.md`, `../PHASE1_WORKFLOW.md`/`PHASE2`/`PHASE3`, and `../../AGENTS.md`.

> **Revision note (2026-09-03).** The first version of this file (2026-06-18)
> described a 3-phase, single-job build. The delivered system is now **three
> Databricks jobs** spanning Phases 0/1/2/3/7/9a/9b/10 (Phase 8a retired), and
> roughly half of the total engineering effort went into things that *only live
> data revealed*: undocumented API payload shapes, silently-wrong SDK kwargs,
> orchestrator semantics, and two direction-violation bugs that quietly reverted
> good data on a schedule. This revision rewrites the prompt to ask for that
> system **from scratch**, and adds a new section — ["What live data taught
> us"](#what-live-data-taught-us) — mapping each hard-won bug to the one prompt
> line that would have pre-empted it. Read that section first if you are
> prompting a similar integration.

---

## Why this prompt works (the principles)

A good agent prompt for a non-trivial build does eight things. Watch for them in
the prompt below:

1. **Frames role + goal + done-criteria up front** — the agent knows what
   "finished" means before writing code.
2. **Supplies context and the authoritative sources** (SDK docs, API specs, sample
   data) instead of expecting the agent to guess external behaviour.
3. **States invariants and non-negotiables** ("one field, one direction"; pure
   logic separate from notebooks) so the architecture doesn't drift.
4. **Decomposes into phases with explicit dependencies**, each independently
   shippable and verifiable.
5. **Tells the agent how to handle the unknown** — validate against a sacrificial
   live record, never guess an API contract, and *record* discoveries.
6. **Defines deliverables and acceptance tests**, not just "make it work."
7. **Pre-loads the known failure modes** (see "ADVERSARIAL CHECKLIST" below).
   Every item there cost real debugging time on this project; stating them up
   front is the single highest-leverage edit to the original prompt.
8. **Specifies the orchestration semantics, not just the code.** A correct
   notebook wired into the DAG wrongly is still a broken pipeline — and the
   failure is silent (see the `EXCLUDED`-cascade story below).

The annotations in the right margin (`# ←`) point these out. Delete them when
using the prompt for real.

---

## The prompt

> Copy-paste-ready. Replace `<…>` placeholders. Phases can be sent one at a time.

```text
ROLE & GOAL                                                           # ← 1. role+goal+done
You are building a scheduled, bi-directional data sync between BeProduct (a style
PLM) and DTC (an Excel-like "Data Collab" tool), staged through Databricks/Delta,
plus enrichment from two third-party sources (a duty/tariff API and a techpack/BOM
catalog). "Done" = the pipeline runs as scheduled Databricks job(s), is idempotent,
logs every action, and has unit-tested decision logic.

CONTEXT — SYSTEMS (read these before coding)                          # ← 2. context+sources
- BeProduct: JSON PLM, parent/child (STYLE header ↔ Colorways, Size, BOM, plus
  per-style "applications" e.g. sample requests). One environment, data partitioned
  by Folder (= customer). Access via the `beproduct` Python SDK over OAuth
  (refresh-token grant). SDK docs: https://python.beproduct.com/ ;
  API Swagger: https://developers.beproduct.com/swagger/v1/swagger.json
- DTC: hierarchy Workspace → Document → Request → Sheet → View. Our data is
  denormalized into one flat wide sheet per request. REST API, `x-api-key` header,
  environments UAT and PROD. API specs are in ./dtc/ (Postman JSON + PDF) — treat
  them as a HINT, not the contract, and VERIFY live (see GROUND RULES).
- <Duty API>: 3rd-party HTS/duty-rate classification. Auth is Microsoft Entra ID
  DELEGATED-user OAuth2 (refresh_token → access_token), NOT client-credentials and
  NOT the DTC key scheme. Calls are slow (~30s each).
- <Techpack/BOM catalog>: an existing Unity Catalog catalog owned by another team;
  read-only, join key <…>.
- Databricks: all tables under Unity Catalog schema `lft.beproduct`; secrets in
  scope `beproduct`. Access via the `databricks` CLI/SDK.
- Timezones: BeProduct is UTC. DTC returns UTC but expects writes in the user's
  profile timezone (treat as +08:00 HKT). Spark returns NAIVE datetimes.

SCOPE (start narrow, then widen)                                      # ← scope discipline
- Customer = KTB. DTC Workspace = "KTB". Documents: "KTB WIP" (view "WIP_ITS_USE" —
  the complete projection, always read this one), "XTS Master", "KTB LinePlan".
- In-scope WIP requests are named "<customer> <seasonCode> <brand>" (e.g.
  "KTB FW26 Wrangler"); seasonCode = 2 letters + 2 digits; exactly one brand per
  request, agreeing with the name. Ignore other naming conventions, and NEVER
  treat a "(BACKUP)"-named request as in scope.
- Per-document scoping rules differ and must be asked about, not inferred: some
  documents (LinePlan) intentionally have NO naming convention and uniqueness is
  human-enforced — detect and warn on conflicts, do not filter or block.

WHAT TO SYNC — and the cardinal rule                                  # ← 3. invariants
Each field syncs in EXACTLY ONE direction; never both (no loops). Maintain a single
source-of-truth mapping file (DTC column ⇄ BeProduct fieldId ⇄ direction) and make
the code read from it conceptually. Match BeProduct fields by fieldId, not display
name (names are inconsistently cased and have trailing spaces).
  ** The cardinal rule has a subtle second half: a column written by ANOTHER PHASE
  of this same pipeline also counts as "the other direction." If phase A writes a
  placeholder into a column and phase B later writes the real value, phase A must
  become write-once (INSERT-only / default-fill), or it will silently revert B on
  every scheduled run. Enumerate, per DTC column, exactly ONE owning writer. **
- BeProduct → DTC: <list>. Some are default-fill-on-INSERT only, never on UPDATE.
- DTC → BeProduct: <list>, split into header-level and colorway-level.
- Keys (matched, never overwritten): <list>.
- Binary/derived columns (images, formula fields) are NOT writable via the normal
  JSON path — see ADVERSARIAL CHECKLIST.

KEY MODELLING DECISIONS                                               # ← state hard choices
- Denormalize 1 style → N colorways → N DTC rows; later, → N materials as well
  (style × color × material). Design the row identity for that from day one.
- In-request match key = (<style key>, <color key>); season & brand are fixed per
  request. DTC ops: rowId → UPDATE (PATCH), rowIndex → INSERT/DELETE; a single
  PATCH cannot mix rowId and rowIndex, and cannot repeat a rowId.
- Season mapping is forward-only: SeasonCode = <prefix from a lookup table> + last
  two digits of the BeProduct year (year is algorithmic, prefix is not).
- Any table you fully OVERWRITE each run cannot also be your cache. Expensive
  third-party lookups go in a SEPARATE, never-truncated cache table keyed on the
  API's real input tuple, with a TTL.

GROUND RULES / INVARIANTS (do not violate)                           # ← 5. handling unknowns
- Notebooks can't run locally (Spark/dbutils). Put ALL deterministic logic in pure,
  importable Python modules with unit tests; keep notebooks as thin Spark/IO
  wrappers. Put ALL HTTP in one connector/client module.
- Do NOT guess an API contract. Verify each write against a SACRIFICIAL live UAT
  record before trusting it, and capture the real request/response. When a call
  fails, surface the server's response body (don't swallow it).
- A `dry_run` pass proves NOTHING about a write path. Every write endpoint must be
  exercised for real, at least once, against the sacrificial record — including the
  INSERT path, not just UPDATE. Budget for this explicitly.
- Never trust a declared read-only/locked flag in EITHER direction: some fields
  flagged writable reject writes, and some flagged locked accept them. Determine
  writability empirically (write-then-revert) and derive exclusion sets from the
  metadata signals that actually correlate.
- Keep a running, dated "verified discoveries" log and a "decisions on record" log
  (an AGENTS.md) so the next agent inherits hard-won facts (exact payload shapes,
  quirks, status codes) and does not re-litigate settled choices.
- Every write path needs a dry_run mode and an idempotent re-run. A partially-failed
  run must be safely resumable — do not gate a whole entity on "any child row looks
  done" (that turns a half-failed run into a permanent no-op). Prefer per-row UPSERT
  semantics with an explicit match key.
- Prefer "never revert" over "always converge": if the upstream source for an entity
  is missing this run, take ZERO actions for that entity rather than clearing it.

ORCHESTRATION REQUIREMENTS (as important as the code)                 # ← 8. DAG semantics
- Multi-task job, dependency-ordered, each phase individually toggleable by a job
  parameter, plus a global dry_run.
- A DISABLED phase must never disable anything downstream. On Databricks, an
  untaken condition-task branch marks dependents `EXCLUDED`, which cascades
  UNCONDITIONALLY and ignores `run_if=ALL_DONE`. Therefore: implement phase toggles
  as an early-exit CHECK INSIDE the notebook (exit as SUCCESS), NOT as a DAG-level
  condition task, for any phase that has dependents.
- Split into multiple jobs along dependency and blast-radius lines: keep slow
  third-party compute, and anything that writes to the shared live system, in
  separate jobs from the main chain. Minimize how long any job holds a live
  document open (the target system may silently lose a concurrent human edit).
- Any phase whose output later phases read from Delta must be followed by an
  explicit re-pull task, or those phases read a pre-push snapshot.
- Note which tasks need special compute (see ADVERSARIAL CHECKLIST) rather than
  assuming one cluster serves all.

ADVERSARIAL CHECKLIST — assume each of these is true until disproved  # ← 7. pre-load failures
API / payload
- The written spec's field names are wrong somewhere; a required array is
  undocumented and omitting it 500s/400s with a confusing message; success
  responses nest ids under an unexpected casing.
- Image/binary columns need a separate multipart endpoint keyed by rowINDEX (not
  rowId), so they can only run AFTER rows exist; the JSON path rejects them
  outright — including a mere copy-forward of an existing value during a row
  duplicate.
- Formula/computed columns also reject writes. Build the non-writable exclusion set
  from the view metadata, generically.
- Some image formats are rejected (transcode); some CDN URLs 403 intermittently.
- Batched writes reject duplicate row keys — merge per physical row before sending.
SDK / language
- A wrongly-cased kwarg may be silently absorbed by **kwargs and IGNORED, widening
  a scoped query to the entire account. Verify counts against the UI.
- Spark returns naive datetimes; `datetime.now(timezone.utc)` is aware; subtracting
  them raises. Normalize at the boundary.
- Any regex normalizer you apply to outgoing values will eat formatting you later
  decide is meaningful (e.g. `\s+`→" " destroys intentional newlines).
Data quality
- The "master" sheet is polluted with config/metadata rows interleaved with real
  rows, distinguishable only by one column. Ask which column.
- The real record key may be a COMPOSITE (name + type), so the same name recurring
  is legitimate, not a collision. Confirm the key before writing dedup logic.
- Fields you were told exist may not exist in the live view; fields may be renamed
  or reassigned mid-project. Verify column names from the VIEW DEFINITION endpoint,
  not from sample rows (empty columns don't appear in sample data).
- A "trigger will populate that column" promise from the other team may not fire in
  the test environment. Have a fallback plan for who writes it.
Auth
- Delegated OAuth refresh tokens ROTATE on use. Secret scopes are read-only from
  jobs, so persist the rotated token to a control table and prefer it over the
  seeded secret. Otherwise the pipeline dies ~90 days after go-live.
- Whether a redirect URI is registered as "Web" vs "Mobile/desktop" determines
  whether the client secret is accepted at the token endpoint.

DELIVERABLES                                                          # ← 6. deliverables
- Notebooks per phase (thin), pure modules per phase (tested), one connector module
  per external system, a notebook-upload script, and a job-deploy script that is
  the single definition of the DAG(s) and supports --dry-run and updating in place.
- Docs: architecture/data-model, per-phase workflow docs, per-component API/SDK
  guides, the field-mapping SSOT file, and the discoveries/decisions log.

ACCEPTANCE CRITERIA                                                   # ← define "verified"
- Unit tests pass for all decision logic (upsert/diff/orphan, reverse mapping,
  image planning, request parsing/scoping, enrichment planning, cache staleness).
- A live UAT dry-run shows the correct plan; a live UAT REAL run applies it,
  exercises INSERT and UPDATE, and is safely re-runnable (idempotent), with every
  action in a sync-log table.
- No DTC column has two writers. No field is written in both directions.
- Disabling any single phase parameter leaves every other phase running.

DELIVER IN PHASES (each shippable + verifiable)                       # ← 4. phased plan
- Phase 0 — Partner/Directory master: pull the vendor/factory master sheets into
  Delta, then upsert into the PLM's Directory. Runs FIRST; everything else waits.
- Phase 1 — BeProduct → DTC field upsert (pull → transform → DTC pull → resolve →
  upsert). Create + share missing in-scope requests. Handle moved-key orphans: if a
  key field changes so a style belongs to a different request, INSERT into the new
  request and flag the stale row Product Status="(removed)" (never delete).
- Phase 2 — DTC → BeProduct pushback of the DTC-owned fields (header + colorway).
  Read current BeProduct values live for an accurate no-op diff; blanks don't clear
  unless explicitly told to.
- Phase 3 — Image: after rows exist, upload the front image into the DTC image cell
  (binary, separate endpoint, keyed on rowindex). Only fill blank cells; transcode
  unsupported formats; reuse a sibling row's already-uploaded image when the value
  is header-level and a sibling already has it.
- Phase 7 — Sample submit history → DTC status columns. The exact string format is a
  business decision: ask for a literal example of the desired cell contents, and
  check that no normalizer downstream mangles it.
- Phase 10 — Material/BOM enrichment from the techpack catalog: expand each
  style×color row into style×color×material (UPDATE the existing row for the main
  segment, INSERT duplicates for additional segments). Per-row UPSERT semantics,
  never-revert.
- Phase 9a — Join WIP × LinePlan and transpose the N vendor/factory slots into a
  costing chart table (full overwrite each run).
- Phase 9b — For costing rows missing duty data, call the duty API (cache first,
  serially by default), fill the costing table, and optionally push back to the
  live sheet as a separate job.

OPEN QUESTIONS — ask me, don't assume                                # ← invite clarification
- Confirm the exact BeProduct fieldIds for ambiguous fields, and the real record
  key for each master entity.
- For each DTC column: who is its single owner (this pipeline's phase X, another
  team's trigger, or a human)?
- Who owns/creates DTC requests, and what visibility/sharing is required?
- A sacrificial in-scope UAT request id I can write to reversibly?
- Which target columns are images/formulas/read-only in the live view?
- Concurrency: can this pipeline write while a human has the document open?
```

---

## How the structure maps to good prompting

| Prompt section | Principle it demonstrates |
|----------------|---------------------------|
| ROLE & GOAL + ACCEPTANCE CRITERIA | Define success before implementation; make "done" testable. |
| CONTEXT — SYSTEMS (with links to SDK/specs) | Give the agent the authoritative sources; don't make it hallucinate external APIs. |
| SCOPE | Constrain v1 to one customer/view/naming so the agent ships, then generalize. |
| WHAT TO SYNC + cardinal rule | One crisp invariant ("one field, one direction — including across your own phases") prevents a whole class of bugs. |
| KEY MODELLING DECISIONS | Name the row-identity and cache-vs-overwrite choices before code exists; both are expensive to retrofit. |
| GROUND RULES | Encode architecture (pure-logic-vs-IO) and a method for the unknown (validate live, never guess, log discoveries). |
| ORCHESTRATION REQUIREMENTS | The DAG is part of the deliverable; toggles and gating have semantics that can silently disable half the pipeline. |
| ADVERSARIAL CHECKLIST | Convert prior incidents into up-front constraints — the cheapest debugging you will ever do. |
| DELIVER IN PHASES | Dependency-ordered, independently verifiable increments beat a big-bang ask. |
| OPEN QUESTIONS | Explicitly invite clarification so the agent surfaces ambiguity instead of guessing. |

---

## What live data taught us

Each row is a real bug or surprise from this project (full detail, with dates, in
`../../AGENTS.md`), the class it belongs to, and the prompt line that would have
pre-empted it. This table is the actual value of this document.

### 1. The spec is not the contract

| What happened | Prompt line that pre-empts it |
|---|---|
| `POST /v1/sheets` used `requestReference`, not the documented `requestName`; omitting two empty arrays crashed the server with "Cannot read properties of undefined (reading 'map')"; the success body nested ids under capital-S `SheetId`. | "Treat the written spec as a HINT, not the contract. Verify each write live and capture the real request/response." |
| Freshly created requests were invisible to the team — a create grants rights to the API identity ONLY; a separate share call per user and per user-group is required. | "Ask who owns/creates requests and what visibility/sharing is required." |
| Allowed columns derived from sample rows produced false "missing column" findings, because empty columns don't appear in sheet data. | "Verify column names from the view-definition endpoint, not from sample rows." |
| The target view was restructured mid-project (198 → 204 fields) and two sample-status columns were reassigned to different meanings. | "Fields may be renamed or reassigned mid-project" + a dated discoveries log so the delta is visible. |

### 2. Only a real write finds the write bugs

| What happened | Prompt line that pre-empts it |
|---|---|
| Phase 10's INSERT path copied the whole source row forward, including the image column → **100% of live INSERTs failed** with "is an image field and cannot have data added to it". Dry-run had passed for weeks; UPDATE-only runs had passed too. | "A dry_run pass proves NOTHING about a write path. Exercise every write endpoint for real, including INSERT, not just UPDATE." |
| Excluding the image column then hit a *second* 400: a **formula** column also rejects writes. The API's own `isReadOnly` flag was `false` on BOTH — useless. The reliable signals were `type == "contact"` and a truthy `formula` key. | "Never trust a declared read-only flag. Derive the non-writable exclusion set generically from view metadata signals you verified." |
| The PLM's `LockField: true` field DID accept API writes (verified by write-then-revert), unblocking a mapping that had been parked as "unsupported" for months. | Same rule, other direction: locked-looking ≠ unwritable. Test, don't assume. |
| A style with 4 vendor slots produced 4 `sheetData` entries sharing one `rowId` → `400 Duplicate rowId found`. | "Batched writes reject duplicate row keys — merge per physical row before sending." |

### 3. Your own pipeline is the other direction

| What happened | Prompt line that pre-empts it |
|---|---|
| Phase 1 had `Fabric Group`/`Placement` as always-overwrite fields, from a pre-Phase-10 era when it wrote a hardcoded `"MAIN MATERIAL CONTENT"` placeholder. Once Phase 10 started writing real BOM values, **the 3×/day schedule silently reverted them** — confirmed in the sync log, not hypothetically. Fix: move both columns to default-fill (INSERT-only). | "A column written by another phase of this same pipeline counts as the other direction. Enumerate exactly ONE owning writer per column." |
| A promised DTC-internal trigger that was supposed to populate `Content` never fired in UAT, permanently blocking a downstream completeness filter. Resolved by having Phase 10 write it directly — an *accepted, explicit* dual-write, plus a backfill branch for rows enriched before the field existed. | "A 'their trigger will fill it' promise may not fire in test. Have a fallback owner — and if you dual-write, say so on the record." |
| An all-or-nothing `style_already_enriched` gate meant a half-failed run became a permanent no-op (any one good row short-circuited the whole style). Replaced with per-row UPSERT keyed on (Fabric Group, Mill Fabric Article #). | "A partially-failed run must be resumable. Prefer per-row UPSERT with an explicit match key over an entity-level 'looks done' gate." |
| Switching to a `_latest` source table left the BOM null for two already-enriched styles; without an explicit never-revert rule, the table swap alone would have wiped their correct data. | "If the upstream source for an entity is missing this run, take ZERO actions for that entity rather than clearing it." |
| The costing MERGE key was first `fabric_content` (free text, not unique), then corrected to `material_no` — because Phase 10 can emit multiple rows per style×color that are otherwise identical. | "Design the style × color × material row identity from day one." |

### 4. The orchestrator has semantics too

| What happened | Prompt line that pre-empts it |
|---|---|
| `run_phase10=false` (a *default*) made `fill_bom_data` `EXCLUDED`, which Databricks cascades **unconditionally, ignoring `run_if=ALL_DONE`** — silently disabling the entire Phase 9a/9b chain on every scheduled run. The same landmine then reappeared twice more via `gate_phase3` and `gate_phase1`. Fix: delete the condition tasks; check the toggle inside the notebook and exit as SUCCESS. | "A disabled phase must never disable anything downstream. Implement toggles as an in-notebook early exit, not a DAG condition task, for any phase with dependents." |
| Phase 10 depended on a WIP snapshot taken *before* Phase 1's push, so it could miss rows Phase 1 had just created in the same run. | "Any phase whose output later phases read from Delta must be followed by an explicit re-pull task." |
| The techpack catalog is a Lakebase DB in Unity Catalog: readable **only from serverless compute**. It worked in a local SQL-warehouse test (serverless) and failed on the classic job cluster with `UnauthorizedAccessException`. | "Note which tasks need special compute rather than assuming one cluster serves all." |
| Splitting into 3 jobs was driven by a *business* constraint: DTC silently loses a human's in-progress edit if the pipeline touches the same request. Slow duty compute (~30s/call, serial) also had no business blocking anything. | "Minimize how long any job holds a live document open. Split along dependency and blast-radius lines." |
| An instance pool is immutable on node type, and `enable_elastic_disk` is rejected when a pool is set. Deleting a pool while a run is in flight fails that run. | Not preventable by prompt — but it *is* preventable by "the deploy script is the single definition of the DAG, and supports --dry-run." |

### 5. Language and SDK footguns

| What happened | Prompt line that pre-empts it |
|---|---|
| `attributes_list(folderId=…)` (wrong casing) was silently swallowed by `**kwargs` and ignored → the query returned **all 104 account-wide styles instead of 8**. Only a UI cross-check caught it. Two other notebooks had the same class of bug in a different shape (no scoping at all, post-filtered client-side). | "A wrongly-cased kwarg may be silently absorbed and ignored, widening a scoped query to the whole account. Verify counts against the UI." |
| `norm()`'s `\s+ → " "` regex would have destroyed the intentional newlines in Phase 7's new multi-line cell format — caught the same day it was introduced. Fixed to `[^\S\n]+`. | "Any regex normalizer will eat formatting you later decide is meaningful." |
| Naive (Spark) vs aware (`datetime.now(timezone.utc)`) subtraction raised `TypeError` in the cache staleness check. | "Spark returns naive datetimes. Normalize at the boundary." |
| Phase 7's cell format went through three iterations in one day (nested array → flat JSON array → quoted CSV lines) because the desired output was described, not exemplified. | "For any formatted string field, ask for a literal example of the desired cell contents." |

### 6. Data is not what you were told

| What happened | Prompt line that pre-empts it |
|---|---|
| The supplier "master" sheet interleaves brand-level access-config rows with real company rows, distinguishable only by a `Type` column (8 of 42 rows were junk). | "Assume the master sheet is polluted with config rows; ask which column distinguishes them." |
| The Directory key was assumed to be `name`; it is actually **`name` + `partner_type`**, so 19 "duplicates" were legitimate distinct records and the tie-break logic was unnecessary. | "Confirm the real record key before writing dedup logic; it may be composite." |
| The advertised "Mill Master" sheet contained zero real mill data in UAT (100% brand-config rows) — descoped rather than pulled empty. | "Verify the source actually contains data before building a phase around it." |
| `app.modifiedAt` is independent of `style.modifiedAt`, so there is **no incremental shortcut** for sample data — the daily job must run FULL. The vendor's "2 calls/sec" was a minimum-throughput SLA, not a cap (10 workers sustained ~7/sec). | "Ask what actually invalidates a cache/incremental filter, and measure the rate limit rather than believing the number." |
| 199 of 227 rows with a null style number all traced to legacy `(BACKUP)` requests — pre-existing pollution, not a sync bug. | "NEVER treat a '(BACKUP)'-named request as in scope." |
| A subset of CDN image URLs return 403 (SAS) within their validity window — a source-side issue, still open, and correctly *not* treated as a code defect. | "Distinguish source-data defects from code defects in the discoveries log; not every failure is yours to fix." |

### 7. Auth is a long-term liability, not a setup step

| What happened | Prompt line that pre-empts it |
|---|---|
| Entra rotates the delegated refresh token on most uses, and `dbutils.secrets` is read-only from jobs. The rotated token is persisted to a Delta control table and preferred over the seeded secret — making it "seed once, then automatic" as long as the job runs at least every ~90 days (it runs 3×/day). | "Delegated OAuth refresh tokens rotate. Secret scopes are read-only from jobs — persist the rotated token to a control table." |
| `AADSTS700025`: registering the redirect URI under "Mobile and desktop applications" makes Entra treat every exchange as a public client and reject the client secret — *even though the app has one*. Determined by platform type, not by the app's config. | "Whether a redirect URI is registered as Web vs Mobile/desktop determines whether the client secret is accepted." |

---

## The habits that mattered most

- **"Verify, don't guess" + a dated discoveries log.** The DTC create/share/image
  endpoints all behaved differently from the written spec. Because the prompt told
  the agent to validate against a sacrificial record and record findings, those
  facts became durable (`AGENTS.md`) instead of being rediscovered each session.
  The log's second half — *decisions on record* — turned out to matter as much: it
  is what stops a later session from "fixing" a deliberate choice.
- **A real write, not a dry run.** Every single one of the most expensive bugs
  (image/formula INSERT rejection, duplicate rowId, the silent Phase 1 revert)
  was invisible to dry-run and to unit tests. Budget for a real, reversible,
  end-to-end live push per phase — and re-run it after every "small" change.
- **Pure logic vs. notebooks.** Insisting decision logic live in unit-tested
  modules (not notebooks that can't run locally) is what made the behaviour
  testable at all — and is why each of the fixes above shipped with tests that
  encode the exact failing scenario.
- **One writer per column.** The invariant most worth stating twice: it was
  violated once accidentally (Phase 1 vs Phase 10) and once deliberately
  (Phase 10 vs the DTC trigger). The deliberate one is fine *because it is on the
  record*; the accidental one silently destroyed live data on a schedule.

> The repository these instructions produced is documented in `../ARCHITECTURE.md`.

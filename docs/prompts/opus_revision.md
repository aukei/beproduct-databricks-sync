# An Exemplar Prompt — BeProduct ⇄ DTC Sync

This file is a **teaching artifact**: it shows how one might have written the
*initial prompt* to a coding agent to produce this repository. The raw,
stream-of-consciousness brief that actually drove the work is preserved at
`/implement_prompts.txt`; this is the "cleaned-up, how-you-should-have-asked"
version, annotated so a human can learn to prompt effectively.

It is **not** project documentation. For what the system actually is, read
`../ARCHITECTURE.md`, `../PHASE1_WORKFLOW.md`/`PHASE2`/`PHASE3`, and `../../AGENTS.md`.

---

## Why this prompt works (the principles)

A good agent prompt for a non-trivial build does six things. Watch for them in
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

The annotations in the right margin (`# ←`) point these out. Delete them when
using the prompt for real.

---

## The prompt

> Copy-paste-ready. Replace `<…>` placeholders. Phases can be sent one at a time.

```text
ROLE & GOAL                                                           # ← 1. role+goal+done
You are building a scheduled, bi-directional data sync between BeProduct (a style
PLM) and DTC (an Excel-like "Data Collab" tool), staged through Databricks/Delta.
"Done" = the whole pipeline runs as one schedulable Databricks job, is idempotent,
logs every action, and has unit-tested decision logic.

CONTEXT — SYSTEMS (read these before coding)                          # ← 2. context+sources
- BeProduct: JSON PLM, parent/child (STYLE header ↔ Colorways, Size, BOM). One
  environment, data partitioned by Folder (= customer). Access via the `beproduct`
  Python SDK over OAuth. SDK docs: https://python.beproduct.com/ ; API Swagger:
  https://developers.beproduct.com/swagger/v1/swagger.json
- DTC: hierarchy Workspace → Document → Request → Sheet → View. Our data is
  denormalized into one flat wide sheet per request. REST API, `x-api-key` header,
  environments UAT and PROD. API specs are in ./dtc/ (Postman JSON + PDF) — treat
  them as the contract but VERIFY live (see GROUND RULES).
- Databricks: all tables under Unity Catalog schema `lft.beproduct`; secrets in
  scope `beproduct`. Access via the `databricks` CLI/SDK.
- Timezones: BeProduct is UTC. DTC returns UTC but expects writes in the user's
  profile timezone (treat as +08:00 HKT).

SCOPE (start narrow)                                                  # ← scope discipline
- Customer = KTB. DTC Workspace = "KTB", Document = "KTB WIP", View = "WIP_ITS_USE"
  (the complete projection — always read this one).
- In-scope requests are named "<customer> <seasonCode> <brand>" (e.g.
  "KTB FW26 Wrangler"); seasonCode = 2 letters + 2 digits; exactly one brand per
  request, agreeing with the name. Ignore any other naming conventions.

WHAT TO SYNC — and the cardinal rule                                  # ← 3. invariants
Each field syncs in EXACTLY ONE direction; never both (no loops). Maintain a single
source-of-truth mapping file (DTC column ⇄ BeProduct fieldId ⇄ direction) and make
the code read from it conceptually. Match BeProduct fields by fieldId, not display
name. Directions:
- BeProduct → DTC: Product Status, Style Description, Class, Sub Class, Division,
  Brand, Garment Finish, Tech Pack Stage, Fabric Group, Placement.
- DTC → BeProduct: Legacy Code, Main Vendor (Sampling), Main Factory (Sampling)
  [header]; Lot# [colorway]; Main Factory Customer ID (no BeProduct target → skip
  and log).
- Keys (matched, never overwritten): LF Style#, Color / Wash.
- Style Image: BeProduct → DTC, but binary and handled separately (see Phase 3).

KEY MODELLING DECISIONS                                               # ← state hard choices
- Denormalize 1 style → N colorways → N DTC rows. For now hardcode ONE BOM/fabric
  line per (style × color): Fabric Group="MAIN MATERIAL CONTENT",
  Placement=<main_material_content>. (Leave a seam for a future style×bom table.)
- In-request match key = (LF Style#, Color / Wash); season & brand are fixed per
  request. DTC ops: rowId → UPDATE (PATCH), rowIndex → INSERT/DELETE; a single
  PATCH cannot mix rowId and rowIndex.
- Season mapping is forward-only: SeasonCode = <prefix from a lookup table> + last
  two digits of the BeProduct year (year is algorithmic, prefix is not).

GROUND RULES / INVARIANTS (do not violate)                           # ← 5. handling unknowns
- Notebooks can't run locally (Spark/dbutils). Put ALL deterministic logic in pure,
  importable Python modules with unit tests; keep notebooks as thin Spark/IO
  wrappers. Put ALL HTTP in one connector/client module.
- Do NOT guess an API contract. Verify each write against a SACRIFICIAL live UAT
  record before trusting it, and capture the real request/response. When a call
  fails, surface the server's response body (don't swallow it).
- Keep a running, dated "verified discoveries" log (an AGENTS.md) so the next agent
  inherits hard-won facts (exact payload shapes, quirks, status codes).
- Every write path needs a dry_run mode and an idempotent re-run.

DELIVERABLES                                                          # ← 6. deliverables
- Notebooks for: BeProduct style pull; denormalizing transform; DTC pull
  (registry-driven); request resolve/create; BeProduct→DTC push; DTC→BeProduct
  pushback; image upload; and ONE orchestrator that runs them in dependency order
  with per-phase toggles + dry_run.
- Pure modules (e.g. sync/phase1, phase2, phase3, registry) + unit tests.
- A deploy script that uploads notebooks and modules to the workspace.
- Docs: an architecture/data-model doc, a per-phase workflow doc, per-component
  API/SDK guides, and the field-mapping SSOT file.

ACCEPTANCE CRITERIA                                                   # ← define "verified"
- Unit tests pass for all decision logic (upsert/diff/orphan, reverse mapping,
  image planning, request parsing/scoping).
- A live UAT dry-run shows the correct plan; a live UAT real-run applies it and is
  safely re-runnable (idempotent), with every action in a sync-log table.
- No field is ever written in both directions.

DELIVER IN PHASES (each shippable + verifiable)                       # ← 4. phased plan
- Phase 1 — BeProduct → DTC field upsert (pull → transform → DTC pull → resolve →
  upsert). Handle moved-key orphans: if a key field changes so a style belongs to a
  different request, INSERT into the new request and flag the stale row
  Product Status="(removed)" (never delete).
- Phase 2 — DTC → BeProduct pushback of the DTC-owned fields (header + colorway-level
  Lot#). Read current BeProduct values live for an accurate no-op diff; blanks don't
  clear unless explicitly told to.
- Phase 3 — Image: after rows exist, upload the BeProduct front image into the DTC
  "Style Image" cell (binary, separate endpoint, keyed on rowindex). Only fill blank
  cells; transcode unsupported formats; skip what can't be rasterised.

OPEN QUESTIONS — ask me, don't assume                                # ← invite clarification
- Confirm the exact BeProduct fieldIds for ambiguous fields.
- Who owns/creates DTC requests, and what visibility/sharing is required?
- A sacrificial in-scope UAT request id I can write to reversibly?
```

---

## How the structure maps to good prompting

| Prompt section | Principle it demonstrates |
|----------------|---------------------------|
| ROLE & GOAL + ACCEPTANCE CRITERIA | Define success before implementation; make "done" testable. |
| CONTEXT — SYSTEMS (with links to SDK/specs) | Give the agent the authoritative sources; don't make it hallucinate external APIs. |
| SCOPE | Constrain v1 to one customer/view/naming so the agent ships, then generalize. |
| WHAT TO SYNC + cardinal rule | One crisp invariant ("one field, one direction") prevents a whole class of bugs. |
| GROUND RULES | Encode architecture (pure-logic-vs-IO) and a method for the unknown (validate live, never guess, log discoveries). |
| DELIVER IN PHASES | Dependency-ordered, independently verifiable increments beat a big-bang ask. |
| OPEN QUESTIONS | Explicitly invite clarification so the agent surfaces ambiguity instead of guessing. |

### Two habits that mattered most here

- **"Verify, don't guess" + a discoveries log.** The real DTC `create` and `share`
  and `image` endpoints all behaved differently from the written spec (field names,
  required arrays, response casing, webp rejection). Because the prompt told the
  agent to validate against a sacrificial record and record findings, those facts
  became durable (`AGENTS.md`) instead of being rediscovered each session.
- **Pure logic vs. notebooks.** Insisting decision logic live in unit-tested modules
  (not notebooks that can't run locally) is what made the behaviour testable at all.

> The repository these instructions produced is documented in `../ARCHITECTURE.md`.

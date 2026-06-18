# The Gemini Exemplar Prompt — BeProduct ⇄ DTC Sync

This file is a **teaching artifact**: it demonstrates how to write an *initial prompt* optimized specifically for **Google Gemini** models (such as `gemini-1.5-pro`, `gemini-2.0`, and `gemini-3.5-flash`) to produce this entire repository. 

While the raw historical prompts are in `/implement_prompts.txt` and the Claude-style exemplar is in `./opus_revision.md`, this document showcases how to leverage Gemini's unique architectural strengths—such as its **massive context window (up to 2M tokens)**, **superior XML parsing**, and **strict adherence to system instructions**—to achieve an extremely clean, robust, and zero-hallucination code generation loop.

This is **not** project documentation. For system-specific guides, refer to `../ARCHITECTURE.md`, `../PHASE1_WORKFLOW.md`/`PHASE2`/`PHASE3`, and `../../AGENTS.md`.

---

## The Gemini Prompting Philosophy

When prompting Gemini for complex coding tasks, you should design your prompts differently than you would for other models. Gemini is built with specific capabilities that thrive on structures and raw datasets:

```
┌────────────────────────────────────────────────────────────────────────┐
│                        GEMINI PROMPT STRUCTURE                         │
├────────────────────────────────────────────────────────────────────────┤
│ 1. SYSTEM INSTRUCTIONS (Immutable rules, Role, Constraints)            │
├────────────────────────────────────────────────────────────────────────┤
│ 2. XML-TAGGED CONTEXT (Raw API specs, SDK files, DB schemas)           │
├────────────────────────────────────────────────────────────────────────┤
│ 3. USER TASK & SCOPE (Phased goals, Key matching logic)                │
├────────────────────────────────────────────────────────────────────────┤
│ 4. ACCEPTANCE & SELF-VERIFICATION (Test protocols, Log formatting)     │
└────────────────────────────────────────────────────────────────────────┘
```

### 1. Feed Raw Sources, Not Lossy Summaries (The "Infinite Context" Advantage)
Gemini’s enormous context window means you should **never summarize API specs or SDK documentation**. Doing so introduces human translation errors and invites model hallucinations. Instead, inject the raw Swagger schemas, Postman dumps, or SDK source codes directly into the prompt. Gemini excels at navigating large documents without attention fatigue.

### 2. Strict XML-Tagged Boundaries
Gemini parses structured data in XML tags exceptionally well. By wrapping code context, specs, and data mapping within clear XML blocks (e.g., `<DtcApiSpec>`, `<BeProductSdkInfo>`), you keep the context segregated from the instructions, which prevents instruction dilution.

### 3. Separation of System Instructions vs. User Prompts
Gemini treats "System Instructions" as high-priority, absolute boundaries. Putting architectural invariants ("never write a field bi-directionally", "keep notebooks as thin wrappers") in the System Instructions prevents the agent from violating your architectural goals during multi-turn generation.

### 4. Interactive Live Verification Loops
Gemini is highly active when utilizing tools. Designing a clear protocol for the model to "verify before trusting" and log findings dynamically ensures that any discrepancies in the sparsely-documented APIs are caught early, documented in an `AGENTS.md` file, and self-corrected.

---

## The Gemini-Optimized Prompt

> This template is copy-paste-ready. Wrap raw context files (like the DTC Swagger JSON, BeProduct Python SDK README, or database schemas) directly within the indicated XML tags.

```xml
<SystemInstructions>
Role: You are a senior principal cloud engineer and integrations architect.
Goal: Build a scheduled, robust, bi-directional, and idempotent data sync between BeProduct (style PLM) and DTC (Excel-like "Data Collab" sheets) staged through Databricks/Delta.

Architectural Invariants (NON-NEGOTIABLE):
1. PURE PY MODULES VS NOTEBOOKS: Notebooks cannot run locally and are hard to unit test. You must put all deterministic sync/mapping logic in pure, importable Python modules under `./dtc/python/sync/` and test them. Notebooks must only act as thin Spark/IO wrappers that import and call these modules.
2. HTTP ISOLATION: Put all HTTP and REST logic inside a single connector/client module (`./dtc/python/client/rest_client.py` and `./dtc/python/connectors/dtc.py`). Never leak request/session boilerplate into business logic.
3. SINGLE DIRECTION MAPPING: Each field must sync in EXACTLY ONE direction (no loops). Maintain a single-source-of-truth file at `./docs/beproduct_style_interested_fields.txt` mapping columns ⇄ BeProduct fieldId ⇄ sync direction. Code constants must mirror this file.
4. RECOVERY & LOGGING: When any HTTP call fails, log the full error response body. Every write path must have a `dry_run` mode, be completely idempotent, and record its results in a sync-log table.
5. IN-SCOPE BOUNDARIES: Focus strictly on Customer = KTB. Naming convention is "<customer> <seasonCode> <brand>" (e.g. "KTB FW26 Wrangler"). Ignore any requests that do not match this pattern.
</SystemInstructions>

<Context>
  <DtcApiSpec>
    <!-- [Inject the raw DTC Postman API JSON dump or PDF API specification text here] -->
  </DtcApiSpec>
  
  <BeProductSdkInfo>
    <!-- [Inject the raw BeProduct Python SDK documentation, README, or example scripts here] -->
  </BeProductSdkInfo>
  
  <FieldMappingSSOT>
    <!-- [Inject the contents of beproduct_style_interested_fields.txt here] -->
    BeProduct -> DTC fields: Product Status, Style Description, Class, Sub Class, Division, Brand, Garment Finish, Tech Pack Stage, Fabric Group, Placement.
    DTC -> BeProduct fields: Legacy Code (header), Main Vendor (Sampling) (header), Main Factory (Sampling) (header), Lot# (colorway).
    Keys (matched, never overwritten): LF Style#, Color / Wash.
    Style Image: BeProduct -> DTC (Phase 3 binary upload).
  </FieldMappingSSOT>
  
  <KeyModelingDecisions>
    - Denormalize 1 style -> N colorways -> N DTC rows. Hardcode ONE BOM line per (style x color) for now: Fabric Group="MAIN MATERIAL CONTENT", Placement=<main_material_content>.
    - Match Key: Within a sheet, a row is uniquely identified by (LF Style#, Color / Wash).
    - RowIndex: DTC keys updates by rowId, but inserts/deletes by rowIndex. You cannot mix rowId and rowIndex in a single PATCH. Map sparse rowIndex properly (coalesce max + 1 on insert).
    - Season mapping: SeasonCode = <prefix from lookup table> + last 2 digits of BeProduct year.
  </KeyModelingDecisions>
</Context>

<Tasks>
Execute this build in three distinct, sequential phases. Verify each phase with tests and live UAT dry-runs before moving to the next.

<Phase1_BeProductToDtc>
Objective: Extract BeProduct styles, denormalize to DTC format, and upsert them to corresponding DTC sheets.
- If a brand-new season + brand is found, automatically create the DTC Request/Sheet via POST /v1/sheets and share all views with 'aiagentwip@lifung.com' and the 'Full Version' view with 'Fabric Group'.
- Compute diffs: for exist keys, UPDATE via PATCH with existing rowId. For new keys, INSERT via PATCH with computed rowIndex.
- Orphan Handling: If a style's key fields change (e.g., brand or season changes), INSERT the style into the new request, and mark the stale row in the old request as "Product Status = (removed)" (do not delete).
</Phase1_BeProductToDtc>

<Phase2_DtcToBeProduct>
Objective: Sync DTC-owned fields back into the BeProduct style (header and colorway-level Lot#).
- Read current BeProduct values live to perform a tight no-op comparison.
- Only push modifications. DTC blanks must not clear BeProduct unless 'push_blanks=true'.
- Perform the reverse mapping, taking the flat colorway rows and updating the BeProduct Colorways array.
</Phase2_DtcToBeProduct>

<Phase3_StyleImages>
Objective: Stream front images from BeProduct to DTC.
- Run as an independent step after Phase 1 has ensured the sheet rows exist.
- Re-read sheets. If "Style Image" cell is empty and BeProduct has a valid "front_image_url", download the image binary.
- Transcode unsupported formats (like webp) to PNG using Pillow, then upload via multipart/form-data to DTC binary endpoint.
</Phase3_StyleImages>
</Tasks>

<VerificationAndAcceptance>
1. WRITE NO GUESSWORK CONTRACTS: Do not assume API behavior. Write a simple validation script to execute writes against the sacrificial UAT record (ID: 6a26581854e92e7acd8fa71b). Read the exact HTTP response headers and payloads.
2. CHRONICLE DISCOVERIES: Keep a dated log (`AGENTS.md`) recording any API behaviors that differ from the written specs.
3. UNIT TESTS: Every core sync module (phase1, phase2, phase3, registry) must have associated unit tests covering mapping, diff calculation, sparse rowIndex planning, and image type classification.
4. RUN CHECKPOINT CHECKS: Run build, type checking, and linter commands locally before preparing files for deployment.
</VerificationAndAcceptance>
```

---

## Why This Prompt Works: Claude (Opus) vs. Gemini

| Feature / Dimension | Claude (Opus) Prompting | Gemini Prompting |
| :--- | :--- | :--- |
| **Context Window Strategy** | Must be carefully budgeted. Prefers highly summarized context, structured JSON descriptions, and precise code snippets. | **Context is Infinite.** Direct feed of entire Swagger specs, Postman files, and raw SDK code modules. Minimal human translation required. |
| **Structural Alignment** | Thrives on natural language specifications, architectural prose, and conceptual guidelines. | **Thrives on XML-bounded blocks.** Parses rigid schema boundaries and separates static rules (System Instructions) from variables. |
| **Error Resiliency** | Relies on deep code analysis, elegant functional abstraction, and logical foresight to avoid errors. | **Thrives on explicit schema contracts.** Excels when given the exact JSON payload shapes and expected status codes to handle edge cases. |
| **Execution Patterns** | Best suited for high-level architectural design and large-scale, single-shot codebase changes. | Best suited for highly analytical parsing, structured output verification, and rigorous step-by-step verification loops. |

---

## Keys to Effective Prompting for Human Learners

When writing your own prompts for Gemini, keep these three primary rules in mind:

1. **Always use `<SystemInstructions>`**: Use system prompts to specify formatting styles, logging rules, constraints, and technologies. This keeps the instructions separate from the actual task you want the agent to perform.
2. **Never Summarize Technical Specifications**: If you have a Swagger file, a database schema, or a README, paste the entire file into the prompt within an XML block. Gemini has the context window to read all of it, and will extract details with far higher fidelity than a human-written summary can provide.
3. **Request a Discoveries Log (`AGENTS.md`)**: Because REST APIs are rarely documented perfectly, tell Gemini to maintain a log of its live discoveries. This forces the model to document the *actual* behavior of the endpoints it encounters, which preserves knowledge for future agents and human developers alike.

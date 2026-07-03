# BeProduct ⇄ DTC Sync — Pipeline Data-Flow Diagram

> Databricks-centred view of the implemented sync pipelines.
> `BeProduct_DTC_sync_dag` — multi-task job, job id 294837488757511. Updated 2026-07-03.

```mermaid
flowchart LR

%% ═══════════════════════════════════════════════════════════════════════════
%%  LEFT: BeProduct        CENTER: Databricks (hub)        RIGHT: DTC
%% ═══════════════════════════════════════════════════════════════════════════

    subgraph BP ["☁ BeProduct — Style PLM"]
        direction TB
        BPAPI(["BeProduct SDK\nOAuth 2.0\n~2–7 calls/s"])
    end

    subgraph DTCS ["☁ DTC — Data Collab (UAT/PROD)"]
        direction TB
        DTCAPI(["DTC REST API\nx-api-key\nWIP_ITS_USE view\n194 columns"])
    end

    subgraph ADB ["⚡ AZURE DATABRICKS — lft.beproduct (Unity Catalog)  ·  data-exchange hub"]
        direction TB

        %% ---- Delta tables ----
        subgraph TBL ["Delta Lake"]
            direction LR
            KS[("ktb_styles\nraw BP styles\n+ sample apps")]
            ST[("beproduct_to_dtc_staging\ndenormalized\nstyle × colour")]
            WIP[("dtc_wip_ktb\npulled DTC rows")]
            REG[("dtc_request_registry\n+ dtc_request_mapping")]
        end

        %% ---- Job tasks (execution order) ----
        subgraph JOB ["BeProduct_DTC_sync_dag  (daily, 8 tasks)"]
            direction TB
            T1["① bp_style_sync\nexclude Finalized\n+ sample enrichment"]
            T2["② transform\ndenormalize\nbrand=brand_hk"]
            T3["③ pull_dtc\n+ registry refresh"]
            T4["④ request_manager\ncreate + share"]
            T5["⑤ phase1_push\nBP→DTC upsert"]
            T6["⑥ phase2_push\nDTC→BP"]
            T7["⑦ repull_dtc\ntargeted"]
            T8["⑧ phase3_images\nfront image"]
        end
    end

%% ═══════════════════════════════════════════════════════════════════════════
%%  FORWARD  BeProduct → DTC  (Phase 1 fields + Phase 3 image)
%% ═══════════════════════════════════════════════════════════════════════════
    BPAPI  ==>|"① pull styles\nattributes_list + app_get"| T1
    T1     ==> KS
    KS     ==> T2
    T2     ==> ST
    ST     ==> T5
    REG    --> T5
    T5     ==>|"⑤ PATCH sheet\nUPDATE rowId / INSERT rowIndex\nBP Style#, Gender, Supplier,\nLegacy Code, LF Style#, +10"| DTCAPI
    ST     --> T8
    T8     ==>|"⑧ POST images (multipart)\nStyle Image, blank cells only"| DTCAPI

%% ═══════════════════════════════════════════════════════════════════════════
%%  PULL  DTC → Databricks  (Phase 1 read + registry)
%% ═══════════════════════════════════════════════════════════════════════════
    DTCAPI ==>|"③ get_sheet × 75\nsearch_requests"| T3
    T3     ==> WIP
    T3     ==> REG
    T4     -->|"④ POST /sheets + /shares"| DTCAPI
    REG    --> T4
    DTCAPI -->|"⑦ re-pull inserted"| T7
    T7     --> WIP

%% ═══════════════════════════════════════════════════════════════════════════
%%  REVERSE  DTC → BeProduct  (Phase 2)
%% ═══════════════════════════════════════════════════════════════════════════
    WIP    ==> T6
    ST     --> T6
    T6     ==>|"⑥ attributes_update\nMain Vendor / Factory (header)\nLot# (colorway)"| BPAPI

%% ---- parallel hint ----
    T1 -.->|parallel| T3

%% ═══════════════════════════════════════════════════════════════════════════
    classDef ext  fill:#dbeafe,stroke:#2563eb,color:#1e3a8a
    classDef tbl  fill:#f0fdf4,stroke:#16a34a,color:#14532d
    classDef task fill:#fefce8,stroke:#ca8a04,color:#713f12
    classDef api  fill:#eff6ff,stroke:#3b82f6,color:#1e3a8a

    class BP,DTCS ext
    class BPAPI,DTCAPI api
    class KS,ST,WIP,REG tbl
    class T1,T2,T3,T4,T5,T6,T7,T8 task
```

---

## Flow legend

| # | Task | Direction | Databricks table touched | External call |
|---|------|-----------|--------------------------|---------------|
| ① | `bp_style_sync` | BeProduct → ADB | write `ktb_styles` | `attributes_list`, `app_get`×6 |
| ② | `transform` | ADB → ADB | `ktb_styles` → `beproduct_to_dtc_staging` | — |
| ③ | `pull_dtc` | DTC → ADB | write `dtc_wip_ktb` + `registry` | `search_requests`, `get_sheet`×75 |
| ④ | `request_manager` | ADB → DTC | write `dtc_request_mapping` | `POST /sheets`, `POST /shares` |
| ⑤ | `phase1_push` | **BeProduct → DTC** | read `staging` + `mapping` | `PATCH /sheets/{id}/views/{id}` |
| ⑥ | `phase2_push` | **DTC → BeProduct** | read `dtc_wip_ktb` + `staging` | `attributes_update` |
| ⑦ | `repull_dtc` | DTC → ADB | refresh `dtc_wip_ktb` | `get_sheet` (inserted only) |
| ⑧ | `phase3_images` | **BeProduct → DTC** | read `dtc_wip_ktb` + `staging` | `POST .../images` (multipart) |

**Parallelism:** ①→② (BeProduct chain) runs in parallel with ③ (DTC pull); both converge at ④.

---

## Direction summary (one-way per field, no loops)

```
                       ┌─────────────────────────────┐
   BeProduct  ═══════▶ │        DATABRICKS           │ ═══════▶  DTC
   (Phase 1,3)         │  ktb_styles → staging       │  (⑤ PATCH, ⑧ images)
                       │                             │
   BeProduct  ◀═══════ │  dtc_wip_ktb ← DTC pull     │ ◀═══════  DTC
   (Phase 2)           │                             │  (③ get_sheet)
                       └─────────────────────────────┘
```

- **BeProduct → DTC** (Phase 1): Product Status, Style Description, Class, Sub Class,
  Division, Brand, Garment Finish, Tech Pack Stage, Fabric Group, Placement, Gender,
  BP Style# (match key), LF Style#, Legacy Code, Supplier (default-fill).
- **BeProduct → DTC** (Phase 3): Style Image (binary, blank cells only).
- **DTC → BeProduct** (Phase 2): Main Vendor (Sampling), Main Factory (Sampling), Lot#.
- **Match key:** `(BP Style#, Color / Wash)` in-request; `[Customer, BP Style#, SeasonCode, Brand]` composite.
- **Filter:** styles with Product Status = "Finalized" are excluded at ①.
```

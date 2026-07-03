# BeProduct ⇄ DTC Sync — Block Diagram

> High-level data-exchange architecture via Azure Databricks.
> Generated 2026-07-02.

```mermaid
flowchart TB

%% ─── External systems ────────────────────────────────────────────────────────

    subgraph BP ["☁  BeProduct  (Style PLM)"]
        direction TB
        BP_API["BeProduct Python SDK\nOAuth 2.0"]
        BP_STYLES["KTB Folder Styles\n───────────────\nBP Style Number  header_number\nLF Style Number  lf_style_number\nBrand            brand_hk\nCustomer Style#  customer_style_number\nProduct Status / Description\nClass / Sub Class / Division\nGarment Finish / Tech Pack Stage\nGender / Colorways / Front Image\n+ 6 Sample-app JSON arrays"]
        BP_API --- BP_STYLES
    end

    subgraph DTC ["☁  DTC  (Data Collab sheets)"]
        direction TB
        DTC_API["DTC REST API\nx-api-key"]
        DTC_WIP["WIP_ITS_USE View  (per Request)\n──────────────────────────────\nBP Style#  ← match key  ❶\nLF Style#  ← optional\nBrand / Color / Wash\nProduct Status / Style Description\nClass / Sub Class / Division\nGarment Finish / Tech Pack Stage\nFabric Group / Placement\nGender  ❶  /  Supplier  ❶\nLegacy Code / Style Image\nLot# / Main Vendor / Main Factory\nMain Factory Customer ID"]
        DTC_API --- DTC_WIP
    end

%% ─── Azure Databricks ────────────────────────────────────────────────────────

    subgraph ADB ["⚡  Azure Databricks  ·  lft.beproduct  (Unity Catalog)"]
        direction TB

        subgraph DELTA ["Delta Lake tables"]
            direction LR
            T_STYLES[("ktb_styles\n────────────\nbp_style_number\nlf_style_number\nbrand  brand_hk\ncustomer_style_number\ngender / product_status\ndivision / colorways_json\nfront_image_url\n6 sample_json cols\n…")]
            T_STAGING[("beproduct_to_dtc_staging\n────────────────────────\nbp_style_number  ← key\nbrand  season_code\nlf_style_number\ncustomer_style_number\ngender / supplier = 'Supplier'\ncolorway_id  fabric_group\nproduct_status / description\n…  beproduct_style_id\nsync_status")]
            T_WIP[("dtc_wip_ktb\n───────────\nrequest_reference\nbp_style_number\ncolor_wash  row_id\ndata_json")]
            T_REG[("dtc_request_registry\n────────────────────\nrequest_id  sheet_id\nview_id  season_code\nbrand  last_extracted")]
            T_MAP[("dtc_request_mapping\n───────────────────\ndtc_request_name\nrequest_id  sheet_id\nview_id")]
        end

        subgraph DAG ["Multi-task Job  BeProduct_DTC_sync_dag  (daily)"]
            direction TB
            S1["① bp_style_sync\n~75–130 s\nExclude status=Finalized\n+ sample-app enrichment"]
            S2["② transform\n~30–50 s\nDenormalize → staging\nseason code mapping\nbrand = brand_hk"]
            S3["③ pull_dtc\n~84 s\nPull all in-scope requests\n+ registry refresh"]
            S4["④ request_manager\n~13 s\nCreate + share missing\nDTC requests"]
            S5["⑤ phase1_push\n~14–60 s\nBeProduct → DTC upsert\nUPDATE / INSERT\northan marks"]
            S6["⑥ phase2_push\n~20–30 s\nDTC → BeProduct\nvendor / factory / lot#"]
            S7["⑦ repull_dtc\n~5–17 s\nTargeted re-pull\n(inserted requests only)"]
            S8["⑧ phase3_images\n~25–130 s\nFront image → DTC\nStyle Image cell"]
        end

    end

%% ─── Step 1 & 2 ──────────────────────────────────────────────────────────────
    BP_API  -->|"API pull\nattributes_list()\napp_get() × 6"| S1
    S1      -->|"overwrite\n(FULL daily)"| T_STYLES
    T_STYLES -->| read | S2
    S2      -->|"overwrite"| T_STAGING

%% ─── Step 3 ──────────────────────────────────────────────────────────────────
    DTC_API -->|"search_requests()\nget_sheet() × 75"| S3
    S3      -->|"overwrite"| T_WIP
    S3      -->|"merge"| T_REG

%% ─── Step 4 ──────────────────────────────────────────────────────────────────
    T_REG   --> S4
    S4      -->|"POST /sheets\nPOST /shares"| DTC_API
    S4      -->|"upsert"| T_MAP

%% ─── Step 5 Phase 1 ──────────────────────────────────────────────────────────
    T_STAGING --> S5
    T_MAP     --> S5
    S5 -->|"PATCH /sheets/{id}/views/{id}\nUPDATE rows  rowId\nINSERT rows  rowIndex"| DTC_API

%% ─── Step 6 Phase 2 ──────────────────────────────────────────────────────────
    T_WIP     --> S6
    T_STAGING --> S6
    S6 -->|"attributes_update()\nVendor / Factory\nLot# per colorway"| BP_API

%% ─── Step 7 re-pull ──────────────────────────────────────────────────────────
    DTC_API -->|"get_sheet()\n(inserted only)"| S7
    S7 -->|"replace rows"| T_WIP

%% ─── Step 8 Phase 3 ──────────────────────────────────────────────────────────
    T_WIP     --> S8
    T_STAGING --> S8
    S8 -->|"POST /sheets/{id}/views/{id}/images\nmultipart jpg/png\nblank cells only"| DTC_API

%% ─── Parallel DAG note ───────────────────────────────────────────────────────
    S1 -.->|"parallel"| S3
    S2 -->|"converge"| S4
    S3 -->|"converge"| S4

%% ─── Styles ──────────────────────────────────────────────────────────────────
    classDef external  fill:#dbeafe,stroke:#3b82f6,color:#1e3a8a
    classDef table     fill:#f0fdf4,stroke:#22c55e,color:#14532d
    classDef step      fill:#fefce8,stroke:#eab308,color:#713f12
    classDef dag       fill:#fff7ed,stroke:#f97316,color:#7c2d12

    class BP,DTC external
    class T_STYLES,T_STAGING,T_WIP,T_REG,T_MAP table
    class S1,S2,S3,S4,S5,S6,S7,S8 step
```

---

## Field direction summary

### BeProduct → DTC (Phase 1)

| Staging column | DTC column | Notes |
|---|---|---|
| `bp_style_number` | `BP Style#` | **match key** ❶ pending DTC col |
| `color` | `Color / Wash` | in-request match key |
| `brand` | `Brand` | from `brand_hk`; constant per request |
| `lf_style_number` | `LF Style#` | optional; upstream-filled |
| `customer_style_number` | `Legacy Code` | optional; upstream-filled |
| `product_status` | `Product Status` | excludes "Finalized" styles |
| `description` | `Style Description` | |
| `product_category` | `Class` | |
| `product_sub_category` | `Sub Class` | |
| `division` | `Division` | |
| `garment_finish` | `Garment Finish` | |
| `techpack_stage` | `Tech Pack Stage` | |
| `fabric_group` | `Fabric Group` | |
| `placement` | `Placement` | |
| `gender` | `Gender` | ❶ pending DTC col |
| `supplier` _(constant_ `"Supplier"`)  | `Supplier` | ❶ pending; default-fill only |
| `front_image_url` | `Style Image` | Phase 3 binary upload only |

### DTC → BeProduct (Phase 2)

| DTC column | BeProduct fieldId | Notes |
|---|---|---|
| `Main Vendor (Sampling)` | `parent_vendor` | header |
| `Main Factory (Sampling)` | `factory` | header |
| `Lot#` | `drawing_number_walmart` | colorway |
| `Main Factory Customer ID` | — | no BP target; skipped |

---

> ❶ Column not yet in DTC view — DTC admin must add.
> After `BP Style#` is added, existing `LF Style#` data must be migrated to `BP Style#`
> and the sync team notified to activate the new match key.

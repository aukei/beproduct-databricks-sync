# BeProduct ⇄ DTC Sync — Pipeline Data-Flow Diagram

> Databricks-centred view of all implemented sync pipelines.
> `BeProduct_DTC_sync_dag` — job 294837488757511. Updated 2026-07-18.

```mermaid
flowchart TB

%% ─── External systems ────────────────────────────────────────────────────────

    subgraph BP ["☁  BeProduct  (Style PLM)"]
        direction TB
        BP_API(["BeProduct SDK\nOAuth 2.0"])
    end

    subgraph DTC ["☁  DTC  (Data Collab  UAT/PROD)"]
        direction TB
        DTC_WIP(["KTB WIP\nWIP_ITS_USE  198 cols\nview 69f04983…"])
        DTC_FAB(["KTB FABRIC\nWIP_ITS_USE  120 cols\nview 6a0ac943…"])
        DTC_LP(["KTB LinePlan\nFull  30 cols\nview 69f07885…"])
    end

%% ─── Azure Databricks ────────────────────────────────────────────────────────

    subgraph ADB ["⚡  Azure Databricks  ·  lft.beproduct  (Unity Catalog)"]
        direction TB

        subgraph DELTA ["Delta Lake"]
            direction LR
            T_STYLES[("ktb_styles\nbp_style_number  lf_style_number\nbrand  gender  customer_style_number\n6 × sample_json  colorways_json\nFINALIZED excluded")]
            T_STAGING[("beproduct_to_dtc_staging\nbp_style_number ← key\nbrand  season_code  color\n6 × sample status cols\nsupplier='Supplier'\ncolorway_id  beproduct_style_id")]
            T_WIP[("dtc_wip_ktb\nbp_style_number  lf_style_number\ncolor_wash  row_id  data_json")]
            T_REG[("dtc_request_registry\n+ dtc_request_mapping")]
            T_FAB[("dtc_fabric_ktb\nlf_material_id  its_key\nmaterial_class  fabric_type\nfabric_content  mill_name\nAdoption=Y only")]
            T_LP[("dtc_lineplan_ktb\nlineplan_ref  projected_volume\ntarget_ldp  target_fob\ninternal_sourced")]
            T_CC[("costing_chart\nStyle × Color × Slot\nhts_code  duty_rate_*\ntariff_rate=NULL → Phase 9b")]
        end

        subgraph DAG ["BeProduct_DTC_sync_dag  (daily, multi-task)"]
            direction TB
            S1["① bp_style_sync\nexcl. Finalized\n+ sample enrichment"]
            S2["② transform\nbrand=brand_hk\nsample UDFs (Phase 7)"]
            S3["③ pull_master_dtc\n+ registry refresh"]
            S4["④ request_manager\ncreate + share"]
            S5["⑤ phase1_push\nBP→DTC Phases 1+7"]
            S6["⑥ phase2_push\nDTC→BP vendor/factory/lot"]
            S7["⑦ repull_dtc\ntargeted re-pull"]
            S8["⑧ phase3_images\nfront image binary"]
            S8A["⑧a pull_fabric_dtc\nPhase 8a  FABRIC\nAdoption=Y"]
            S9A1["⑨a₁ pull_lineplan_dtc\nPhase 9a  LinePlan"]
            S9A2["⑨a₂ build_costing_chart\nWIP × LinePlan join\n4-slot transpose"]
        end
    end

%% ─── WIP chain (Phases 1 / 2 / 3 / 7) ───────────────────────────────────────
    BP_API  ==>|"① pull styles\nattributes_list + app_get×6"| S1
    S1      ==> T_STYLES
    T_STYLES ==> S2
    S2      ==> T_STAGING
    DTC_WIP ==>|"③ get_sheet×75\nsearch_requests"| S3
    S3      ==> T_WIP
    S3      ==> T_REG
    T_STAGING --> S4
    T_REG   --> S4
    S4      -->|"POST /sheets\nPOST /shares"| DTC_WIP
    T_STAGING --> S5
    T_REG   --> S5
    S5      ==>|"⑤ PATCH  BP Style#/Color key\nPhase 1+7 fields\nSupplier default-fill"| DTC_WIP
    T_WIP   --> S6
    T_STAGING --> S6
    S6      ==>|"⑥ attributes_update\nVendor/Factory/Lot#"| BP_API
    DTC_WIP -->|"⑦ targeted re-pull"| S7
    S7      --> T_WIP
    T_WIP   --> S8
    T_STAGING --> S8
    S8      ==>|"⑧ POST images\nmultipart  blank cells only"| DTC_WIP

%% ─── Phase 8a  (parallel, independent) ──────────────────────────────────────
    DTC_FAB ==>|"search+get_sheet\nAdoption=Y filter\ninclude_test_sheets"| S8A
    S8A     ==> T_FAB

%% ─── Phase 9a  (parallel, independent) ──────────────────────────────────────
    DTC_LP  ==>|"search+get_sheet\nFull view fallback"| S9A1
    S9A1    ==> T_LP
    T_LP    --> S9A2
    T_WIP   --> S9A2
    S9A2    ==> T_CC

%% ─── Parallel hints ──────────────────────────────────────────────────────────
    S1 -.->|"parallel"| S3
    S2 -->|"converge"| S4
    S3 -->|"converge"| S4

%% ─── Styles ──────────────────────────────────────────────────────────────────
    classDef ext   fill:#dbeafe,stroke:#2563eb,color:#1e3a8a
    classDef table fill:#f0fdf4,stroke:#16a34a,color:#14532d
    classDef step  fill:#fefce8,stroke:#ca8a04,color:#713f12

    class BP,DTC ext
    class T_STYLES,T_STAGING,T_WIP,T_REG,T_FAB,T_LP,T_CC table
    class S1,S2,S3,S4,S5,S6,S7,S8,S8A,S9A1,S9A2 step
```

---

## Field direction summary

### BeProduct → DTC (Phase 1 + 7)

| Staging column | DTC column | Phase |
|---|---|---|
| `bp_style_number` | `BP Style#` — match key | 6 |
| `color` | `Color / Wash` — match key | |
| `brand` (brand_hk) | `Brand` — routing key | 6 |
| `product_status` | `Product Status` | 1 |
| `description` | `Style Description` | 1 |
| `product_category` | `Class` | 1 |
| `product_sub_category` | `Sub Class` | 1 |
| `division` | `Division` | 1 |
| `garment_finish` | `Garment Finish` | 1 |
| `techpack_stage` | `Tech Pack Stage` | 1 |
| `fabric_group` | `Fabric Group` | 1 |
| `placement` | `Placement` | 1 |
| `gender` | `Gender` | 6 |
| `lf_style_number` | `LF Style#` (optional) | 6 |
| `customer_style_number` | `Legacy Code` (optional) | 6 |
| `supplier` = `"Supplier"` | `Supplier` (default-fill) | 6 |
| `proto_sample_status` | `Proto Sample - Sample Status` | 7 |
| `preline_sample_status` | `Pre-line Sample - Status` | 7 |
| `sms_sample_status` | `SMS - Sample Status` | 7 |
| `fit_sample_status` | `1st Fit Sample Approval Status` | 7 |
| `pp_sample_status` | `2nd Fit Sample Approval Status` | 7 |
| `top_sample_status` | `TOP Sample Approval Status` | 7 |
| `front_image_url` | `Style Image` (Phase 3, binary) | 3 |

### DTC → BeProduct (Phase 2)

| DTC column | BP fieldId | Level |
|---|---|---|
| `Main Vendor (Sampling)` | `parent_vendor` | header |
| `Main Factory (Sampling)` | `factory` | header |
| `Lot#` | `drawing_number_walmart` | colorway |
| `Main Factory Customer ID` | — (skipped) | — |

### DTC FABRIC → Delta (Phase 8a, Adoption=Y)

`LF Material ID` → `lf_material_id` (BP Material Master key); `Fabric Content` → `fabric_content` (BP Material Description); `Material Class`, `Fabric Type`, `Mill Fabric Article #`, `Mill Name`, `KB Fabric Code (SAP Code)`.

### DTC LinePlan + WIP × LinePlan → Costing Chart (Phase 9a)

Join key: WIP `"Lineplan Ref #"` = LinePlan `"Lineplan Ref #"`.
Transpose: Main / Vendor 1 / Vendor 2 / Vendor 3 slots → one row each per vendor.
`tariff_rate = NULL` placeholder (Phase 9b NT Orbit Duty Tools).

---

> ❶ Pending DTC admin action: `BP Style#`, `Gender`, `Supplier` columns confirmed
> present in WIP_ITS_USE view (198 fields) but all cells blank — need data migration
> from `LF Style#` for `BP Style#` before match-key switch can activate.

-- lft.beproduct.v_data_gaps -- one row per (thing x gap), for the non-IT
-- "Data gaps" dashboard (dashboards/data_gaps/, deployed by
-- scripts/deploy_gap_dashboard.py).
--
-- Every check maps to a runbook in docs/TROUBLESHOOTING.md; `runbook` names it.
-- This view RE-DERIVES the pipeline's gates from the tables the pipeline leaves
-- behind. It is a v0: the long-term plan is for each stage to write its own
-- per-row drop reason, so this cannot drift from the real gates. Until then,
-- ANY change to a gate in PIPELINE.md must be mirrored here.
--
-- Freshness: every input is rewritten by the main job (every 15 min), so the
-- view is as fresh as the last main run. `as_of` is the DTC snapshot time.
--
-- Columns: area, gap_code, severity (blocker|warning|info), owner, request,
--          bp_style, color, article, detail, action, runbook, as_of
CREATE OR REPLACE VIEW lft.beproduct.v_data_gaps
COMMENT 'Data gaps per style/colour/material for the non-IT Data gaps dashboard. See dashboards/data_gaps/v_data_gaps.sql'
AS
WITH
-- ── DTC column names the PIPELINE reads: sync/lineplan.py LINEPLAN_REF_COLS,
--    current first. Keep in step -- a name missing here reads as NULL. ──────
names AS (
  SELECT array('LinePlan ref#', 'Lineplan Ref #') AS lineplan_ref_cols
),
wip AS (
  SELECT
    w.request_reference AS request,
    w.bp_style_number   AS bp_style,
    w.color_wash        AS color,
    w.row_index,
    w.data_json,
    get_json_object(w.data_json, "$['Fabric Group']")          AS fabric_group,
    get_json_object(w.data_json, "$['Mill Fabric Article #']") AS article,
    get_json_object(w.data_json, "$['Content']")               AS content,
    coalesce(nullif(trim(get_json_object(w.data_json, concat("$['", n.lineplan_ref_cols[0], "']"))), ''),
             nullif(trim(get_json_object(w.data_json, concat("$['", n.lineplan_ref_cols[1], "']"))), '')) AS lineplan_ref,
    get_json_object(w.data_json, "$['Main Vendor (Sampling)']")  AS v_main,
    get_json_object(w.data_json, "$['Main Factory (Sampling)']") AS f_main,
    get_json_object(w.data_json, "$['Factory Production Country for Main Factory']") AS c_main,
    get_json_object(w.data_json, "$['Main Factory HTS Code']")   AS h_main,
    get_json_object(w.data_json, "$['Vendor 1']")  AS v_1,
    get_json_object(w.data_json, "$['Factory 1']") AS f_1,
    get_json_object(w.data_json, "$['Factory Production Country for Factory 1']") AS c_1,
    get_json_object(w.data_json, "$['Factory 1 - HTS code']") AS h_1,
    get_json_object(w.data_json, "$['Vendor 2']")  AS v_2,
    get_json_object(w.data_json, "$['Factory 2']") AS f_2,
    get_json_object(w.data_json, "$['Factory Production Country for Factory 2']") AS c_2,
    get_json_object(w.data_json, "$['Factory 2 - HTS code']") AS h_2,
    get_json_object(w.data_json, "$['Vendor 3']")  AS v_3,
    get_json_object(w.data_json, "$['Factory 3']") AS f_3,
    get_json_object(w.data_json, "$['Factory Production Country for Factory 3']") AS c_3,
    get_json_object(w.data_json, "$['Factory 3 - HTS code']") AS h_3
  FROM lft.beproduct.dtc_wip_ktb w CROSS JOIN names n
),
as_of AS (SELECT max(extracted_at) AS as_of FROM lft.beproduct.dtc_wip_ktb),
stg AS (
  SELECT DISTINCT dtc_request_name AS request, bp_style_number AS bp_style, color, product_status
  FROM lft.beproduct.beproduct_to_dtc_staging
),
styles AS (
  SELECT bp_style_number AS bp_style, season, year, product_status
  FROM lft.beproduct.ktb_styles
),
resolved AS (SELECT DISTINCT dtc_request_name AS request FROM lft.beproduct.dtc_request_mapping),
bom AS (
  SELECT bp_style_number AS bp_style, main_fabric_count, fabric_count, error
  FROM lft.beproduct.bom_segments
),
-- Costing reads ONE row per style x colour: the Main Fabric row, else the
-- lowest rowIndex (PIPELINE.md Stage 30). DTC-owned inputs must be on it.
rep AS (
  SELECT * FROM (
    SELECT wip.*, row_number() OVER (
      PARTITION BY bp_style, color
      ORDER BY CASE WHEN trim(fabric_group) = 'Main Fabric' THEN 0 ELSE 1 END, row_index) AS rk
    FROM wip)
  WHERE rk = 1
),
slots AS (
  SELECT r.request, r.bp_style, r.color, r.article, s.slot, s.vendor, s.factory, s.country, s.hts
  FROM rep r
  LATERAL VIEW stack(4,
    'Main', r.v_main, r.f_main, r.c_main, r.h_main,
    '1',    r.v_1,    r.f_1,    r.c_1,    r.h_1,
    '2',    r.v_2,    r.f_2,    r.c_2,    r.h_2,
    '3',    r.v_3,    r.f_3,    r.c_3,    r.h_3) s AS slot, vendor, factory, country, hts
),
lp_refs AS (
  SELECT DISTINCT trim(lineplan_ref) AS ref FROM lft.beproduct.dtc_lineplan_ktb
  WHERE nullif(trim(lineplan_ref), '') IS NOT NULL
),
lp_conflicts AS (
  SELECT trim(lineplan_ref) AS ref FROM lft.beproduct.dtc_lineplan_ktb
  WHERE nullif(trim(lineplan_ref), '') IS NOT NULL
  GROUP BY 1
  HAVING count(DISTINCT coalesce(projected_volume, '~')) > 1
      OR count(DISTINCT coalesce(target_ldp, '~')) > 1
      OR count(DISTINCT coalesce(target_fob, '~')) > 1
),
-- The pipeline's Lineplan column is on NO row while a look-alike is present.
lp_renamed AS (
  SELECT count(*) AS cnt, concat_ws(', ', collect_set(k)) AS keys,
         count(*) > 0 AND NOT EXISTS (SELECT 1 FROM wip WHERE lineplan_ref IS NOT NULL) AS renamed
  FROM (SELECT explode(json_object_keys(data_json)) AS k FROM wip)
  WHERE lower(regexp_replace(k, '[^A-Za-z]', '')) LIKE '%lineplanref%'
    AND NOT array_contains((SELECT lineplan_ref_cols FROM names), k)
),
p2_last AS (
  SELECT run_id FROM lft.beproduct.dtc_to_beproduct_sync_log
  ORDER BY log_time DESC LIMIT 1
),
push_last AS (
  SELECT stage, max_by(run_id, log_time) AS run_id
  FROM lft.beproduct.beproduct_to_dtc_sync_log
  WHERE stage IN ('wip_push', 'images') AND log_time > current_timestamp() - INTERVAL 1 DAY
  GROUP BY stage
),
gaps AS (

  -- ═══ SYSTEM: a whole column the pipeline reads has vanished from DTC ═══════
  -- Catches a silent DTC rename: the pipeline's column is absent on EVERY row
  -- while a similarly-named one is present. Per-row LINEPLAN_REF_MISSING is
  -- suppressed meanwhile -- the users DID fill it in; the pipeline is blind.
  SELECT 'System' AS area, 'LINEPLAN_COLUMN_RENAMED' AS gap_code, 'blocker' AS severity,
         'IT' AS owner, NULL AS request, NULL AS bp_style, NULL AS color, NULL AS article,
         concat('No DTC row has any of ', concat_ws(' / ', n.lineplan_ref_cols), ', but ', r.cnt,
                ' rows have ', r.keys, '. The pipeline cannot see any Lineplan Ref, so NO costing line can form.') AS detail,
         'IT: add the new name to sync/lineplan.py LINEPLAN_REF_COLS and to the names CTE of this view.' AS action,
         '3' AS runbook
  FROM names n CROSS JOIN lp_renamed r
  WHERE r.renamed

  UNION ALL
  SELECT 'System', 'LINEPLAN_SHEET_HAS_NO_REFS', 'blocker', 'IT', NULL, NULL, NULL, NULL,
         'The LinePlan pull holds rows but none with a Lineplan Ref #. Either the LinePlan column was renamed or nobody has filled it in.',
         'IT: check the LinePlan column name first; otherwise the LinePlan owner fills Lineplan Ref #.',
         '3'
  WHERE EXISTS (SELECT 1 FROM lft.beproduct.dtc_lineplan_ktb)
    AND NOT EXISTS (SELECT 1 FROM lp_refs)

  UNION ALL
  SELECT 'System', 'COSTING_CHART_EMPTY', 'warning', 'IT', NULL, NULL, NULL, NULL,
         'costing_chart has 0 rows although DTC WIP has rows. See the Costing gaps below for why.',
         'Read the per-style Costing gaps. If none explain it, IT checks the build_costing exit value.',
         '3'
  WHERE EXISTS (SELECT 1 FROM wip) AND NOT EXISTS (SELECT 1 FROM lft.beproduct.costing_chart)

  -- ═══ WIP RECORD (runbook 1) ═══════════════════════════════════════════════
  UNION ALL
  SELECT 'WIP record', 'STYLE_FINALIZED_OR_DROPPED', 'info', 'BeProduct user', NULL, bp_style, NULL, NULL,
         concat('Product Status is ', product_status, ', so the style no longer syncs to DTC.'),
         'Nothing to do, unless it should still sync: then change the status in BeProduct.',
         '1.1'
  FROM styles WHERE product_status IN ('Finalized', 'Drop')

  UNION ALL
  SELECT 'WIP record', 'NO_SEASON_MAPPING', 'blocker', 'IT', NULL, s.bp_style, NULL, NULL,
         concat('BeProduct season "', s.season, ' ', s.year, '" has no DTC season code, so the style cannot be routed to a request.'),
         'IT: add the season to dtc_seasoncode_mapping (00_init_season_mapping).',
         '1.1'
  FROM styles s
  LEFT ANTI JOIN lft.beproduct.dtc_seasoncode_mapping m
    ON m.CUSTOMER = 'KTB' AND upper(trim(m.BPSEASON)) = upper(trim(s.season))

  UNION ALL
  SELECT 'WIP record', 'NOT_STAGED', 'blocker', 'IT', NULL, s.bp_style, NULL, NULL,
         'The style is in BeProduct but did not reach staging, so it cannot be sent to DTC.',
         'IT: check the transform task of the last main run.',
         '1.1'
  FROM styles s
  LEFT ANTI JOIN stg ON stg.bp_style = s.bp_style
  WHERE coalesce(s.product_status, '') NOT IN ('Finalized', 'Drop')
    AND EXISTS (SELECT 1 FROM lft.beproduct.dtc_seasoncode_mapping m
                WHERE m.CUSTOMER = 'KTB' AND upper(trim(m.BPSEASON)) = upper(trim(s.season)))

  UNION ALL
  SELECT 'WIP record', 'DTC_REQUEST_NOT_FOUND', 'blocker', 'DTC admin', stg.request, stg.bp_style, stg.color, NULL,
         concat('No active, in-scope DTC request is named "', stg.request, '".'),
         'DTC admin: create or reactivate the request, or remove cancel/backup/archive/delete/-SUPPLIER from its name. IT can see the exact reason in the sync log.',
         '1.2'
  FROM stg LEFT ANTI JOIN resolved r ON r.request = stg.request

  UNION ALL
  SELECT 'WIP record', 'NOT_YET_IN_DTC', 'warning', 'Wait / IT', stg.request, stg.bp_style, stg.color, NULL,
         'In BeProduct and routed to a request, but no DTC row yet.',
         'Wait for the next main run (15 min). If it is still missing after 30 min, contact IT.',
         '1.3'
  FROM stg
  JOIN resolved r ON r.request = stg.request
  LEFT ANTI JOIN wip ON wip.request = stg.request AND wip.bp_style = stg.bp_style AND wip.color = stg.color

  UNION ALL
  SELECT 'WIP record', 'ROW_NOT_IN_BEPRODUCT', 'warning', 'Data owner', w.request, w.bp_style, w.color, NULL,
         concat(count(*), ' DTC row(s) have a style/colour that BeProduct does not have (deleted colourway, or typed directly into DTC). The pipeline never updates or deletes them.'),
         'Data owner: add the colourway in BeProduct, or delete the DTC rows if they are obsolete.',
         '1.7'
  FROM wip w LEFT ANTI JOIN stg ON stg.bp_style = w.bp_style AND stg.color = w.color
  GROUP BY w.request, w.bp_style, w.color

  -- ═══ MATERIAL ROWS (runbook 1.8) ═════════════════════════════════════════
  UNION ALL
  SELECT 'Material rows', 'NO_TECHPACK_BOM', 'blocker', 'Techpack team', NULL, s.bp_style, NULL, NULL,
         'No techpack BOM for this style, so DTC shows Fabric Group "NO TPM BOM" and no costing line can form.',
         'Techpack team: complete the BOM extraction for this style.',
         '1.8'
  FROM (SELECT DISTINCT bp_style FROM stg) s LEFT ANTI JOIN bom b ON b.bp_style = s.bp_style

  UNION ALL
  SELECT 'Material rows', 'BOM_HAS_NO_MAIN_FABRIC', 'blocker', 'Techpack team', NULL, bp_style, NULL, NULL,
         concat('The BOM has ', main_fabric_count, ' "Main Fabric" segments; exactly 1 is required. Nothing is enriched for the whole style.'),
         'Techpack team: mark exactly one BOM line as Main Fabric.',
         '1.8'
  FROM bom WHERE coalesce(main_fabric_count, 0) <> 1

  UNION ALL
  SELECT 'Material rows', 'BOM_UNREADABLE', 'blocker', 'Techpack team', NULL, bp_style, NULL, NULL,
         concat('The BOM could not be read: ', error),
         'Techpack team: fix the BOM data; IT can help with the error text.',
         '1.8'
  FROM bom WHERE nullif(trim(error), '') IS NOT NULL

  UNION ALL
  SELECT 'Material rows', 'MATERIAL_ROWS_MISSING', 'warning', 'Wait / IT', w.request, w.bp_style, w.color, NULL,
         concat('DTC has ', count(*), ' material row(s) for this colour; the BOM implies ', 1 + b.fabric_count, '.'),
         'Wait for the next main run (15 min). If it persists, contact IT.',
         '1.8'
  FROM wip w JOIN bom b ON b.bp_style = w.bp_style AND b.main_fabric_count = 1
  GROUP BY w.request, w.bp_style, w.color, b.fabric_count
  HAVING count(*) < 1 + b.fabric_count

  -- ═══ COSTING (runbook 3) -- checked on the Main Fabric row only ══════════
  UNION ALL
  SELECT 'Costing', 'CONTENT_BLANK', 'blocker', 'Techpack team', request, bp_style, color, article,
         'Content is blank on the Main Fabric row, so no costing line can form.',
         'Techpack team: fill Material Content in the BOM; or type Content on the Main Fabric row in DTC.',
         '3'
  FROM rep WHERE trim(fabric_group) = 'Main Fabric' AND nullif(trim(content), '') IS NULL

  UNION ALL
  SELECT 'Costing', 'LINEPLAN_REF_MISSING', 'blocker', 'DTC user', request, bp_style, color, article,
         'Lineplan Ref # is blank on the Main Fabric row, so no costing line can form.',
         'DTC user: enter Lineplan Ref # on the Main Fabric row (not on the Fabric rows).',
         '3'
  FROM rep WHERE trim(fabric_group) = 'Main Fabric' AND nullif(trim(lineplan_ref), '') IS NULL
    AND NOT (SELECT renamed FROM lp_renamed)

  UNION ALL
  SELECT 'Costing', 'LINEPLAN_REF_NOT_IN_LINEPLAN', 'blocker', 'DTC user', rep.request, rep.bp_style, rep.color, rep.article,
         concat('Lineplan Ref # "', trim(rep.lineplan_ref), '" does not exist in any active LinePlan request.'),
         'DTC user: check the ref for typos, or ask the LinePlan owner to add it.',
         '3'
  FROM rep LEFT ANTI JOIN lp_refs ON lp_refs.ref = trim(rep.lineplan_ref)
  WHERE trim(rep.fabric_group) = 'Main Fabric' AND nullif(trim(rep.lineplan_ref), '') IS NOT NULL

  UNION ALL
  SELECT 'Costing', 'LINEPLAN_REF_DUPLICATED', 'warning', 'LinePlan owner', rep.request, rep.bp_style, rep.color, rep.article,
         concat('Lineplan Ref # "', trim(rep.lineplan_ref), '" appears more than once in LinePlan with different quantity/LDP/FOB. The value used is arbitrary.'),
         'LinePlan owner: make the ref unique, or make the duplicate rows agree.',
         '3'
  FROM rep JOIN lp_conflicts c ON c.ref = trim(rep.lineplan_ref)
  WHERE trim(rep.fabric_group) = 'Main Fabric'

  UNION ALL
  SELECT 'Costing', 'NO_VENDOR', 'blocker', 'DTC user', request, bp_style, color, article,
         'No vendor on the Main Fabric row (Main Vendor (Sampling) and Vendor 1-3 all blank), so no costing line can form.',
         'DTC user: enter the vendor on the Main Fabric row.',
         '3'
  FROM rep
  WHERE trim(fabric_group) = 'Main Fabric'
    AND coalesce(nullif(trim(v_main), ''), nullif(trim(v_1), ''),
                 nullif(trim(v_2), ''), nullif(trim(v_3), '')) IS NULL

  -- ═══ DUTY (runbook 4) -- per vendor slot on the Main Fabric row ═════════
  UNION ALL
  SELECT 'Duty', 'FACTORY_MISSING', 'blocker', 'DTC user', request, bp_style, color, article,
         concat('Slot ', slot, ': vendor ', vendor, ' has no factory. Production country follows the factory, so no duty rate can be looked up.'),
         concat('DTC user: enter ', CASE slot WHEN 'Main' THEN 'Main Factory (Sampling)' ELSE concat('Factory ', slot) END, ' on the Main Fabric row.'),
         '4.1'
  FROM slots
  WHERE nullif(trim(vendor), '') IS NOT NULL AND nullif(trim(factory), '') IS NULL

  UNION ALL
  SELECT 'Duty', 'COUNTRY_NOT_STORED', 'blocker', 'DTC admin', request, bp_style, color, article,
         concat('Slot ', slot, ': factory ', factory, ' is set but its production country is not stored in DTC, so no duty rate can be looked up.'),
         'DTC admin: put the "Factory Production Country for ..." columns on the Full view; the user then re-saves the row. (A lookup column is only stored if it is on the view being saved.)',
         '2.2 / 4.1'
  FROM slots
  WHERE nullif(trim(factory), '') IS NOT NULL AND nullif(trim(country), '') IS NULL

  UNION ALL
  SELECT 'Duty', 'DUTY_PENDING', 'warning', 'Wait / IT', NULL, c.bp_style_no, c.color_name, c.material_no,
         concat('Slot ', c.supplier_type, ' (', c.supplier, ' / ', c.production_country, '): ',
                concat_ws(', ',
                  CASE WHEN nullif(trim(c.hts_code), '') IS NULL THEN 'HTS' END,
                  CASE WHEN c.duty_rate_us IS NULL THEN 'duty US' END,
                  CASE WHEN c.duty_rate_ca IS NULL THEN 'duty CA' END,
                  CASE WHEN c.duty_rate_mx IS NULL THEN 'duty MX' END,
                  CASE WHEN c.tariff_rate IS NULL THEN 'tariff' END),
                ' still blank.'),
         'Wait: the duty lookup runs every 15 min, and each new line takes ~1-2 min. If it is still blank after 1 hour, contact IT.',
         '4.1'
  FROM lft.beproduct.costing_chart c
  WHERE nullif(trim(c.production_country), '') IS NOT NULL
    AND (nullif(trim(c.hts_code), '') IS NULL OR c.duty_rate_us IS NULL OR c.duty_rate_ca IS NULL
         OR c.duty_rate_mx IS NULL OR c.tariff_rate IS NULL)

  UNION ALL
  SELECT 'Duty', 'DUTY_NOT_YET_IN_DTC', 'info', 'Wait / IT', s.request, s.bp_style, s.color, s.article,
         concat('Slot ', s.slot, ': HTS ', c.hts_code, ' is computed but not yet in the DTC sheet.'),
         'Wait for the next main run (15 min). If it persists, contact IT.',
         '4.2'
  FROM lft.beproduct.costing_chart c
  JOIN slots s ON s.bp_style = c.bp_style_no AND s.color = c.color_name AND s.slot = c.supplier_type
  WHERE nullif(trim(c.hts_code), '') IS NOT NULL AND nullif(trim(s.hts), '') IS NULL

  -- ═══ DTC -> BEPRODUCT (runbook 2) -- last phase2 run ═════════════════════
  UNION ALL
  SELECT 'To BeProduct', 'ROWS_DISAGREE', 'warning', 'DTC user', NULL, c.bp_style, NULL, NULL,
         concat(c.field, ' has ', c.n_values, ' different values across this style''s DTC rows: ',
                c.values_and_rows,
                CASE WHEN c.beproduct_now IS NOT NULL THEN concat('.  BeProduct now holds: ', c.beproduct_now) ELSE '' END,
                '.  It is ONE value per style in BeProduct, so it gets whichever row comes first.'),
         concat('DTC user: make every row of this style hold the same ', c.field,
                ', or blank it on all rows but one (blanks are ignored).'),
         '2.3'
  FROM lft.beproduct.v_style_field_conflicts c

  UNION ALL
  SELECT 'To BeProduct', upper(l.reason), 'warning',
         CASE l.reason WHEN 'missing_colorway_id' THEN 'BeProduct user' ELSE 'IT' END,
         l.dtc_request_name, l.lf_style_number, l.color, NULL,
         coalesce(l.detail, l.reason),
         CASE l.reason
           WHEN 'missing_colorway_id' THEN 'BeProduct user: create this colourway in BeProduct (Lot# needs a real colourway).'
           WHEN 'no_beproduct_identity' THEN 'Data owner: this DTC row has no matching BeProduct style/colour (see "ROW_NOT_IN_BEPRODUCT").'
           ELSE 'IT: see the phase2 log for this style.' END,
         '2.3'
  FROM (SELECT DISTINCT dtc_request_name, lf_style_number, color, reason, detail
        FROM lft.beproduct.dtc_to_beproduct_sync_log
        WHERE run_id = (SELECT run_id FROM p2_last) AND status = 'error'
          AND reason <> 'header_value_conflict') l

  -- ═══ PUSH / IMAGE errors -- last run of each ═════════════════════════════
  UNION ALL
  SELECT CASE l.stage WHEN 'images' THEN 'Style image' ELSE 'WIP record' END,
         upper(coalesce(nullif(l.reason, ''), l.operation)),
         CASE WHEN l.status = 'error' THEN 'warning' ELSE 'info' END,
         CASE WHEN l.reason IN ('no_source_image', 'unsupported_type') THEN 'BeProduct user' ELSE 'IT' END,
         l.dtc_request_name, l.lf_style_number, l.color, NULL,
         concat(count(*), ' row(s): ', coalesce(max(substr(l.detail, 1, 200)), l.reason)),
         CASE l.reason
           WHEN 'no_source_image'  THEN 'BeProduct user: add a front image to the style.'
           WHEN 'unsupported_type' THEN 'BeProduct user: use a JPG or PNG front image.'
           ELSE 'IT: see the sync log for this style.' END,
         CASE l.stage WHEN 'images' THEN '6' ELSE '1.3' END
  FROM lft.beproduct.beproduct_to_dtc_sync_log l
  JOIN push_last p ON p.stage = l.stage AND p.run_id = l.run_id
  WHERE l.status = 'error' OR l.reason IN ('no_source_image', 'unsupported_type')
     OR l.operation IN ('EXCEPTION')
  GROUP BY l.stage, l.reason, l.operation, l.status, l.dtc_request_name, l.lf_style_number, l.color
)
SELECT g.*, a.as_of
FROM gaps g CROSS JOIN as_of a

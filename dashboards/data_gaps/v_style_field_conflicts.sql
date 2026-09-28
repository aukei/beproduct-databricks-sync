-- lft.beproduct.v_style_field_conflicts -- one row per (style x STYLE-LEVEL
-- field) whose DTC rows disagree. Summary behind phase2's
-- `header_value_conflict` errors, for the "Data gaps" dashboard.
--
-- Why these conflict: the four DTC -> BeProduct HEADER fields below are ONE
-- value per style in BeProduct, but DTC has one row per style x colour x
-- material. phase2 takes the first value it meets and flags the rest
-- (sync/phase2.py, build_beproduct_updates), so BeProduct gets an arbitrary
-- one. Blank cells are ignored, exactly as phase2 ignores them.
--
-- The field list MUST equal sync/phase2.py REVERSE_HEADER_FIELDS.
-- Values are compared trimmed + whitespace-collapsed, like phase1.norm().
-- Deployed by scripts/deploy_gap_dashboard.py (before v_data_gaps, which reads it).
CREATE OR REPLACE VIEW lft.beproduct.v_style_field_conflicts
COMMENT 'Style-level DTC->BeProduct fields whose DTC rows disagree (phase2 header_value_conflict), one row per style x field'
AS
WITH
fields AS (
  SELECT * FROM VALUES
    ('Main Vendor (Sampling)',                      'parent_vendor'),
    ('Main Factory (Sampling)',                     'factory'),
    ('Main Factory Customer ID',                    'customer_factory_code'),
    ('Factory Production Country for Main Factory', 'country_of_origin')
  AS t(field, beproduct_field)
),
cells AS (
  SELECT w.bp_style_number AS bp_style, w.color_wash AS color,
         coalesce(get_json_object(w.data_json, "$['Fabric Group']"), '?') AS fabric_group,
         f.field, f.beproduct_field,
         regexp_replace(trim(get_json_object(w.data_json, concat("$['", f.field, "']"))), '[ \\t]+', ' ') AS value
  FROM lft.beproduct.dtc_wip_ktb w CROSS JOIN fields f
  WHERE w.bp_style_number IS NOT NULL
),
by_value AS (
  SELECT bp_style, field, beproduct_field, value,
         count(*) AS n_rows,
         concat_ws(', ', array_sort(collect_list(concat(color, ' / ', fabric_group)))) AS rows_with_value
  FROM cells
  WHERE nullif(value, '') IS NOT NULL
  GROUP BY bp_style, field, beproduct_field, value
),
bp_now AS (
  -- ktb_styles stores a DropDown as its Python repr ({'text':..,'value':..});
  -- the 'value' is what DTC holds and what phase2 writes.
  SELECT bp_style_number AS bp_style, 'parent_vendor' AS beproduct_field,
         regexp_extract(parent_vendor, "'value': '([^']*)'", 1) AS bp_value
  FROM lft.beproduct.ktb_styles
  UNION ALL
  SELECT bp_style_number, 'factory', regexp_extract(factory, "'value': '([^']*)'", 1)
  FROM lft.beproduct.ktb_styles
)
SELECT v.bp_style, v.field,
       count(*) AS n_values,
       sum(v.n_rows) AS n_rows,
       concat_ws('  |  ', array_sort(collect_list(
         concat(v.value, '  <-  ', v.rows_with_value)))) AS values_and_rows,
       max(nullif(b.bp_value, '')) AS beproduct_now
FROM by_value v
LEFT JOIN bp_now b ON b.bp_style = v.bp_style AND b.beproduct_field = v.beproduct_field
GROUP BY v.bp_style, v.field
HAVING count(*) > 1

/*
Read-only preflight checks for dbo.pipeline_control in the Uber Eats project.

Run against the configured SQL source before enabling a control row. These checks
validate obvious metadata problems; they do NOT replace an approved table/column
allowlist, permissions review, source-specific watermark contract, or ADF runtime test.

Assumptions based on the exported pipeline:
  control_id, table_name, watermark_column, last_watermark_value
  source tables and columns are referenced as dbo.<identifier>
*/

-- 1. Required metadata fields and invalid simple SQL identifiers.
-- The current pipeline builds dbo.<table_name> and interpolates watermark_column.
SELECT
    control_id,
    table_name,
    watermark_column,
    last_watermark_value,
    CASE
        WHEN control_id IS NULL THEN 'missing control_id'
        WHEN NULLIF(LTRIM(RTRIM(table_name)), '') IS NULL THEN 'missing table_name'
        WHEN NULLIF(LTRIM(RTRIM(watermark_column)), '') IS NULL THEN 'missing watermark_column'
        WHEN table_name LIKE '%[^A-Za-z0-9_]%' THEN 'table_name contains unsupported characters'
        WHEN watermark_column LIKE '%[^A-Za-z0-9_]%' THEN 'watermark_column contains unsupported characters'
        WHEN last_watermark_value IS NULL THEN 'missing watermark; initial-load policy required'
        ELSE 'review'
    END AS preflight_issue
FROM dbo.pipeline_control
WHERE control_id IS NULL
   OR NULLIF(LTRIM(RTRIM(table_name)), '') IS NULL
   OR NULLIF(LTRIM(RTRIM(watermark_column)), '') IS NULL
   OR table_name LIKE '%[^A-Za-z0-9_]%'
   OR watermark_column LIKE '%[^A-Za-z0-9_]%'
   OR last_watermark_value IS NULL;

-- 2. Duplicate control identifiers.
SELECT control_id, COUNT(*) AS row_count
FROM dbo.pipeline_control
GROUP BY control_id
HAVING control_id IS NULL OR COUNT(*) > 1;

-- 3. Duplicate source mappings. Multiple control rows for the same source
-- may cause repeated extraction unless explicitly intended and documented.
SELECT table_name, watermark_column, COUNT(*) AS row_count
FROM dbo.pipeline_control
GROUP BY table_name, watermark_column
HAVING COUNT(*) > 1;

-- 4. Human-reviewed allowlist check.
-- Compare every returned table/column pair to a separately approved source
-- registry before running the ADF pipeline. A character check is NOT an
-- authorization boundary: a syntactically valid but unapproved identifier
-- must still be rejected by deployment controls.
SELECT control_id, table_name, watermark_column
FROM dbo.pipeline_control
ORDER BY control_id;


-- 5. Confirm each configured dbo source table and watermark column exists.
-- This is a catalog check, not an allowlist: an existing object may still be
-- outside the set of sources approved for this pipeline.
SELECT
    pc.control_id,
    pc.table_name,
    pc.watermark_column,
    CASE
        WHEN t.object_id IS NULL THEN 'source table not found in dbo'
        WHEN c.column_id IS NULL THEN 'watermark column not found on source table'
        ELSE 'object exists; verify watermark type and source semantics'
    END AS catalog_check,
    ty.name AS watermark_data_type
FROM dbo.pipeline_control AS pc
LEFT JOIN sys.schemas AS s
    ON s.name = 'dbo'
LEFT JOIN sys.tables AS t
    ON t.schema_id = s.schema_id
   AND t.name = pc.table_name
LEFT JOIN sys.columns AS c
    ON c.object_id = t.object_id
   AND c.name = pc.watermark_column
LEFT JOIN sys.types AS ty
    ON ty.user_type_id = c.user_type_id
WHERE t.object_id IS NULL
   OR c.column_id IS NULL
   OR c.column_id IS NOT NULL
ORDER BY pc.control_id;

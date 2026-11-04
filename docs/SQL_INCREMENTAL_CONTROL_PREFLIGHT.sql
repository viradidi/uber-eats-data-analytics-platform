/*
Read-only preflight checks for dbo.pipeline_control in the Uber Eats project.

The ADF SQL pipeline processes only rows with is_active = 1 and fails with
SQL_NO_ACTIVE_CONTROL_ROWS when there are no enabled rows. Run these checks
before enabling a control row. They do NOT replace an approved source allowlist,
permissions review, source-specific watermark contract, or ADF runtime test.

Control metadata used by the exported pipeline:
  control_id, schema_name, table_name, watermark_column,
  last_watermark_value, is_active

Source objects are addressed using schema_name, table_name, and watermark_column.
*/

-- 0. Activation must be explicit; NULL is neither enabled nor an approved state.
SELECT control_id, source_name, is_active
FROM dbo.pipeline_control
WHERE is_active IS NULL;

-- 1. Required metadata and identifier checks for enabled rows.
SELECT
    control_id,
    schema_name,
    table_name,
    watermark_column,
    last_watermark_value,
    is_active,
    CASE
        WHEN control_id IS NULL THEN 'missing control_id'
        WHEN NULLIF(LTRIM(RTRIM(schema_name)), '') IS NULL THEN 'missing schema_name'
        WHEN NULLIF(LTRIM(RTRIM(table_name)), '') IS NULL THEN 'missing table_name'
        WHEN NULLIF(LTRIM(RTRIM(watermark_column)), '') IS NULL THEN 'missing watermark_column'
        WHEN schema_name LIKE '%[^A-Za-z0-9_]%' OR LEFT(schema_name, 1) NOT LIKE '[A-Za-z_]' THEN 'schema_name contains unsupported characters'
        WHEN table_name LIKE '%[^A-Za-z0-9_]%' OR LEFT(table_name, 1) NOT LIKE '[A-Za-z_]' THEN 'table_name contains unsupported characters'
        WHEN watermark_column LIKE '%[^A-Za-z0-9_]%' OR LEFT(watermark_column, 1) NOT LIKE '[A-Za-z_]' THEN 'watermark_column contains unsupported characters'
        WHEN last_watermark_value IS NULL THEN 'missing watermark; initial-load policy required'
        ELSE 'review'
    END AS preflight_issue
FROM dbo.pipeline_control
WHERE is_active = 1
  AND (
       control_id IS NULL
    OR NULLIF(LTRIM(RTRIM(schema_name)), '') IS NULL
    OR NULLIF(LTRIM(RTRIM(table_name)), '') IS NULL
    OR NULLIF(LTRIM(RTRIM(watermark_column)), '') IS NULL
    OR schema_name LIKE '%[^A-Za-z0-9_]%'
    OR LEFT(schema_name, 1) NOT LIKE '[A-Za-z_]'
    OR table_name LIKE '%[^A-Za-z0-9_]%'
    OR LEFT(table_name, 1) NOT LIKE '[A-Za-z_]'
    OR watermark_column LIKE '%[^A-Za-z0-9_]%'
    OR LEFT(watermark_column, 1) NOT LIKE '[A-Za-z_]'
    OR last_watermark_value IS NULL
  );

-- 2. Duplicate control identifiers. This applies to all rows, enabled or not.
SELECT control_id, COUNT(*) AS row_count
FROM dbo.pipeline_control
GROUP BY control_id
HAVING control_id IS NULL OR COUNT(*) > 1;

-- 3. Duplicate enabled source mappings. Multiple active rows for the same
-- source/cursor can cause repeated extraction unless explicitly intended.
SELECT schema_name, table_name, watermark_column, COUNT(*) AS row_count
FROM dbo.pipeline_control
WHERE is_active = 1
GROUP BY schema_name, table_name, watermark_column
HAVING COUNT(*) > 1;

-- 4. Human-reviewed allowlist check for enabled sources.
-- Syntax checks are NOT an authorization boundary. Compare every returned
-- schema/table/column pair to a separately approved source registry.
SELECT control_id, source_name, schema_name, table_name, watermark_column
FROM dbo.pipeline_control
WHERE is_active = 1
ORDER BY control_id;

-- 5. Source-object catalog checks must run in the SOURCE SQL database.
-- The control database may be separate from the source database after restore;
-- querying sys.schemas/sys.tables here would inspect the wrong database.
-- Use sql/diagnostics/inspect_source_contract_candidates.sql after copying the
-- approved active control rows into its @ActiveSources table variable.

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

-- 5. Catalog check for enabled source objects. Existence is not authorization;
-- also verify the watermark type and documented change semantics.
SELECT
    pc.control_id,
    pc.schema_name,
    pc.table_name,
    pc.watermark_column,
    CASE
        WHEN t.object_id IS NULL THEN 'source table not found in configured schema'
        WHEN c.column_id IS NULL THEN 'watermark column not found on source table'
        ELSE 'object exists; verify watermark type and source semantics'
    END AS catalog_check,
    ty.name AS watermark_data_type
FROM dbo.pipeline_control AS pc
LEFT JOIN sys.schemas AS s
    ON s.name = pc.schema_name
LEFT JOIN sys.tables AS t
    ON t.schema_id = s.schema_id
   AND t.name = pc.table_name
LEFT JOIN sys.columns AS c
    ON c.object_id = t.object_id
   AND c.name = pc.watermark_column
LEFT JOIN sys.types AS ty
    ON ty.user_type_id = c.user_type_id
WHERE pc.is_active = 1
ORDER BY pc.control_id;

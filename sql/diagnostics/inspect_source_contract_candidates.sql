/*
    Uber Eats SQL source-contract discovery (READ ONLY).

    Run this script in the SOURCE SQL database used by ls_sql_database.
    If source and control are separate, do not run it against the control DB.
    Export enabled rows from dbo.pipeline_control in the control database and
    paste them into the in-memory @ActiveSources table below. No permanent data
    is modified by this script.

    Control DB export:
      SELECT control_id, schema_name, table_name, watermark_column,
             last_watermark_value
      FROM dbo.pipeline_control
      WHERE is_active = 1
      ORDER BY control_id;
*/

SET NOCOUNT ON;
DECLARE @CollectRowStats bit = 0; -- Set to 1 only after approving source-table scan cost.

DECLARE @ActiveSources TABLE
(
    control_id int NOT NULL,
    schema_name sysname NOT NULL,
    table_name sysname NOT NULL,
    watermark_column sysname NOT NULL,
    last_watermark_value datetime2(7) NULL
);

/*
INSERT INTO @ActiveSources
    (control_id, schema_name, table_name, watermark_column, last_watermark_value)
VALUES
    (<control_id>, N'<schema_name>', N'<table_name>', N'<watermark_column>',
     CONVERT(datetime2(7), N'<checkpoint ISO 8601 value>', 126));
*/

IF NOT EXISTS (SELECT 1 FROM @ActiveSources)
    THROW 51200, 'Populate @ActiveSources with approved active control rows from the control database.', 1;

IF EXISTS
(
    SELECT 1 FROM @ActiveSources
    WHERE NULLIF(LTRIM(RTRIM(schema_name)), N'') IS NULL
       OR NULLIF(LTRIM(RTRIM(table_name)), N'') IS NULL
       OR NULLIF(LTRIM(RTRIM(watermark_column)), N'') IS NULL
       OR schema_name LIKE N'%[^A-Za-z0-9_]%'
       OR table_name LIKE N'%[^A-Za-z0-9_]%'
       OR watermark_column LIKE N'%[^A-Za-z0-9_]%'
       OR LEFT(schema_name, 1) NOT LIKE N'[A-Za-z_]'
       OR LEFT(table_name, 1) NOT LIKE N'[A-Za-z_]'
       OR LEFT(watermark_column, 1) NOT LIKE N'[A-Za-z_]'
)
    THROW 51201, 'The supplied source metadata contains an empty or unsupported identifier.', 1;

IF EXISTS (SELECT control_id FROM @ActiveSources GROUP BY control_id HAVING COUNT_BIG(*) > 1)
    THROW 51202, 'The supplied source inventory contains a duplicate control_id.', 1;

-- Result set 1: verify source tables and watermark columns in THIS database.
SELECT
    src.control_id, src.schema_name, src.table_name, src.watermark_column,
    src.last_watermark_value, t.object_id AS source_object_id,
    c.column_id AS watermark_column_id, ty.name AS watermark_data_type,
    c.precision AS watermark_precision, c.scale AS watermark_scale,
    c.is_nullable AS watermark_is_nullable,
    CASE
        WHEN t.object_id IS NULL THEN 'SOURCE_TABLE_NOT_FOUND_IN_THIS_DATABASE'
        WHEN c.column_id IS NULL THEN 'WATERMARK_COLUMN_NOT_FOUND'
        WHEN ty.name NOT IN ('datetime', 'datetime2', 'smalldatetime') THEN 'UNSUPPORTED_WATERMARK_TYPE_FOR_CURRENT_ADF_PIPELINE'
        WHEN c.is_computed = 1 THEN 'COMPUTED_WATERMARK_REQUIRES_REVIEW'
        ELSE 'CATALOG_MATCH_REQUIRES_SEMANTIC_APPROVAL'
    END AS catalog_status
FROM @ActiveSources AS src
LEFT JOIN sys.schemas AS s ON s.name = src.schema_name
LEFT JOIN sys.tables AS t ON t.schema_id = s.schema_id AND t.name = src.table_name
LEFT JOIN sys.columns AS c ON c.object_id = t.object_id AND c.name = src.watermark_column
LEFT JOIN sys.types AS ty ON ty.user_type_id = c.user_type_id
ORDER BY src.control_id;

-- Result set 2: primary/unique index columns are candidate keys only.
SELECT
    src.control_id, src.schema_name, src.table_name,
    ix.name AS unique_index_name, ix.is_primary_key, ix.is_unique_constraint,
    ix.has_filter, ix.filter_definition, ixc.key_ordinal,
    c.name AS candidate_key_column
FROM @ActiveSources AS src
JOIN sys.schemas AS s ON s.name = src.schema_name
JOIN sys.tables AS t ON t.schema_id = s.schema_id AND t.name = src.table_name
JOIN sys.indexes AS ix
    ON ix.object_id = t.object_id AND ix.is_unique = 1
   AND ix.is_disabled = 0 AND ix.is_hypothetical = 0
JOIN sys.index_columns AS ixc
    ON ixc.object_id = ix.object_id AND ixc.index_id = ix.index_id
   AND ixc.key_ordinal > 0
JOIN sys.columns AS c ON c.object_id = ixc.object_id AND c.column_id = ixc.column_id
ORDER BY src.control_id, ix.name, ixc.key_ordinal;

-- Identify source tables with no primary/unique index candidate.
SELECT src.control_id, src.schema_name, src.table_name,
       'NO_PRIMARY_OR_UNIQUE_INDEX_CANDIDATE' AS key_inventory_status
FROM @ActiveSources AS src
JOIN sys.schemas AS s ON s.name = src.schema_name
JOIN sys.tables AS t ON t.schema_id = s.schema_id AND t.name = src.table_name
WHERE NOT EXISTS
(
    SELECT 1 FROM sys.indexes AS ix
    WHERE ix.object_id = t.object_id AND ix.is_unique = 1
      AND ix.is_disabled = 0 AND ix.is_hypothetical = 0
)
ORDER BY src.control_id;

-- Optional result set 3: table scans are disabled by default for large tables.
IF @CollectRowStats = 1
BEGIN
    DECLARE @Stats TABLE
    (
        control_id int NOT NULL, total_rows bigint NULL, null_cursor_rows bigint NULL,
        minimum_cursor nvarchar(48) NULL, maximum_cursor nvarchar(48) NULL,
        rows_at_maximum_cursor bigint NULL, rows_at_or_before_checkpoint bigint NULL
    );

    DECLARE
        @control_id int, @schema_name sysname, @table_name sysname,
        @watermark_column sysname, @checkpoint datetime2(7),
        @qualified_table nvarchar(520), @quoted_watermark nvarchar(258),
        @sql nvarchar(max), @total_rows bigint, @null_cursor_rows bigint,
        @minimum_cursor nvarchar(48), @maximum_cursor nvarchar(48),
        @rows_at_maximum_cursor bigint, @rows_at_or_before_checkpoint bigint;

    DECLARE source_cursor CURSOR LOCAL FAST_FORWARD FOR
        SELECT src.control_id, src.schema_name, src.table_name,
               src.watermark_column, src.last_watermark_value
        FROM @ActiveSources AS src
        JOIN sys.schemas AS s ON s.name = src.schema_name
        JOIN sys.tables AS t ON t.schema_id = s.schema_id AND t.name = src.table_name
        JOIN sys.columns AS c ON c.object_id = t.object_id AND c.name = src.watermark_column
        JOIN sys.types AS ty ON ty.user_type_id = c.user_type_id
        WHERE ty.name IN ('datetime', 'datetime2', 'smalldatetime')
          AND c.is_computed = 0;

    OPEN source_cursor;
    FETCH NEXT FROM source_cursor
      INTO @control_id, @schema_name, @table_name, @watermark_column, @checkpoint;

    WHILE @@FETCH_STATUS = 0
    BEGIN
        SET @qualified_table = QUOTENAME(@schema_name) + N'.' + QUOTENAME(@table_name);
        SET @quoted_watermark = QUOTENAME(@watermark_column);
        SET @sql = N'
            SELECT
                @total_rows = COUNT_BIG(*),
                @null_cursor_rows = COUNT_BIG(CASE WHEN ' + @quoted_watermark + N' IS NULL THEN 1 END),
                @minimum_cursor = CONVERT(nvarchar(48), MIN(' + @quoted_watermark + N'), 126),
                @maximum_cursor = CONVERT(nvarchar(48), MAX(' + @quoted_watermark + N'), 126),
                @rows_at_maximum_cursor = COUNT_BIG(
                    CASE WHEN ' + @quoted_watermark + N' = (
                        SELECT MAX(' + @quoted_watermark + N') FROM ' + @qualified_table + N'
                    ) THEN 1 END
                ),
                @rows_at_or_before_checkpoint = COUNT_BIG(
                    CASE WHEN @checkpoint IS NOT NULL AND ' + @quoted_watermark + N' <= @checkpoint THEN 1 END
                )
            FROM ' + @qualified_table + N';';

        SET @total_rows = NULL;
        SET @null_cursor_rows = NULL;
        SET @minimum_cursor = NULL;
        SET @maximum_cursor = NULL;
        SET @rows_at_maximum_cursor = NULL;
        SET @rows_at_or_before_checkpoint = NULL;

        EXEC sys.sp_executesql
            @sql,
            N'@checkpoint datetime2(7),
              @total_rows bigint OUTPUT,
              @null_cursor_rows bigint OUTPUT,
              @minimum_cursor nvarchar(48) OUTPUT,
              @maximum_cursor nvarchar(48) OUTPUT,
              @rows_at_maximum_cursor bigint OUTPUT,
              @rows_at_or_before_checkpoint bigint OUTPUT',
            @checkpoint = @checkpoint,
            @total_rows = @total_rows OUTPUT,
            @null_cursor_rows = @null_cursor_rows OUTPUT,
            @minimum_cursor = @minimum_cursor OUTPUT,
            @maximum_cursor = @maximum_cursor OUTPUT,
            @rows_at_maximum_cursor = @rows_at_maximum_cursor OUTPUT,
            @rows_at_or_before_checkpoint = @rows_at_or_before_checkpoint OUTPUT;

        INSERT INTO @Stats
        VALUES (@control_id, @total_rows, @null_cursor_rows, @minimum_cursor,
                @maximum_cursor, @rows_at_maximum_cursor, @rows_at_or_before_checkpoint);

        FETCH NEXT FROM source_cursor
          INTO @control_id, @schema_name, @table_name, @watermark_column, @checkpoint;
    END;

    CLOSE source_cursor;
    DEALLOCATE source_cursor;

    SELECT * FROM @Stats ORDER BY control_id;
END;

PRINT 'Inventory complete. Catalog keys and datetime columns are candidates only; source owners must approve keys and cursor semantics.';

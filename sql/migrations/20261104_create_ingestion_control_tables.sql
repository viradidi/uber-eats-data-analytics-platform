/*
    Bootstrap the ADF control tables used by the exported pipelines.

    Safe to rerun:
      - creates the API/SQL control tables only when absent;
      - verifies required columns when tables already exist;
      - creates unique indexes when absent;
      - inserts missing synthetic API endpoint controls without reactivating
        or resetting existing endpoint rows;
      - does NOT invent or seed SQL source tables, business keys, or watermarks.

    Apply with an authorized SQL deployment identity after reviewing the target
    database. This script does not grant permissions to the Data Factory identity.
*/
SET NOCOUNT ON;
SET XACT_ABORT ON;

BEGIN TRY
    BEGIN TRANSACTION;

    IF OBJECT_ID(N'dbo.pipeline_control', N'U') IS NULL
    BEGIN
        CREATE TABLE dbo.pipeline_control
        (
            control_id int IDENTITY(1,1) NOT NULL
                CONSTRAINT PK_pipeline_control PRIMARY KEY,
            source_name varchar(128) NOT NULL,
            schema_name varchar(128) NOT NULL,
            table_name varchar(128) NOT NULL,
            load_type varchar(32) NOT NULL,
            watermark_column varchar(128) NOT NULL,
            last_watermark_value datetime2(7) NULL,
            batch_size int NULL,
            target_table varchar(256) NOT NULL,
            is_active bit NOT NULL
                CONSTRAINT DF_pipeline_control_is_active DEFAULT (0),
            last_run_start datetime NULL,
            last_run_end datetime NULL,
            last_run_status varchar(32) NULL,
            created_at datetime NOT NULL
                CONSTRAINT DF_pipeline_control_created_at DEFAULT (GETDATE()),
            updated_at datetime NOT NULL
                CONSTRAINT DF_pipeline_control_updated_at DEFAULT (GETDATE()),
            CONSTRAINT CK_pipeline_control_active_watermark
                CHECK (is_active = 0 OR last_watermark_value IS NOT NULL),
            CONSTRAINT CK_pipeline_control_schema_identifier
                CHECK (
                    schema_name NOT LIKE '%[^A-Za-z0-9_ ]%'
                    AND LEFT(schema_name, 1) LIKE '[A-Za-z_]'
                    AND schema_name NOT LIKE '% %'
                ),
            CONSTRAINT CK_pipeline_control_table_identifier
                CHECK (
                    table_name NOT LIKE '%[^A-Za-z0-9_ ]%'
                    AND LEFT(table_name, 1) LIKE '[A-Za-z_]'
                    AND table_name NOT LIKE '% %'
                ),
            CONSTRAINT CK_pipeline_control_watermark_identifier
                CHECK (
                    watermark_column NOT LIKE '%[^A-Za-z0-9_ ]%'
                    AND LEFT(watermark_column, 1) LIKE '[A-Za-z_]'
                    AND watermark_column NOT LIKE '% %'
                )
        );
    END
    ELSE IF EXISTS
    (
        SELECT required.column_name
        FROM (VALUES
            ('control_id'),
            ('source_name'),
            ('schema_name'),
            ('table_name'),
            ('load_type'),
            ('watermark_column'),
            ('last_watermark_value'),
            ('batch_size'),
            ('target_table'),
            ('is_active'),
            ('last_run_start'),
            ('last_run_end'),
            ('last_run_status'),
            ('created_at'),
            ('updated_at')
        ) AS required(column_name)
        WHERE COL_LENGTH(N'dbo.pipeline_control', required.column_name) IS NULL
    )
    BEGIN
        THROW 51100, 'Existing dbo.pipeline_control is missing one or more ADF-required columns; inspect and migrate it explicitly.', 1;
    END;

    IF NOT EXISTS
    (
        SELECT 1
        FROM sys.indexes
        WHERE object_id = OBJECT_ID(N'dbo.pipeline_control')
          AND name = N'UX_pipeline_control_active_source_cursor'
    )
    BEGIN
        CREATE UNIQUE INDEX UX_pipeline_control_active_source_cursor
            ON dbo.pipeline_control(schema_name, table_name, watermark_column)
            WHERE is_active = 1;
    END;

    IF OBJECT_ID(N'dbo.api_pipeline_control', N'U') IS NULL
    BEGIN
        CREATE TABLE dbo.api_pipeline_control
        (
            control_id int IDENTITY(1,1) NOT NULL
                CONSTRAINT PK_api_pipeline_control PRIMARY KEY,
            endpoint_name varchar(64) NOT NULL,
            target_folder varchar(256) NOT NULL,
            is_active bit NOT NULL
                CONSTRAINT DF_api_pipeline_control_is_active DEFAULT (1),
            last_watermark_value datetime2(7) NULL,
            last_run_start datetime NULL,
            last_run_end datetime NULL,
            last_run_status varchar(32) NULL,
            created_at datetime NOT NULL
                CONSTRAINT DF_api_pipeline_control_created_at DEFAULT (GETDATE()),
            updated_at datetime NOT NULL
                CONSTRAINT DF_api_pipeline_control_updated_at DEFAULT (GETDATE())
        );
    END
    ELSE IF EXISTS
    (
        SELECT required.column_name
        FROM (VALUES
            ('control_id'),
            ('endpoint_name'),
            ('target_folder'),
            ('is_active'),
            ('last_watermark_value'),
            ('last_run_start'),
            ('last_run_end'),
            ('last_run_status'),
            ('created_at'),
            ('updated_at')
        ) AS required(column_name)
        WHERE COL_LENGTH(N'dbo.api_pipeline_control', required.column_name) IS NULL
    )
    BEGIN
        THROW 51101, 'Existing dbo.api_pipeline_control is missing one or more ADF-required columns; inspect and migrate it explicitly.', 1;
    END;

    IF NOT EXISTS
    (
        SELECT 1
        FROM sys.indexes
        WHERE object_id = OBJECT_ID(N'dbo.api_pipeline_control')
          AND name = N'UX_api_pipeline_control_endpoint_name'
    )
    BEGIN
        CREATE UNIQUE INDEX UX_api_pipeline_control_endpoint_name
            ON dbo.api_pipeline_control(endpoint_name);
    END;

    /*
      These four endpoint names and folders come from api_metadata_v2 (2).json.
      Existing rows retain their active flag and watermark; missing rows are
      inserted active so the mock API branch has its documented four endpoints.
    */
    UPDATE target
       SET target.target_folder = source.target_folder
    FROM dbo.api_pipeline_control AS target
    INNER JOIN
    (
        VALUES
            ('payments', 'api/payments'),
            ('deliveries', 'api/deliveries'),
            ('ratings', 'api/ratings'),
            ('order-status', 'api/order_status')
    ) AS source(endpoint_name, target_folder)
        ON source.endpoint_name = target.endpoint_name;

    INSERT INTO dbo.api_pipeline_control
        (endpoint_name, target_folder, is_active)
    SELECT source.endpoint_name, source.target_folder, 1
    FROM
    (
        VALUES
            ('payments', 'api/payments'),
            ('deliveries', 'api/deliveries'),
            ('ratings', 'api/ratings'),
            ('order-status', 'api/order_status')
    ) AS source(endpoint_name, target_folder)
    WHERE NOT EXISTS
    (
        SELECT 1
        FROM dbo.api_pipeline_control AS target
        WHERE target.endpoint_name = source.endpoint_name
    );

    COMMIT TRANSACTION;
    PRINT 'Verified/created dbo.pipeline_control and dbo.api_pipeline_control; seeded missing API endpoint controls.';
    PRINT 'No SQL source control rows or watermark values were created.';
END TRY
BEGIN CATCH
    IF XACT_STATE() <> 0
        ROLLBACK TRANSACTION;
    THROW;
END CATCH;

/*
    Metadata required by the repository-owned SQL Silver publisher.

    Apply this migration to the CONTROL SQL database after:
      1. dbo.pipeline_control exists (20261104_create_ingestion_control_tables.sql)
      2. approved SQL source control rows have been created.

    No source keys, column types, targets, or active Silver configs are invented.
    Every config is disabled until a source owner approves and enables it.
*/
SET NOCOUNT ON;
SET XACT_ABORT ON;

BEGIN TRY
    BEGIN TRANSACTION;

    IF OBJECT_ID(N'dbo.sql_silver_control', N'U') IS NULL
    BEGIN
        CREATE TABLE dbo.sql_silver_control
        (
            control_id int NOT NULL
                CONSTRAINT PK_sql_silver_control PRIMARY KEY,
            is_enabled bit NOT NULL
                CONSTRAINT DF_sql_silver_control_is_enabled DEFAULT (0),
            write_strategy varchar(20) NOT NULL
                CONSTRAINT DF_sql_silver_control_write_strategy DEFAULT ('DELTA_MERGE'),
            business_key_columns_json nvarchar(2000) NULL,
            required_columns_json nvarchar(max) NULL,
            column_types_json nvarchar(max) NULL,
            created_at datetime2(0) NOT NULL
                CONSTRAINT DF_sql_silver_control_created_at DEFAULT (SYSUTCDATETIME()),
            updated_at datetime2(0) NOT NULL
                CONSTRAINT DF_sql_silver_control_updated_at DEFAULT (SYSUTCDATETIME()),
            CONSTRAINT FK_sql_silver_control_pipeline_control
                FOREIGN KEY (control_id) REFERENCES dbo.pipeline_control(control_id),
            CONSTRAINT CK_sql_silver_control_write_strategy
                CHECK (write_strategy = 'DELTA_MERGE'),
            CONSTRAINT CK_sql_silver_control_enabled_contract
                CHECK (
                    is_enabled = 0
                    OR (
                        business_key_columns_json IS NOT NULL
                        AND ISJSON(business_key_columns_json) = 1
                        AND required_columns_json IS NOT NULL
                        AND ISJSON(required_columns_json) = 1
                        AND column_types_json IS NOT NULL
                        AND ISJSON(column_types_json) = 1
                    )
                )
        );
    END
    ELSE IF EXISTS
    (
        SELECT required.column_name
        FROM (VALUES
            ('control_id'),
            ('is_enabled'),
            ('write_strategy'),
            ('business_key_columns_json'),
            ('required_columns_json'),
            ('column_types_json'),
            ('created_at'),
            ('updated_at')
        ) AS required(column_name)
        WHERE COL_LENGTH(N'dbo.sql_silver_control', required.column_name) IS NULL
    )
    BEGIN
        THROW 51300, 'Existing dbo.sql_silver_control is missing one or more required columns; inspect and migrate it explicitly.', 1;
    END;

    IF NOT EXISTS (
        SELECT 1 FROM sys.foreign_keys
        WHERE name = N'FK_sql_silver_control_pipeline_control'
          AND parent_object_id = OBJECT_ID(N'dbo.sql_silver_control')
    )
    BEGIN
        ALTER TABLE dbo.sql_silver_control WITH CHECK
        ADD CONSTRAINT FK_sql_silver_control_pipeline_control
            FOREIGN KEY (control_id) REFERENCES dbo.pipeline_control(control_id);
    END;

    IF NOT EXISTS (
        SELECT 1 FROM sys.check_constraints
        WHERE name = N'CK_sql_silver_control_write_strategy'
          AND parent_object_id = OBJECT_ID(N'dbo.sql_silver_control')
    )
    BEGIN
        ALTER TABLE dbo.sql_silver_control WITH CHECK
        ADD CONSTRAINT CK_sql_silver_control_write_strategy
            CHECK (write_strategy = 'DELTA_MERGE');
    END;

    IF NOT EXISTS (
        SELECT 1 FROM sys.check_constraints
        WHERE name = N'CK_sql_silver_control_enabled_contract'
          AND parent_object_id = OBJECT_ID(N'dbo.sql_silver_control')
    )
    BEGIN
        ALTER TABLE dbo.sql_silver_control WITH CHECK
        ADD CONSTRAINT CK_sql_silver_control_enabled_contract
            CHECK (
                is_enabled = 0
                OR (
                    business_key_columns_json IS NOT NULL
                    AND ISJSON(business_key_columns_json) = 1
                    AND required_columns_json IS NOT NULL
                    AND ISJSON(required_columns_json) = 1
                    AND column_types_json IS NOT NULL
                    AND ISJSON(column_types_json) = 1
                )
            );
    END;

    COMMIT TRANSACTION;
    PRINT 'Verified dbo.sql_silver_control. No source configurations were created or enabled.';
END TRY
BEGIN CATCH
    IF XACT_STATE() <> 0
        ROLLBACK TRANSACTION;
    THROW;
END CATCH;

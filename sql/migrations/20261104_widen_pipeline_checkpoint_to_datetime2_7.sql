/*
    Migration: widen the incremental control checkpoint to datetime2(7).

    The exported ADF control dataset previously described
    last_watermark_value as datetime (millisecond-scale metadata). A SQL source
    cursor can be datetime2 with precision up to seven fractional digits. The
    control value must not silently round/truncate that cursor before the next
    strict lower-bound extraction.

    This migration:
      - requires dbo.pipeline_control and last_watermark_value to exist;
      - accepts only datetime, datetime2, or smalldatetime as the old type;
      - preserves the column's existing NULL / NOT NULL property;
      - changes only the control checkpoint column;
      - does not drop rows, reset checkpoints, or alter source tables.

    Review indexes/default constraints and take a database backup according to
    your normal change process before applying this migration in an environment.
*/
SET NOCOUNT ON;
SET XACT_ABORT ON;

DECLARE @object_id int = OBJECT_ID(N'dbo.pipeline_control', N'U');
IF @object_id IS NULL
    THROW 51020, 'dbo.pipeline_control does not exist; migration stopped.', 1;

DECLARE @is_nullable bit;
DECLARE @type_name sysname;
DECLARE @scale tinyint;

SELECT
    @is_nullable = c.is_nullable,
    @type_name = ty.name,
    @scale = c.scale
FROM sys.columns AS c
JOIN sys.types AS ty
    ON ty.user_type_id = c.user_type_id
WHERE c.object_id = @object_id
  AND c.name = N'last_watermark_value';

IF @type_name IS NULL
    THROW 51021, 'pipeline_control.last_watermark_value does not exist; migration stopped.', 1;

IF @type_name NOT IN (N'datetime', N'datetime2', N'smalldatetime')
    THROW 51022, 'last_watermark_value is not a supported datetime type; inspect and migrate manually.', 1;

IF @type_name = N'datetime2' AND @scale = 7
BEGIN
    PRINT 'last_watermark_value is already datetime2(7); no change required.';
END
ELSE
BEGIN
    DECLARE @nullability nvarchar(8) =
        CASE WHEN @is_nullable = 1 THEN N' NULL' ELSE N' NOT NULL' END;

    DECLARE @sql nvarchar(max) =
        N'ALTER TABLE dbo.pipeline_control ALTER COLUMN last_watermark_value datetime2(7)' +
        @nullability + N';';

    BEGIN TRY
        BEGIN TRANSACTION;
        EXEC sys.sp_executesql @sql;
        COMMIT TRANSACTION;
        PRINT 'pipeline_control.last_watermark_value widened to datetime2(7).';
    END TRY
    BEGIN CATCH
        IF XACT_STATE() <> 0
            ROLLBACK TRANSACTION;
        THROW;
    END CATCH;
END;

IF NOT EXISTS (
    SELECT 1
    FROM sys.columns AS c
    JOIN sys.types AS ty
        ON ty.user_type_id = c.user_type_id
    WHERE c.object_id = OBJECT_ID(N'dbo.pipeline_control', N'U')
      AND c.name = N'last_watermark_value'
      AND ty.name = N'datetime2'
      AND c.scale = 7
)
    THROW 51023, 'Checkpoint precision verification failed after migration.', 1;

PRINT 'Verified: dbo.pipeline_control.last_watermark_value is datetime2(7).';

/*
    Add stable-ID cursor to API checkpoints.
    Existing timestamp values are preserved; NULL IDs request one full-snapshot
    replay so equal-timestamp rows at the old boundary are not skipped.
    Safe to rerun. Does not delete or rewrite existing control rows.
*/
SET NOCOUNT ON;
SET XACT_ABORT ON;

IF OBJECT_ID(N'dbo.api_pipeline_control', N'U') IS NULL
    THROW 51200, 'dbo.api_pipeline_control does not exist; apply bootstrap first.', 1;

IF COL_LENGTH(N'dbo.api_pipeline_control', N'last_watermark_id') IS NULL
BEGIN
    ALTER TABLE dbo.api_pipeline_control ADD last_watermark_id nvarchar(256) NULL;
    PRINT 'Added nullable dbo.api_pipeline_control.last_watermark_id.';
END
ELSE IF NOT EXISTS
(
    SELECT 1 FROM sys.columns AS c
    JOIN sys.types AS ty ON ty.user_type_id=c.user_type_id
    WHERE c.object_id=OBJECT_ID(N'dbo.api_pipeline_control',N'U')
      AND c.name=N'last_watermark_id' AND ty.name=N'nvarchar'
      AND c.max_length>=512 AND c.is_nullable=1
)
    THROW 51201, 'Existing last_watermark_id has incompatible type or nullability.', 1;

SELECT COUNT_BIG(*) AS legacy_rows_requiring_one_full_snapshot_replay
FROM dbo.api_pipeline_control
WHERE is_active=1 AND (last_watermark_value IS NULL OR last_watermark_id IS NULL);

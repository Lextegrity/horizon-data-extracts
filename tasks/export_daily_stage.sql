// STAGE
// Run delta export daily at 6am UTC (1am EST)
// Which generates delta files for the previous day
CREATE OR REPLACE TASK EXPORT.PUBLIC.export_daily_stage
    WAREHOUSE = GENERAL
    SCHEDULE = 'USING CRON 0 6 * * * UTC'
    COMMENT = 'Runs the daily export of customer data for the stage environment'
AS 
    CALL EXPORT.PUBLIC.export_delta('stage')
;

-- Need to call this once to begin the task after creation
-- ALTER TASK EXPORT.PUBLIC.export_daily_stage RESUME;

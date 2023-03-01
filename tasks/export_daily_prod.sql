// PROD
// Run delta export daily at 2am UTC
// Which generates delta files for the previous day
CREATE OR REPLACE TASK EXPORT.PUBLIC.export_daily_prod
  WAREHOUSE = GENERAL
  SCHEDULE = 'USING CRON 0 2 * * * UTC'
  COMMENT = 'Runs the daily export of customer data for the production environment'
AS 
    CALL EXPORT.PUBLIC.export_delta('prod')
;

-- Need to call this once to begin the task after creation
-- ALTER TASK EXPORT.PUBLIC.export_daily_prod RESUME;

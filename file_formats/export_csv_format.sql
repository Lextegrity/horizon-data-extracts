-- IF THIS DEFINITION CHANGES, ANY TABLES OR STAGES THAT REFERENCE IT MUST BE RECREATED
-- Should just be the `export_category` procedures
-- https://docs.snowflake.com/en/sql-reference/sql/create-file-format.html#usage-notes
CREATE OR REPLACE FILE FORMAT EXPORT.PUBLIC.export_csv_format
  TYPE = csv
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  COMPRESSION = NONE
  -- FIELD_DELIMITER = '|'
  -- SKIP_HEADER = 1
  NULL_IF = ('')
  EMPTY_FIELD_AS_NULL = false
;

CREATE STAGE EXPORT.PUBLIC.export_stage_s3
    URL = 's3://stagevpc-environments3bucketd212651c-1ie9g30pdj5d6/data/'
    FILE_FORMAT = EXPORT.PUBLIC.export_csv_format
    STORAGE_INTEGRATION = export_s3_int
;

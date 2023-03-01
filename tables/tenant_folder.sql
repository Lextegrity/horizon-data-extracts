CREATE TABLE EXPORT.PUBLIC.TENANT_FOLDER (
    tenant_id       varchar     NOT NULL PRIMARY KEY
  , folder_name     varchar     NOT NULL
) COMMENT = 'Contains mapping of tenant id to folder name in S3 if it differs';

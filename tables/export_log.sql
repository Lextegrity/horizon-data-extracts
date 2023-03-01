CREATE TABLE export_log (
    id              varchar     NOT NULL
  , tenant_id       varchar     NOT NULL
  , env             varchar
  , category        varchar     NOT NULL
  , method          varchar     NOT NULL
  , start_ts        timestamptz NOT NULL
  , end_ts          timestamptz NOT NULL
  , bucket          varchar     NOT NULL
  , file_path       varchar     NOT NULL
  , completed       boolean     DEFAULT false
  , export_count    int
  , error_message   varchar
  , export_ts       timestamptz NOT NULL
  
) COMMENT = 'Contains the export history for all tenants and categories';

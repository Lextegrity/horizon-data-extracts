CREATE PROCEDURE export_adhoc (
        TENANT_ID string
      , ENV string
      , CATEGORY string
      , START_TS string
      , END_TS string
    )
    RETURNS string -- uuid of export
    LANGUAGE javascript
    RETURNS NULL ON NULL INPUT
    COMMENT = 'Handles adhoc export of tenant and a single category. Essentially a wrapper around export_category that sets the method.'
    EXECUTE AS CALLER
    as $$
        const export_id_row = snowflake.createStatement({
            sqlText: `
                CALL EXPORT.PUBLIC.export_category(
                    '${TENANT_ID}'
                  , '${ENV}'
                  , '${CATEGORY}'
                  , '${START_TS}'
                  , '${END_TS}'
                  , 'adhoc'
                );
            `
        }).execute()
        export_id_row.next()
        const export_id = export_id_row.getColumnValueAsString(1);
        
        /**
         * Return export id
         */
        return export_id
    $$;

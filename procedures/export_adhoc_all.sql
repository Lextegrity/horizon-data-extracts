CREATE PROCEDURE export_adhoc_all (
        TENANT_ID string
      , ENV string
      , START_TS string
      , END_TS string
    )
    RETURNS boolean
    LANGUAGE javascript
    RETURNS NULL ON NULL INPUT
    COMMENT = 'Handles adhoc export of tenant and all their categories.'
    EXECUTE AS CALLER
    as $$
        /**
         * Define set of categories, each requires DB function `export_${category}` to be present
         */
        const categories = [
            'actions',
            'activities',
            'parties',
            'party_details',
            'requests',
            'request_activity_parties',
            'responses',
            'users'
        ];


        /**
         * For each category, call stored procedure for exporting category
         */
        categories.forEach(category => {
          snowflake.createStatement({
            sqlText: `
                CALL EXPORT.PUBLIC.export_adhoc(
                    '${TENANT_ID}'
                  , '${ENV}'
                  , '${category}'
                  , '${START_TS}'
                  , '${END_TS}'
                );
            `
          }).execute()
        });
        
        /**
         * Return
         */
        return true
    $$;

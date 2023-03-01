CREATE PROCEDURE export_full (
        TENANT_ID string
      , ENV string
      , INITIAL boolean
    )
    RETURNS boolean
    LANGUAGE javascript
    RETURNS NULL ON NULL INPUT
    COMMENT = 'Handles full export of tenant and their categories'
    EXECUTE AS CALLER
    AS $$
        /**
         * Set session timezone to `UTC`
         */
        snowflake.createStatement({
            sqlText: `ALTER SESSION SET timezone = 'UTC';`
        }).execute();


        /**
         * Set start timestamp
         */
        const start_ts = '1970-01-01 00:00:00';


        /**
         * If initial, use yesterday's date. So next delta will not overlap.
         * Otherwise, use current timestamp
         */
        const end_ts_row = snowflake.createStatement({
            sqlText: 'SELECT current_timestamp(0);'
        }).execute();
        end_ts_row.next();
        
        const yesterday_row = snowflake.createStatement({
            sqlText: `SELECT current_date - INTERVAL '1 day';`
        }).execute();
        yesterday_row.next();
        const yesterday = yesterday_row.getColumnValueAsString(1).slice(0,11);
        
        let end_ts;
        if(INITIAL) {
            end_ts = yesterday.concat('23:59:59');
        } else {
            end_ts = end_ts_row.getColumnValueAsString(1);
        }
        

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
                CALL EXPORT.PUBLIC.export_category(
                    '${TENANT_ID}'
                  , '${ENV}'
                  , '${category}'
                  , '${start_ts}'
                  , '${end_ts}'
                  , 'full'
                );
            `
          }).execute()
        });
        
        
        /**
         * Return
         */
        return true
    $$;

CREATE PROCEDURE export_delta (
    ENV string
)
    RETURNS string
    LANGUAGE javascript
    COMMENT = 'Handles exporting all tenants and all categories'
    EXECUTE AS CALLER
    as $$
        /**
         * Set session timezone to `UTC`
         */
        snowflake.createStatement({
            sqlText: `ALTER SESSION SET timezone = 'UTC';`
        }).execute();


        /**
         * Set start and end ts
         */
        // Get yesterday's date
        const yesterday_row = snowflake.createStatement({
            sqlText: `SELECT current_date - INTERVAL '1 day';`
        }).execute();
        yesterday_row.next();
        const yesterday = yesterday_row.getColumnValueAsString(1).slice(0,11);
        
        // Use start of day for start ts
        const start_ts = yesterday.concat('00:00:00');
        
        // Use end of day for end ts
        const end_ts = yesterday.concat('23:59:59');
                                 
        /**
         * Get list of tenants to export
         */
        const tenants = [];
        const tenant_list = snowflake.createStatement({
           sqlText: `
              SELECT id
              FROM RAW.HORIZON_${ENV}.HZN_MASTER_TENANT
              WHERE is_active = true
              ;
           `
        }).execute()
        while(tenant_list.next()) {
            tenants.push(tenant_list.getColumnValueAsString(1));
        }


        /**
         * Get list of categories to export
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
         * For each tenant, and each category, call export_category
         */
        let err = '';
        err += `${start_ts} -> ${end_ts}\n`
        tenants.forEach((tenant) => {
            err += `${tenant}\n`
            categories.forEach((category) => {
                err += `${category}\n`
                try {
                    snowflake.createStatement({
                        sqlText: `
                            CALL EXPORT.PUBLIC.export_category(
                                '${tenant}'
                              , '${ENV}'
                              , '${category}'
                              , '${start_ts}'
                              , '${end_ts}'
                              , 'delta'
                            );
                        `
                    }).execute()
                } catch (error) {
                  // Don't block other exports on failure
                  err += error.toString() + '\n'
                }
            });
        });


        /**
         * Return
         */
        if(err) {
            return err
        }
        return 'ok';
    $$;

CREATE PROCEDURE export_full_init (
      ENV string
    )
    RETURNS boolean
    LANGUAGE javascript
    RETURNS NULL ON NULL INPUT
    COMMENT = 'Handles initial export of all tenants and categories. Differs from export_full in that it uses up to end of previous day, so delta will not overlap.'
    EXECUTE AS CALLER
    AS $$
        /**
         * Set session timezone to `UTC`
         */
        snowflake.createStatement({
            sqlText: `ALTER SESSION SET timezone = 'UTC';`
        }).execute();

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
         * For each tenant, call stored procedure for exporting full tenant data
         */
        tenants.forEach(tenant => {
          snowflake.createStatement({
            sqlText: `
                CALL EXPORT.PUBLIC.export_full(
                    '${tenant}'
                  , '${ENV}'
                  , true
                );
            `
          }).execute()
        });
        
        
        /**
         * Return
         */
        return true
    $$;

CREATE PROCEDURE export_category (
        TENANT_ID string
      , ENV string
      , CATEGORY string
      , START_TS string
      , END_TS string
      , METHOD string -- full/delta/adhoc
    )
    RETURNS string -- uuid of export id
    LANGUAGE javascript
    RETURNS NULL ON NULL INPUT
    COMMENT = 'Handles exporting a single category of Horizon data to associated S3 bucket'
    EXECUTE AS CALLER
    AS $$
        const envs = [
            'stage',
            'prod'
        ];
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
         * Set session timezone to `UTC`
         */
        snowflake.createStatement({
            sqlText: `ALTER SESSION SET timezone = 'UTC';`
        }).execute();


        /**
         * Get current timestamp
         */
        const current_ts_row = snowflake.createStatement({
            sqlText: 'SELECT current_timestamp(0);'
        }).execute();
        current_ts_row.next();
        const current_ts = current_ts_row.getColumnValueAsString(1);


        /**
         * Create filename
         *  [customer_name]_[env]_[data_object]_[run_date]_[extract_type]_[date_from]_[date_to].[extension]
         *  eg. acme_approvals_requests_20220721180505_delta_20220720_20220721.csv
         * If method type 'full', just use end date
         *  eg. acme_approvals_requests_20220721180505_full_20220721.csv
         */
        const r = /:|-| /g
        
        // 2022-12-13 22:47:43 => 20221213224743
        const run_date = current_ts.replace(r, '');
        
        // 2022-12-13 22:47:43 => 20221213
        const date_from = START_TS.replace(r, '').slice(0,8);
        const date_to = END_TS.replace(r, '').slice(0,8);
        
        let filename = `${TENANT_ID}_${ENV}_${CATEGORY}_${run_date}_${METHOD}_`;
        if(METHOD == 'full') {
            filename += `${date_to}.csv`
        } else {
            filename += `${date_from}_${date_to}.csv`
        }


        /**
         * Set folder name:
         *  /[customer_name]/extracts/
         *      Root directory where any extract documentation will be stored, in addition to the directories that will store the extract files themselves.
         *  /[customer_name]/extracts/[env]/_adhoc 
         *      Directory where any ad-hoc (requested out of the typical periodic processing of extracts) will be placed.
         *  /[customer_name]/extracts/[env]/[YYYY-MM]/ 
         *      The directory which will be dynamically created based on the date of the data extracted (not the run date). 
         */
        
        // Root tenant folder name in S3 might differ from Horizon's tenant_id
        //  eg. hal -> halliburton
        // IF there is a value for the tenant in table: EXPORT.PUBLIC.TENANT_FOLDER
        //  use that value instead of TENANT_ID
        const tenant_folder_name = snowflake.createStatement({
            sqlText: `
                SELECT folder_name
                FROM EXPORT.PUBLIC.TENANT_FOLDER
                WHERE tenant_id = '${TENANT_ID}'
                ;
            `
        }).execute();
        
        let tenant_folder = TENANT_ID;
        if(tenant_folder_name.getRowCount() > 0){
            tenant_folder_name.next();
            tenant_folder = tenant_folder_name.getColumnValueAsString(1);
        }
        
        // If delta, use date for folder name [YYYY-MM], otherwise use method
        let folder_name = `_${METHOD}`;
        if(METHOD == 'delta') {
            folder_name = `${date_from.slice(0,4)}-${date_from.slice(4,6)}`;
        }
        
        const folder_path = `${tenant_folder}/extracts/${ENV}/${folder_name}/`;
        const file_path = folder_path + filename;


        /**
         * Set bucket (only for export log recording, actual bucket determined by stage/storage integration)
         */
        const bucket = 'stagevpc-environments3bucketd212651c-1ie9g30pdj5d6';


        /**
         * Create export ID
         */
        const export_id_row = snowflake.createStatement({
            sqlText: 'SELECT uuid_string();'
        }).execute();
        export_id_row.next();
        const export_id = export_id_row.getColumnValueAsString(1);


        /**
         * Create an entry in the export log for this export
         */
        snowflake.createStatement({
            sqlText: `
                INSERT INTO EXPORT.PUBLIC.export_log (
                    id
                  , env
                  , tenant_id
                  , category
                  , method
                  , bucket
                  , file_path
                  , start_ts
                  , end_ts
                  , export_ts
                ) VALUES (
                    '${export_id}'
                  , '${ENV}'
                  , '${TENANT_ID}'
                  , '${CATEGORY}'
                  , '${METHOD}'
                  , '${bucket}'
                  , '${file_path}'
                  , '${START_TS}'
                  , '${END_TS}'
                  , '${current_ts}'
                );
            `
        }).execute();


        /**
         * Retrieve (and copy to S3) the rows from the export function
         * for the given category.
         *
         *  Updates entry with row count on success
         *                with error message on error
         */
        try {
            // Throw here for export log to record
            if(!categories.includes(CATEGORY)) {
                throw new Error('${CATEGORY} not a valid category')
            }
            if(!envs.includes(ENV)) {
                throw new Error(`${ENV} not a valid environment`)
            }
            const exported_rows = snowflake.createStatement({
                sqlText: `
                    COPY INTO '@export_stage_s3/${file_path}'
                    FROM (
                        SELECT *
                        FROM TABLE(
                            EXPORT.PUBLIC.export_${CATEGORY}_${ENV}('${TENANT_ID}', '${START_TS}', '${END_TS}')
                        )
                    )
                    SINGLE=true
                    HEADER=true
                    MAX_FILE_SIZE=1000000000
                    ;
                `
            }).execute();
            exported_rows.next();
            const export_count = exported_rows.getColumnValue('rows_unloaded');
            
            // If we export 0 rows, no file is created.
            // We still want file created with headers,
            // so do some nonsense to export the columns
            if(export_count == 0) {
                snowflake.createStatement({
                    sqlText: `
                        SELECT *
                        FROM TABLE(
                            EXPORT.PUBLIC.export_${CATEGORY}_${ENV}('${TENANT_ID}', '${START_TS}', '${END_TS}')
                        )
                    `
                }).execute();
                
                const descResult = snowflake.createStatement({
                    sqlText: `
                        DESC RESULT last_query_id();
                    `
                }).execute();
                const columnNamesList = []
                while(descResult.next()) {
                    columnNamesList.push(descResult.getColumnValueAsString('name'))
                }
                const columnNamesString = columnNamesList.map(cn => {
                    return `'${cn}'`
                }).join(',')
                
                snowflake.createStatement({
                    sqlText: `
                        COPY INTO '@export_stage_s3/${file_path}'
                        FROM (
                            SELECT *
                            FROM VALUES(${columnNamesString})
                        )
                        SINGLE=true
                        HEADER=false
                        ;
                    `
                }).execute()
            }
            
            // Update entry with success
            snowflake.createStatement({
                sqlText: `
                    UPDATE EXPORT.PUBLIC.export_log
                    SET completed = true,
                        export_count = ${export_count}
                    WHERE id = '${export_id}'
                `
            }).execute();
        } catch(err) {
            // Update entry with error
            snowflake.createStatement({
                sqlText: `
                    UPDATE EXPORT.PUBLIC.export_log
                    SET completed = true,
                        error_message = '${err.toString()}'
                    WHERE id = '${export_id}'
                `
            }).execute();
        }


        /**
         * Return export id
         */
        return export_id;
    $$;

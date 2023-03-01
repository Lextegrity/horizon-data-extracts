CREATE OR REPLACE FUNCTION export_party_details_prod (
    arg_tenant_id varchar
  , arg_start_date varchar
  , arg_end_date varchar
    ) RETURNS TABLE( 
        party_id varchar
      , info_code varchar
      , value varchar
      , modified_datetime timestamptz
      , customer_id varchar
    )
    RETURNS NULL ON NULL INPUT
    COMMENT = 'Produces the change in rows for `party details` that have occured between the start and end date for a given tenant'
    AS $$
        WITH id_insert_update AS (
            -- Party
            SELECT ID AS id
                 , TENANT_ID AS tenant_id
                 , max(DB_INSERT_TIMESTAMP) AS insert_dt
                 , max(DB_UPDATE_TIMESTAMP) AS update_dt
            FROM RAW.HORIZON_PROD.HZN_MASTER_PARTY
            WHERE TENANT_ID = arg_tenant_id
            GROUP BY 1, 2
            
            UNION
            
            -- Party Information
            SELECT PARTY_ID AS id
                 , TENANT_ID AS tenant_id
                 , max(DB_INSERT_TIMESTAMP) AS insert_dt
                 , max(DB_UPDATE_TIMESTAMP) AS update_dt
            FROM RAW.HORIZON_PROD.HZN_MASTER_PARTY_INFORMATION
            WHERE TENANT_ID = arg_tenant_id
            GROUP BY 1, 2
        ),
        agg_id_insert_update AS (
            SELECT id
                 , tenant_id
                 , max(insert_dt) AS insert_dt
                 , max(update_dt) AS update_dt
            FROM id_insert_update
            GROUP BY 1, 2
        ),
        
        delta_determination AS (
            SELECT tenant_id
                 , id
                 , greatest(
                     coalesce(insert_dt, '1900-01-01 00:00:00'::timestamptz)
                   , coalesce(update_dt, '1900-01-01 00:00:00'::timestamptz)
                 ) AS delta_date
            FROM agg_id_insert_update
            WHERE delta_date::timestamptz >= to_timestamp_tz(arg_start_date, 'YYYY-MM-DD HH:MI:SS')
              AND delta_date::timestamptz <= to_timestamp_tz(arg_end_date, 'YYYY-MM-DD HH:MI:SS')
              AND tenant_id = arg_tenant_id
        )
        
        SELECT PARTY_ID AS party_id
             , INFO_TYPE_CODE AS info_code
             , VALUE AS value
             , dd.delta_date AS modified_datetime
             , mpi.TENANT_ID AS customer_id
        FROM RAW.HORIZON_PROD.HZN_MASTER_PARTY_INFORMATION mpi
            -- Delta Restriction (parameter looks like: '2022-05-12 21:58:55')
            JOIN delta_determination dd ON dd.id = mpi.party_id AND mpi.TENANT_ID = arg_tenant_id
        ORDER BY dd.delta_date DESC
    $$;

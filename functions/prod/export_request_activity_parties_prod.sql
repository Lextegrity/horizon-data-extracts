CREATE OR REPLACE FUNCTION export_request_activity_parties_prod (
    arg_tenant_id varchar
  , arg_start_date varchar
  , arg_end_date varchar
    ) RETURNS TABLE( 
        request_id varchar
      , request_activity_id varchar
      , party_id varchar
      , amount number
      , currency_code varchar
      , modified_datetime timestamptz
      , customer_id varchar
    )
    RETURNS NULL ON NULL INPUT
    COMMENT = 'Produces the change in rows for `request activity parties` that have occured between the start and end date for a given tenant'
    AS $$
        WITH id_insert_update AS (
            -- Request
            SELECT ID                       AS id
                 , TENANT_ID                AS tenant_id
                 , max(DB_INSERT_TIMESTAMP) AS insert_dt
                 , max(DB_UPDATE_TIMESTAMP) AS update_dt
            FROM RAW.HORIZON_PROD.HZN_REQUEST_REQUEST
            WHERE TENANT_ID = arg_tenant_id
            GROUP BY 1, 2
            
            UNION
            
            -- Request Actions
            SELECT r.ID                       AS id
                 , r.TENANT_ID                AS tenant_id
                 , max(aa.ZTIMESTAMP) AS insert_dt
                 , max(aa.ZTIMESTAMP) AS update_dt -- note insert/update are the same.
            FROM RAW.HORIZON_PROD.HZN_REQUEST_REQUEST r
                 LEFT JOIN RAW.HORIZON_PROD.HZN_REQUEST_REQUEST_STEP rs ON rs.REQUEST_ID = r.ID
                 LEFT JOIN RAW.HORIZON_PROD.HZN_REQUEST_WORKFLOW_STEP rws ON rws.REQUEST_STEP_ID = rs.ID
                 LEFT JOIN RAW.HORIZON_PROD.HZN_REQUEST_WORKFLOW_ACTION aa ON aa.WORKFLOW_STEP_ID = rws.ID
            WHERE r.TENANT_ID = arg_tenant_id
            GROUP BY 1, 2
            
            UNION
            
            -- Request Versions
            SELECT REQUEST_ID               AS id
                 , TENANT_ID                AS tenant_id
                 , max(DB_INSERT_TIMESTAMP) AS insert_dt
                 , max(DB_UPDATE_TIMESTAMP) AS update_dt
            FROM RAW.HORIZON_PROD.HZN_REQUEST_REQUEST_HISTORY
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
        
        SELECT r.ID                            AS request_id
             , ra.ID                           AS request_activity_id
             , rp.PARTY_ID                     AS party_id
             , rap.SPEND_AMOUNT                AS amount
             , rap.CURRENCY_CODE               AS currency_code
             , dd.delta_date                   AS modified_datetime
             , r.TENANT_ID                     AS customer_id
        FROM RAW.HORIZON_PROD.HZN_REQUEST_REQUEST r
            JOIN RAW.HORIZON_PROD.HZN_REQUEST_REQUEST_ACTIVITY ra ON r.ID = ra.REQUEST_ID
            JOIN RAW.HORIZON_PROD.HZN_REQUEST_REQUEST_ACTIVITY_PARTY rap ON ra.id = rap.REQUEST_ACTIVITY_ID
            JOIN RAW.HORIZON_PROD.HZN_REQUEST_REQUEST_PARTY rp on rp.id = rap.REQUEST_PARTY_ID
            -- Delta Restriction (parameter looks like: '2022-05-12 21:58:55')
            join delta_determination dd on dd.id = r.ID
        WHERE r.ZSTATUS <> 'new_status' -- ensures we dont receive an unsaved request
          AND r.TENANT_ID = arg_tenant_id
          AND rp.PARTY_ID is not null
        order by dd.delta_date DESC
    $$;

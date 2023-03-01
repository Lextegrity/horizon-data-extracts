CREATE OR REPLACE FUNCTION export_activities_prod (
    arg_tenant_id varchar
  , arg_start_date varchar
  , arg_end_date varchar
    ) RETURNS TABLE( 
        id varchar
      , request_id varchar
      , is_primary_activity boolean
      , activity_status varchar
      , is_active boolean
      , activity_name varchar
      , amount number
      , currency_code varchar
      , modified_datetime timestamptz
      , customer_id varchar
    )
    RETURNS NULL ON NULL INPUT
    COMMENT = 'Produces the change in rows for `activities` that have occured between the start and end date for a given tenant'
    AS $$
        WITH id_insert_update AS (
            -- Request
            SELECT ID                         AS id
                 , TENANT_ID                  AS tenant_id
                 , max(DB_INSERT_TIMESTAMP)   AS insert_dt
                 , max(DB_UPDATE_TIMESTAMP)   AS update_dt
            FROM RAW.HORIZON_PROD.HZN_REQUEST_REQUEST
            WHERE TENANT_ID = arg_tenant_id
            GROUP BY 1, 2
            
            UNION
            
            -- Request Actions
            SELECT r.ID                       AS id
                 , r.TENANT_ID                AS tenant_id
                 , max(aa.ZTIMESTAMP)         AS insert_dt
                 , max(aa.ZTIMESTAMP)         AS update_dt -- note insert/update are the same.
            FROM RAW.HORIZON_PROD.HZN_REQUEST_REQUEST r
                 LEFT JOIN RAW.HORIZON_PROD.HZN_REQUEST_REQUEST_STEP rs ON rs.REQUEST_ID = r.ID
                 LEFT JOIN RAW.HORIZON_PROD.HZN_REQUEST_WORKFLOW_STEP rws ON rws.REQUEST_STEP_ID = rs.ID
                 LEFT JOIN RAW.HORIZON_PROD.HZN_REQUEST_WORKFLOW_ACTION aa ON aa.WORKFLOW_STEP_ID = rws.ID
            WHERE r.TENANT_ID = arg_tenant_id
            GROUP BY 1, 2
            
            UNION
            
            -- Request Versions
            SELECT REQUEST_ID                 AS id
                 , TENANT_ID                  AS tenant_id
                 , max(DB_INSERT_TIMESTAMP)   AS insert_dt
                 , max(DB_UPDATE_TIMESTAMP)   AS update_dt
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
        
        SELECT ra.ID                           AS id
             , r.ID                            AS request_id
             , ra.IS_PRIMARY                   AS is_primary_activity
             , sli.DEFAULT_TEXT                AS activity_status
             , a.IS_ACTIVE                     AS is_active
             , a.DEFAULT_TITLE                 AS activity_name
             , ra.SPEND_AMOUNT                 AS amount
             , ra.CURRENCY_CODE                AS currency_code
             , dd.delta_date                   AS modified_datetime
             , r.TENANT_ID                     AS customer_id
        FROM RAW.HORIZON_PROD.HZN_REQUEST_REQUEST r
            -- Request Activity
            JOIN RAW.HORIZON_PROD.HZN_REQUEST_REQUEST_ACTIVITY ra ON ra.REQUEST_ID = r.ID
            JOIN RAW.HORIZON_PROD.HZN_REQUEST_ACTIVITY a ON a.CODE = ra.ACTIVITY_CODE AND a.TENANT_ID = r.TENANT_ID
            JOIN RAW.HORIZON_PROD.HZN_MASTER_SYSTEM_LIST_ITEM sli ON sli.CODE = ra.ZSTATUS AND sli.LIST = 'request_status'
            -- Delta Restriction (parameter looks like: '2022-05-12 21:58:55')
            JOIN delta_determination dd ON dd.id = r.ID
        WHERE r.ZSTATUS <> 'new_status' -- ensures we dont receive an unsaved request
          AND ra.ZSTATUS <> 'new_status'
          AND r.TENANT_ID = arg_tenant_id
        ORDER BY dd.delta_date DESC
    $$;

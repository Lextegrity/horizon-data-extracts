CREATE OR REPLACE FUNCTION export_requests_prod (
    arg_tenant_id varchar
  , arg_start_date varchar
  , arg_end_date varchar
) RETURNS TABLE( 
        id varchar
      , formatted_id varchar
      , code varchar
      , status varchar
      , type varchar
      , created_datetime timestamptz
      , first_submission_datetime timestamptz
      , latest_submission_datetime timestamptz
      , last_updated_datetime timestamptz
      , completion_datetime timestamptz
      , submitter_id varchar
      , title varchar
      , amount number
      , currency_code varchar
      , application varchar
      , modified_datetime timestamptz
      , customer_id varchar
    )
    RETURNS NULL ON NULL INPUT
    COMMENT = 'Produces the change in rows for `requests` that have occured between the start and end date for a given tenant'
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
            SELECT r.ID               AS id
                 , r.TENANT_ID        AS tenant_id
                 , max(aa.ZTIMESTAMP) AS insert_dt
                 , max(aa.ZTIMESTAMP) AS update_dt -- note insert/update are the same.
            FROM RAW.HORIZON_PROD.HZN_REQUEST_REQUEST r
                LEFT JOIN RAW.HORIZON_PROD.HZN_REQUEST_REQUEST_STEP rs 
                    ON rs.REQUEST_ID = r.ID
                LEFT JOIN RAW.HORIZON_PROD.HZN_REQUEST_WORKFLOW_STEP rws
                    ON rws.REQUEST_STEP_ID = rs.ID
                LEFT JOIN RAW.HORIZON_PROD.HZN_REQUEST_WORKFLOW_ACTION aa
                    ON aa.WORKFLOW_STEP_ID = rws.ID
            WHERE r.tenant_id = arg_tenant_id
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
        ),
        
        created_dates AS (
            SELECT REQUEST_ID
                 , CREATE_TIMESTAMP AS created_timestamp
            FROM RAW.HORIZON_PROD.HZN_REQUEST_REQUEST_VERSION
            WHERE TENANT_ID = arg_tenant_id
            QUALIFY row_number() over (
                partition BY REQUEST_ID ORDER BY CREATED_TIMESTAMP ASC
            ) = 1 -- Limit the the most recent datetime.
            ORDER BY REQUEST_ID
        ),
        
        version_details AS (
            SELECT rv.TENANT_ID
                 , rv.REQUEST_ID
                 , rv.SUBMIT_TIMESTAMP
                 , CASE
                    WHEN rv.SUBMIT_TIMESTAMP is null then null
                    ELSE row_number() over (
                        partition BY rva.REQUEST_ID ORDER BY rva.SUBMIT_TIMESTAMP ASC
                    )
                   END rownum_asc
                 , CASE
                    WHEN rv.SUBMIT_TIMESTAMP is null then null
                    ELSE row_number() over (
                        PARTITION BY rvd.request_id ORDER BY rvd.SUBMIT_TIMESTAMP DESC
                    )
                   END rownum_desc
            FROM RAW.HORIZON_PROD.HZN_REQUEST_REQUEST_VERSION rv
                LEFT JOIN RAW.HORIZON_PROD.HZN_REQUEST_REQUEST_VERSION rva 
                    ON rv.ID = rva.ID AND
                       rva.SUBMIT_TIMESTAMP is not null
                LEFT JOIN RAW.HORIZON_PROD.HZN_REQUEST_REQUEST_VERSION rvd
                    ON rv.ID = rvd.ID AND
                       rvd.SUBMIT_TIMESTAMP is not null
            WHERE rv.TENANT_ID = arg_tenant_id
        ),
        
        completion_dates AS (
            SELECT REQUEST_ID
                 , ZSTATUS
                 , ZTIMESTAMP AS completed_timestamp
            FROM RAW.HORIZON_PROD.HZN_REQUEST_REQUEST_HISTORY
            WHERE (
                ZSTATUS = 'completed_status' OR
                ZSTATUS = 'canceled_status'  OR
                ZSTATUS = 'rejected_status'  OR
                ZSTATUS = 'deleted_status'
            ) AND
                TENANT_ID = arg_tenant_id
            QUALIFY row_number() over (
                partition BY REQUEST_ID ORDER BY ZTIMESTAMP DESC
            ) = 1 -- Limit the the most recent datetime.
            ORDER BY REQUEST_ID
        ),
        
        activity_sums AS (
            SELECT TENANT_ID
                 , REQUEST_ID
                 , max(CURRENCY_CODE) AS currency_code
                 , sum(coalesce(SPEND_AMOUNT, 0)) AS spend_amount_sum
            FROM RAW.HORIZON_PROD.HZN_REQUEST_REQUEST_ACTIVITY
            WHERE tenant_id = arg_tenant_id
            GROUP BY 1, 2
        )
        
        SELECT r.id                                 AS id
             , r.FORMATTED_ID                       AS formatted_id
             , r.TEMPLATE_CODE                      AS code
             , sli.DEFAULT_TEXT                     AS status
             , a.DEFAULT_TITLE                      AS type
             , created_dates.CREATED_TIMESTAMP      AS created_datetime
             , first_submit.SUBMIT_TIMESTAMP        AS first_submission_datetime
             , latest_submit.SUBMIT_TIMESTAMP       AS latest_submission_datetime
             , r.DB_UPDATE_TIMESTAMP                AS last_updated_datetime
             , completion_dates.COMPLETED_TIMESTAMP AS completion_datetime
             , usr.PARTY_ID                         AS submitter_id
             , r.ZNAME                              AS title
             , spend_amount_sum                     AS amount
             , currency_code                        AS currency_code
             , a.PRODUCT_CODE                       AS application
             , dd.delta_date                        AS modified_datetime
             , r.tenant_id                          AS customer_id
        FROM RAW.HORIZON_PROD.HZN_REQUEST_REQUEST r
             -- Request type
             JOIN RAW.HORIZON_PROD.HZN_REQUEST_ACTIVITY a
                ON a.CODE = r.PRIMARY_ACTIVITY_CODE AND
                   a.TENANT_ID = r.TENANT_ID
        -- Status
             JOIN RAW.HORIZON_PROD.HZN_MASTER_SYSTEM_LIST_ITEM sli
                ON sli.code = r.zstatus AND
                   sli.list = 'request_status'
        -- Timing
             JOIN version_details first_submit
                ON first_submit.REQUEST_ID = r.ID AND 
                   first_submit.rownum_asc = 1 AND
                   first_submit.TENANT_ID = r.TENANT_ID
             JOIN version_details latest_submit
                ON latest_submit.REQUEST_ID = r.ID AND
                   latest_submit.rownum_desc = 1 AND
                   latest_submit.TENANT_ID = r.TENANT_ID
        -- Amount
             LEFT JOIN activity_sums
                ON activity_sums.REQUEST_ID = r.ID AND 
                   activity_sums.TENANT_ID = r.TENANT_ID
        -- User
             JOIN RAW.HORIZON_PROD.HZN_IDENTITY_ZUSER usr
                ON usr.ID = r.OWNER_USER_ID
        -- Created Dates
             LEFT JOIN created_dates 
                ON created_dates.REQUEST_ID = r.ID
        -- Completed Dates
             LEFT JOIN completion_dates 
                ON completion_dates.REQUEST_ID = r.id
        -- Delta Restriction (parameter looks like: '2022-05-12 21:58:55')
             JOIN delta_determination dd
                ON dd.id = r.ID
        WHERE r.zstatus <> 'new_status' -- ensures we dont receive an unsaved request
          AND r.tenant_id = arg_tenant_id
        ORDER BY r.tenant_id, created_dates.CREATED_TIMESTAMP DESC
    $$;

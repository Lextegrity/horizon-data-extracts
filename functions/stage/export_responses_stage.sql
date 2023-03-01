CREATE OR REPLACE FUNCTION export_responses_stage (
    arg_tenant_id varchar
  , arg_start_date varchar
  , arg_end_date varchar
    ) RETURNS TABLE( 
        request_id varchar
      , request_activity_id varchar
      , party_id varchar
      , question_code varchar
      , library_item_code varchar
      , question_default_text varchar
      , response_data varchar
      , response_type varchar
      , response_order int
      , attachment_names varchar
      , modified_datetime timestamptz
      , customer_id varchar
    )
    RETURNS NULL ON NULL INPUT
    COMMENT = 'Produces the change in rows for `responses` that have occured between the start and end date for a given tenant'
    AS $$
         WITH id_insert_update AS (
            -- Request
            SELECT ID                       AS id
                 , TENANT_ID                AS tenant_id
                 , max(DB_INSERT_TIMESTAMP) AS insert_dt
                 , max(DB_UPDATE_TIMESTAMP) AS update_dt
            FROM RAW.HORIZON_STAGE.HZN_REQUEST_REQUEST
            WHERE TENANT_ID = arg_tenant_id
            GROUP BY 1, 2
            
            UNION
            
            -- Request Actions
            SELECT r.ID               AS id
                 , r.TENANT_ID        AS tenant_id
                 , max(aa.ZTIMESTAMP) AS insert_dt
                 , max(aa.ZTIMESTAMP) AS update_dt -- note insert/update are the same.
            FROM RAW.HORIZON_STAGE.HZN_REQUEST_REQUEST r
                     LEFT JOIN RAW.HORIZON_STAGE.HZN_REQUEST_REQUEST_STEP rs ON rs.REQUEST_ID = r.ID
                     LEFT JOIN RAW.HORIZON_STAGE.HZN_REQUEST_WORKFLOW_STEP rws ON rws.REQUEST_STEP_ID = rs.ID
                     LEFT JOIN RAW.HORIZON_STAGE.HZN_REQUEST_WORKFLOW_ACTION aa ON aa.WORKFLOW_STEP_ID = rws.ID
            WHERE r.TENANT_ID = arg_tenant_id
            GROUP BY 1, 2
            
            UNION
            
            -- Request Versions
            SELECT REQUEST_ID               AS id
                 , TENANT_ID                AS tenant_id
                 , max(DB_INSERT_TIMESTAMP) AS insert_dt
                 , max(DB_UPDATE_TIMESTAMP) AS update_dt
            FROM RAW.HORIZON_STAGE.HZN_REQUEST_REQUEST_HISTORY
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
                 ) as delta_date
            FROM agg_id_insert_update
            WHERE delta_date::timestamptz >= to_timestamp_tz(arg_start_date, 'YYYY-MM-DD HH:MI:SS')
              AND delta_date::timestamptz <= to_timestamp_tz(arg_end_date, 'YYYY-MM-DD HH:MI:SS')
              AND tenant_id = arg_tenant_id
        ),
        
        response_details AS (
            SELECT db_insert_timestamp
                 , db_insert_username
                 , db_update_timestamp
                 , db_update_username
                 , default_data
                 , id
                 , request_item_id
                 , request_version_id
                 , response_timestamp
                 , secondary_data
                 , tenant_id
                 , user_id
                 , COALESCE(
                     to_variant(zdata__de),
                     to_variant(zdata__it),
                     to_variant(zdata__st),
                     zdata__va
                   ) as zdata
             FROM RAW.HORIZON_STAGE.HZN_REQUEST_RESPONSE
             WHERE tenant_id = arg_tenant_id
        ),
        
        attachment_details AS (
            SELECT FK_ID
                 , listagg(UPLOADED_FILENAME, '**') as filenames
            FROM RAW.HORIZON_STAGE.HZN_REQUEST_FILE_ATTACHMENT
            WHERE FK_TABLE = 'response'
              AND TENANT_ID = arg_tenant_id
            GROUP BY FK_ID
        )
        
        SELECT DISTINCT r.ID                 AS request_id
                      , ra.ID                AS request_activity_id
                      , rrp.PARTY_ID         AS party_id
                      , ri.CODE              AS question_code
                      , ri.LIBRARY_ITEM_CODE AS library_item_code
                      , ri.DEFAULT_TEXT      AS question_default_text
                      , last_value(rs.ZDATA::varchar) OVER (
                            PARTITION BY r.ID, rq.ID, rq.CODE, ri.LIBRARY_ITEM_CODE, ri.CODE
                            ORDER BY rv.CREATE_TIMESTAMP
                            RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                        )                    AS response_data
                      , ri.RESPONSE_TYPE     AS response_type
                      , ri.ZORDER            AS response_order
                      , filenames            AS attachment_names
                      , dd.delta_date        AS modified_datetime
                      , r.TENANT_ID          AS customer_id
        FROM RAW.HORIZON_STAGE.HZN_REQUEST_REQUEST r
             JOIN RAW.HORIZON_STAGE.HZN_REQUEST_REQUEST_ACTIVITY ra 
                ON ra.REQUEST_ID = r.ID
             JOIN RAW.HORIZON_STAGE.HZN_REQUEST_REQUEST_QUESTIONNAIRE rq
                ON rq.REQUEST_ACTIVITY_ID = ra.ID AND 
                   rq.REQUEST_ID = ra.REQUEST_ID
             JOIN RAW.HORIZON_STAGE.HZN_REQUEST_REQUEST_ITEM ri
                ON ri.REQUEST_QUESTIONNAIRE_ID = rq.ID
             JOIN RAW.HORIZON_STAGE.HZN_REQUEST_REQUEST_VERSION rv
                  ON rv.REQUEST_QUESTIONNAIRE_ID = rq.ID
             JOIN response_details rs 
                  ON rs.REQUEST_ITEM_ID = ri.ID AND 
                     rs.REQUEST_VERSION_ID = rv.ID
             LEFT JOIN RAW.HORIZON_STAGE.HZN_REQUEST_REQUEST_ACTIVITY_PARTY rap
                  ON rap.REQUEST_PARTY_ID = rq.REQUEST_PARTY_ID AND
                     rap.REQUEST_ACTIVITY_ID = rq.REQUEST_ACTIVITY_ID
             LEFT JOIN RAW.HORIZON_STAGE.HZN_REQUEST_REQUEST_PARTY rrp
                  ON rrp.ID = rap.REQUEST_PARTY_ID AND
                     rrp.TENANT_ID = r.TENANT_ID
             LEFT JOIN attachment_details ad on ad.fk_id = rs.ID
            -- Delta Restriction (parameter looks like: '2022-05-12 21:58:55')
             JOIN delta_determination dd on dd.id = r.ID
        WHERE r.TENANT_ID = arg_tenant_id
        ORDER BY dd.delta_date DESC
    $$;

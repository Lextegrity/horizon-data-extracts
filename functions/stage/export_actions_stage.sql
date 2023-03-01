CREATE OR REPLACE FUNCTION export_actions_stage (
    arg_tenant_id varchar
  , arg_start_date varchar
  , arg_end_date varchar
    ) RETURNS TABLE( 
        id varchar
      , action_datetime timestamptz
      , actioned_by_party_id varchar
      , actioned_by_name varchar
      , actioned_by_role varchar
      , action_type varchar
      , request_id varchar
      , actioned_object_type varchar
      , actioned_object_id varchar
      , request_step_order int
      , request_step_name varchar
      , comment varchar
      , requires_mitigation boolean
      , voided boolean
      , modified_datetime timestamptz
      , customer_id varchar
    )
    RETURNS NULL ON NULL INPUT
    COMMENT = 'Produces the change in rows for `actions` that have occured between the start and end date for a given tenant'
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
            SELECT REQUEST_ID               as id
                 , TENANT_ID                as tenant_id
                 , max(DB_INSERT_TIMESTAMP) as insert_dt
                 , max(DB_UPDATE_TIMESTAMP) as update_dt
            from RAW.HORIZON_STAGE.HZN_REQUEST_REQUEST_HISTORY
            WHERE TENANT_ID = arg_tenant_id
            GROUP BY 1, 2
        ),
        
        agg_id_insert_update AS (
            SELECT id
                 , tenant_id
                 , max(insert_dt) as insert_dt
                 , max(update_dt) as update_dt
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
        )
        
        SELECT aa.ID                      AS id
             , aa.ZTIMESTAMP              AS action_datetime
             , usr.PARTY_ID               AS actioned_by_party_id
             , usr_party.FULL_NAME        AS actioned_by_name
             , wu.WORKFLOW_ROLE           AS actioned_by_role
             , sli.DEFAULT_TEXT           AS action_type
             , r.ID                       AS request_id
             , CASE
                   WHEN aa.REQUEST_ACTIVITY_ID      IS NOT null THEN 'Activity'
                   WHEN aa.REQUEST_PARTY_ID         IS NOT null THEN 'Party'
                   WHEN aa.REQUEST_QUESTIONNAIRE_ID IS NOT null THEN 'Questionnaire'
                   ELSE                                              'Request' 
               END AS actioned_object_type
             , CASE
                   WHEN aa.REQUEST_ACTIVITY_ID      IS NOT null THEN aa.request_activity_id
                   WHEN aa.REQUEST_PARTY_ID         IS NOT null THEN aa.request_party_id
                   WHEN aa.REQUEST_QUESTIONNAIRE_ID IS NOT null THEN aa.request_questionnaire_id
                   ELSE                                              aa.request_id 
               END as actioned_object_id
             , rs.ZORDER                  AS request_step_order
             , rs.DEFAULT_TITLE           AS request_step_name
             , rwc.ZCOMMENT               AS comment
             , aa.REQUIRES_MITIGATION     AS requires_mitigation
             , aa.VOIDED                  AS voided
             , dd.delta_date              AS modified_datetime
             , r.TENANT_ID                AS customer_id
        FROM RAW.HORIZON_STAGE.HZN_REQUEST_REQUEST r
             LEFT JOIN RAW.HORIZON_STAGE.HZN_REQUEST_REQUEST_STEP rs 
                ON rs.REQUEST_ID = r.ID
             LEFT JOIN RAW.HORIZON_STAGE.HZN_REQUEST_WORKFLOW_STEP rws 
                ON rws.REQUEST_STEP_ID = rs.ID
             LEFT JOIN RAW.HORIZON_STAGE.HZN_REQUEST_WORKFLOW_ACTION aa 
                ON aa.WORKFLOW_STEP_ID = RWS.ID
             JOIN RAW.HORIZON_STAGE.HZN_MASTER_SYSTEM_LIST_ITEM sli 
                ON sli.CODE = aa.ZTYPE
            -- Name of the Workflow Actioner
             LEFT JOIN RAW.HORIZON_STAGE.HZN_REQUEST_WORKFLOW_USER wu ON wu.id = aa.WORKFLOW_USER_ID
                AND wu.REQUEST_ID = aa.REQUEST_ID
                AND wu.WORKFLOW_STEP_ID = aa.WORKFLOW_STEP_ID
             LEFT JOIN RAW.HORIZON_STAGE.HZN_IDENTITY_ZUSER usr 
                ON usr.ID = wu.USER_ID
             LEFT JOIN RAW.HORIZON_STAGE.HZN_MASTER_INDIVIDUAL usr_party 
                ON usr_party.PARTY_ID = usr.PARTY_ID
            -- Comment
             LEFT JOIN RAW.HORIZON_STAGE.HZN_REQUEST_WORKFLOW_COMMENT rwc
                ON rwc.ID = aa.WORKFLOW_COMMENT_ID
            -- Delta Restriction (parameter looks like: '2022-05-12 21:58:55')
             JOIN delta_determination dd ON dd.id = r.ID
        WHERE r.ZSTATUS <> 'new_status' -- ensures we dont receive an unsaved request
          AND r.TENANT_ID = arg_tenant_id
        ORDER BY dd.delta_date DESC
    $$;

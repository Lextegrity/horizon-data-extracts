CREATE OR REPLACE FUNCTION export_users_stage (
    arg_tenant_id varchar
  , arg_start_date varchar
  , arg_end_date varchar
    ) RETURNS TABLE( 
        id varchar
      , name varchar
      , status varchar
      , function varchar
      , email varchar
      , username varchar
      , external_id varchar
      , country varchar
      , created_datetime timestamptz
      , modified_datetime timestamptz
      , customer_id varchar
    )
    RETURNS NULL ON NULL INPUT
    COMMENT = 'Produces the change in rows for `users` that have occured between the start and end date for a given tenant'
    AS $$
        WITH id_insert_update AS (
            -- User
            SELECT PARTY_ID                 AS id
                 , TENANT_ID                AS tenant_id
                 , max(DB_INSERT_TIMESTAMP) AS insert_dt
                 , max(DB_UPDATE_TIMESTAMP) AS update_dt
            FROM RAW.HORIZON_STAGE.HZN_IDENTITY_ZUSER
            WHERE TENANT_ID = arg_tenant_id
            GROUP BY 1, 2
            
            UNION
            
            -- Individual
            SELECT PARTY_ID                 as id
                 , TENANT_ID                as tenant_id
                 , max(DB_INSERT_TIMESTAMP) as insert_dt
                 , max(DB_UPDATE_TIMESTAMP) as update_dt
            FROM RAW.HORIZON_STAGE.HZN_MASTER_INDIVIDUAL
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
        )
        
        SELECT i.PARTY_ID            AS id
             , i.FULL_NAME           AS name
             , i.EMPLOYEE_STATUS     AS status
             , coalesce(
                 usr_function.DEFAULT_TEXT,
                 i.ZFUNCTION
             )                       AS function
             , p.EMAIL               AS email
             , z.USERNAME            AS username
             , i.EXTERNAL_ID         AS external_id
             , c.DEFAULT_NAME        AS country
             , z.DB_INSERT_TIMESTAMP AS created_datetime
             , dd.delta_date         AS modified_datetime
             , i.TENANT_ID           AS customer_id
             -- Not Sharing this detail at this time
        --      , i.DIVISION
        --      , i.BUSINESS_AREA
        --      , i.EMPLOYEE_CATEGORY
        --      , i.SUBFUNCTION
        FROM RAW.HORIZON_STAGE.HZN_IDENTITY_ZUSER z
            LEFT JOIN RAW.HORIZON_STAGE.HZN_MASTER_INDIVIDUAL i 
                ON i.PARTY_ID = z.PARTY_ID
        -- Department
            LEFT JOIN RAW.HORIZON_STAGE.HZN_MASTER_LIST_ITEM usr_function
                ON usr_function.CODE = i.ZFUNCTION AND 
                   usr_function.LIST_CODE = 'department' AND
                   usr_function.TENANT_ID = i.TENANT_ID
        -- Country
            LEFT JOIN RAW.HORIZON_STAGE.HZN_MASTER_PARTY_COUNTRY pc
                ON pc.PARTY_ID = i.PARTY_ID
            LEFT JOIN RAW.HORIZON_STAGE.HZN_MASTER_REF_COUNTRY c 
                ON c.CODE = pc.COUNTRY_CODE
            LEFT JOIN RAW.HORIZON_STAGE.HZN_MASTER_PARTY p
                ON i.PARTY_ID = p.ID
        -- Delta Restriction (parameter looks like: '2022-05-12 21:58:55')
            JOIN delta_determination dd on dd.id = z.PARTY_ID
        WHERE z.TENANT_ID = arg_tenant_id
        ORDER BY dd.delta_date DESC
    $$;

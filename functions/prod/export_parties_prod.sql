CREATE OR REPLACE FUNCTION export_parties_prod (
    arg_tenant_id varchar
  , arg_start_date varchar
  , arg_end_date varchar
    ) RETURNS TABLE( 
        id varchar
      , name varchar
      , name_informal varchar
      , source_type varchar
      , function varchar
      , subfunction varchar
      , is_worker boolean
      , is_govt boolean
      , is_hcx boolean
      , is_3p_or_vendor boolean
      , is_bank boolean
      , is_customer boolean
      , is_state_owned boolean
      , customer_id varchar
      , fmv_amount number
      , fmv_currency_code varchar
      , fmv_effective_date timestamptz
      , fmv_specialty_code varchar
      , fmv_tier_code varchar
      , modified_datetime timestamptz
      , email varchar
      , erp_id varchar
    )
    RETURNS NULL ON NULL INPUT
    COMMENT = 'Produces the change in rows for `actions` that have occured between the start and end date for a given tenant'
    AS $$
    WITH id_insert_update AS (
            -- Party
            SELECT ID                       AS id
                 , TENANT_ID                AS tenant_id
                 , max(DB_INSERT_TIMESTAMP) AS insert_dt
                 , max(DB_UPDATE_TIMESTAMP) AS update_dt
            FROM RAW.HORIZON_PROD.HZN_MASTER_PARTY
            WHERE TENANT_ID = arg_tenant_id
            GROUP BY 1, 2
            
            UNION
            
            -- Party Information
            SELECT PARTY_ID                 AS id
                 , TENANT_ID                AS tenant_id
                 , max(DB_INSERT_TIMESTAMP) AS insert_dt
                 , max(DB_UPDATE_TIMESTAMP) AS update_dt
            FROM RAW.HORIZON_PROD.HZN_MASTER_PARTY_INFORMATION
            WHERE tenant_id = arg_tenant_id
            GROUP BY 1, 2
            
            UNION
            
            -- Fair Market Value
            SELECT PARTY_ID                 AS id
                 , TENANT_ID                AS tenant_id
                 , max(DB_INSERT_TIMESTAMP) AS insert_dt
                 , max(DB_UPDATE_TIMESTAMP) AS update_dt
            FROM RAW.HORIZON_PROD.HZN_MASTER_PARTY_FAIR_MARKET_VALUE
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
        
        party_data AS (
            SELECT PARTY_ID               AS id
                 , FULL_NAME              AS name
                 , PREFERRED_FULL_NAME    AS name_informal
                 , 'Individual'           AS source_type
                 , ZFUNCTION              AS function
                 , SUBFUNCTION            AS subfunction
                 , IS_WORKER              AS is_worker
                 , IS_GOVERNMENT_OFFICIAL AS is_govt
                 , IS_HCP                 AS is_hcx
                 , IS_THIRD_PARTY_WORKER  AS is_3p_or_vendor
                 , null                   AS is_bank
                 , null                   AS is_customer
                 , null                   AS is_state_owned
                 , TENANT_ID              AS customer_id
            FROM RAW.HORIZON_PROD.HZN_MASTER_INDIVIDUAL
            WHERE TENANT_ID = arg_tenant_id
            
            UNION
            
            SELECT PARTY_ID             AS id
                 , OFFICIAL_NAME        AS name
                 , INFORMAL_NAME        AS name_informal
                 , 'Organization'       AS source_type
                 , null                 AS function
                 , null                 AS subfunction
                 , null                 AS is_worker
                 , IS_GOVERNMENT_ENTITY AS is_govt
                 , IS_HCO               AS is_hcx
                 , IS_VENDOR            AS is_3p_or_vendor
                 , IS_BANK              AS is_bank
                 , IS_CUSTOMER          AS is_customer
                 , IS_STATE_OWNED       AS is_state_owned
                 , TENANT_ID            AS customer_id
            FROM RAW.HORIZON_PROD.HZN_MASTER_ORGANIZATION
            WHERE TENANT_ID = arg_tenant_id
        )
        
        SELECT pd.*
             , fmv.AMOUNT         AS fmv_amount
             , fmv.CURRENCY_CODE  AS fmv_currency_code
             , fmv.EFFECTIVE_DATE AS fmv_effective_date
             , fmv.SPECIALTY_CODE AS fmv_specialty_code
             , fmv.TIER_CODE      AS fmv_tier_code
             , delta_date         AS modified_datetime
             , party.email        AS email
             , party.erp_id       AS erp_id
        FROM party_data pd
            LEFT JOIN RAW.HORIZON_PROD.HZN_MASTER_PARTY_FAIR_MARKET_VALUE fmv 
                ON fmv.PARTY_ID = pd.id
            LEFT JOIN RAW.HORIZON_PROD.HZN_MASTER_PARTY party
                ON party.id = pd.id AND
                   party.tenant_id = pd.customer_id
            -- Delta Restriction (parameter looks like: '2022-05-12 21:58:55')
            JOIN delta_determination dd ON pd.id = dd.id
        ORDER BY dd.delta_date DESC
    $$;

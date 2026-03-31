-- Delta slice: rows in stg_customers that were added *since* BEFORE_DELTA (sync date > max in clone).
-- When no new load happened, this returns 0 rows so delta_validation passes (before_cnt = after_cnt).
SELECT
       "CUSTOMER_ID",
       "ADDRESS",
       "REGION_CODE",
       "PRIMARY_PHONE",
       "ANNUAL_REVENUE",
       "SALES_BLOCK_FLAG",
       "ADDRESS_ID",
       "ACCOUNT_GROUP",
       "POSTING_BLOCK_FLAG",
       "LANGUAGE_CODE",
       "ORDER_BLOCK_FLAG",
       "SALES_REP_TYPE",
       "SALES_REP_ROLE",
       "SALES_COMPANY_CODE",
       "SALES_REP_NAME",
       "CREATED_USER",
       "CREATED_TIMESTAMP",
       "FIVETRANSYNC_DATE"
FROM {{ ref('stg_customers') }}
WHERE "FIVETRANSYNC_DATE" > (
  SELECT COALESCE(MAX("FIVETRANSYNC_DATE"), '1900-01-01')
  FROM {{ get_before_delta_table_ref('stg_customers', '_BEFORE_DELTA') }}
)

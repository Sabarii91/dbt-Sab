{{ config(
    materialized='incremental',
    unique_key='CUSTOMER_ID',
    pre_hook=[
      "{{ before_delta_clone_pre_hook() }}"
    ],
    post_hook=[
      "{{ before_delta_clone_post_hook() }}"
    ]
) }}

{% set buffer_seconds = var('incremental_buffer_seconds', 0) %}

with watermark as (
    {% if is_incremental() %}
        select dateadd('second', -{{ buffer_seconds }}, 
                      coalesce(max(FIVETRANSYNC_DATE), 
                              to_timestamp('1900-01-01 00:00:00'))) as last_load_ts
        from {{ this }}
    {% else %}
        select to_timestamp('1900-01-01 00:00:00') as last_load_ts
    {% endif %}
)

SELECT
       "CUST_NBR" AS "CUSTOMER_ID",
       street || ' ' || "CITY" || ' ' || po_cd AS "ADDRESS",
       "REGION_KEY" AS "REGION_CODE",
       "TEL_NBR_1" AS "PRIMARY_PHONE",
       "ANNUAL_SALES" AS "ANNUAL_REVENUE",
       COALESCE("CENTRAL_SALES_BLK", NULL) AS "SALES_BLOCK_FLAG",
       "ADRS_NBR" AS "ADDRESS_ID",
       "ACCT_GRP" AS "ACCOUNT_GROUP",
       "CENTRAL_POST_BLK" AS "POSTING_BLOCK_FLAG",
       TRIM("LANG_KEY") AS "LANGUAGE_CODE",
       "CENTRAL_ORDR_BLK" AS "ORDER_BLOCK_FLAG",
       REPLACE("SALES_REP_TP",'C','Z') AS "SALES_REP_TYPE",
       {{ map_sales_rep_role('SALES_REP_ROLE_CLASSIFICATION') }} AS "SALES_REP_ROLE",
       "SALES_REP_CMPNY_CD" AS "SALES_COMPANY_CODE",
       LOWER("SALES_REP_MISC_CD") AS "SALES_REP_NAME",
       "CREATED_BY" AS "CREATED_USER",
       TO_CHAR("CREATED_DT", 'YYYY-MM-DD') AS "CREATED_TIMESTAMP",
       "FIVETRANSYNC_DATE"
FROM {{ source('raw', 'CUST_SALES') }}

{% if is_incremental() %}
CROSS JOIN watermark
WHERE "FIVETRANSYNC_DATE" > watermark.last_load_ts
{% endif %}

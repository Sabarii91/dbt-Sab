{#
  Expected delta rows for stg_material.

  Like `stg_material_delta_slice`, this selects NEW/UPDATED rows compared
  to the stg_material_BEFORE_DELTA clone, but it is derived directly from
  the raw MAT_ATXT transform.

  This lets the custom Option A runner validate that the transformed delta
  matches what landed in `stg_material`.
#}

with before_rel as (
  select *
  from {{ get_before_delta_table_ref('stg_material', '_BEFORE_DELTA') }}
),
src as (
  select
    "PRODUCT"       AS "M_NBR",
    "PRODHIERARCHY" AS "LVL_CD",
    "PRODGROUP"     AS "M_GRP",
    "PRODOLDID"     AS "OM_NBR",
    "PRODTYPE"      AS "M_TP",
    "TSHELFLIFE"    AS "T_SH_LF",
    "MSHELF"        AS "M_SH_LF",
    Upper("BUNIT")  AS "BA_UOM",
    CASE
      WHEN "ISBMREQUIRED"='True' THEN 'X'
      ELSE ' '
    END AS "B_M_IND"
  from {{ source('raw', 'MAT_ATXT') }}
)
select
  s."M_NBR",
  s."LVL_CD",
  s."M_GRP",
  s."OM_NBR",
  s."M_TP",
  s."T_SH_LF",
  s."M_SH_LF",
  s."BA_UOM",
  s."B_M_IND"
from src s
left join before_rel b
  on s."M_NBR" = b."M_NBR"
where
  b."M_NBR" IS NULL
  OR s."LVL_CD" IS DISTINCT FROM b."LVL_CD"
  OR s."M_GRP" IS DISTINCT FROM b."M_GRP"
  OR s."OM_NBR" IS DISTINCT FROM b."OM_NBR"
  OR s."M_TP" IS DISTINCT FROM b."M_TP"
  OR s."T_SH_LF" IS DISTINCT FROM b."T_SH_LF"
  OR s."M_SH_LF" IS DISTINCT FROM b."M_SH_LF"
  OR s."BA_UOM" IS DISTINCT FROM b."BA_UOM"
  OR s."B_M_IND" IS DISTINCT FROM b."B_M_IND"

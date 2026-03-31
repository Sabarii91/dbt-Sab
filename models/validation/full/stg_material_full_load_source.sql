{#
  Full-load reference transform for stg_material.

  Used by Option A full-load comprehensive_compare.
#}

SELECT
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
FROM {{ source('raw', 'MAT_ATXT') }}

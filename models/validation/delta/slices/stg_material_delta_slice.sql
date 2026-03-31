{#
  Delta slice for material: return only NEW and UPDATED rows compared to the
  stg_material_BEFORE_DELTA clone.

  This avoids relying on a specific watermark column in MAT_ATXT; instead, it uses
  null-safe column comparisons on all non-key fields.
#}

SELECT
  s."M_NBR",
  s."LVL_CD",
  s."M_GRP",
  s."OM_NBR",
  s."M_TP",
  s."T_SH_LF",
  s."M_SH_LF",
  s."BA_UOM",
  s."B_M_IND"
FROM {{ ref('stg_material') }} s
LEFT JOIN {{ get_before_delta_table_ref('stg_material', '_BEFORE_DELTA') }} b
  ON s."M_NBR" = b."M_NBR"
WHERE
  b."M_NBR" IS NULL
  OR s."LVL_CD" IS DISTINCT FROM b."LVL_CD"
  OR s."M_GRP" IS DISTINCT FROM b."M_GRP"
  OR s."OM_NBR" IS DISTINCT FROM b."OM_NBR"
  OR s."M_TP" IS DISTINCT FROM b."M_TP"
  OR s."T_SH_LF" IS DISTINCT FROM b."T_SH_LF"
  OR s."M_SH_LF" IS DISTINCT FROM b."M_SH_LF"
  OR s."BA_UOM" IS DISTINCT FROM b."BA_UOM"
  OR s."B_M_IND" IS DISTINCT FROM b."B_M_IND"

{#
  Shared hooks for maintaining BEFORE_DELTA clone tables used by delta validation.
  - Pre-hook: on incremental runs, clone current target state before merge/update.
  - Post-hook: on full refresh runs, clone rebuilt target as baseline.
#}

{% macro before_delta_clone_pre_hook() %}
  {% if is_incremental() %}
    CREATE OR REPLACE TRANSIENT TABLE {{ this.database }}.{{ var('before_delta_schema', 'QA') }}.{{ this.identifier }}_BEFORE_DELTA
    CLONE {{ this }}
  {% endif %}
{% endmacro %}

{% macro before_delta_clone_post_hook() %}
  {% if not is_incremental() %}
    CREATE OR REPLACE TRANSIENT TABLE {{ this.database }}.{{ var('before_delta_schema', 'QA') }}.{{ this.identifier }}_BEFORE_DELTA
    CLONE {{ this }}
  {% endif %}
{% endmacro %}

{#
  Dynamic validation runner driven by a seed CSV (default: test_config_full_load).

  Outputs:
    1) Row-level failure tables (existing behavior):
       - comprehensive_compare_<model_name>_failures
       - delta_compare_<model_name>_failures
    2) Audit tables (new):
       - validation_audit_log
       - validation_anomaly_log
#}
{% macro run_validation_tests(
  config_seed='test_config_full_load',
  qa_anchor_model=none,
  validation_schema=none,
  validation_database=none,
  model_name_filter=none,
  model_name_like=none,
  test_type_filter=none
) %}
  {% set config_relation = ref(config_seed) %}
  {# Resolve audit target dynamically:
     1) qa_anchor_model when provided
     2) explicit args validation_database/validation_schema
     3) target.database + var('validation_schema', 'QA') fallback #}
  {% if qa_anchor_model %}
    {% set qa_anchor = ref(qa_anchor_model) %}
    {% set qa_db = qa_anchor.database %}
    {% set qa_schema = qa_anchor.schema %}
  {% else %}
    {% set qa_db = validation_database if validation_database else target.database %}
    {% set qa_schema = validation_schema if validation_schema else var('validation_schema', 'QA') %}
  {% endif %}
  {% set qa_prefix = (qa_db ~ '.' if qa_db else '') ~ qa_schema ~ '.' %}

  {% set audit_run_id = run_started_at.strftime('%Y%m%d%H%M%S') ~ '_' ~ invocation_id[:8] %}
  {% set escaped_model_name_filter = model_name_filter | replace("'", "''") if model_name_filter else none %}
  {% set escaped_model_name_like = model_name_like | replace("'", "''") if model_name_like else none %}
  {% set escaped_test_type_filter = test_type_filter | replace("'", "''") if test_type_filter else none %}
  {% set config_sql %}
    select *
    from {{ config_relation }}
    where 1=1
      {% if escaped_model_name_filter %}
      and model_name = '{{ escaped_model_name_filter }}'
      {% endif %}
      {% if escaped_model_name_like %}
      and model_name like '{{ escaped_model_name_like }}'
      {% endif %}
      {% if escaped_test_type_filter %}
      and test_type = '{{ escaped_test_type_filter }}'
      {% endif %}
    order by model_name, test_type
  {% endset %}
  {% set config_result = run_query(config_sql) %}

  {% if execute %}
    {% set init_audit_tables_sql %}
      create table if not exists {{ qa_prefix }}validation_audit_log (
        audit_run_id string,
        execution_id string,
        audit_ts timestamp_ntz,
        model_name string,
        test_type string,
        load_type string,
        status string,
        source_model string,
        source_row_count number,
        target_model string,
        target_row_count_before number,
        target_row_count_after number,
        rows_compared number,
        unique_keys_compared number,
        duplicate_groups_count number,
        duplicate_rows_count number,
        match_count number,
        anomaly_count number,
        delta_new_count number,
        delta_updated_count number,
        execution_duration_ms number,
        error_message string
      );

      create table if not exists {{ qa_prefix }}validation_anomaly_log (
        total_anomalies_count number,
        value_mismatch_count number,
        missing_in_target_count number,
        missing_in_source_count number,
        source_duplicate_count number,
        target_duplicate_count number,
        duplicate_no_match_count number,
        duplicate_partial_match_count number,
        duplicate_full_match_count number,
        delta_new_count number,
        delta_updated_count number,
        execution_id string,
        audit_run_id string,
        audit_ts timestamp_ntz,
        status string,
        model_name string,
        test_query_model string,
        compare_mode string,
        load_type string
      );

      alter table {{ qa_prefix }}validation_audit_log add column if not exists test_type string;
      alter table {{ qa_prefix }}validation_audit_log add column if not exists source_model string;
      alter table {{ qa_prefix }}validation_audit_log add column if not exists target_model string;
      alter table {{ qa_prefix }}validation_audit_log add column if not exists target_row_count_before number;
      alter table {{ qa_prefix }}validation_audit_log add column if not exists target_row_count_after number;
      alter table {{ qa_prefix }}validation_audit_log add column if not exists unique_keys_compared number;
      alter table {{ qa_prefix }}validation_audit_log add column if not exists duplicate_groups_count number;
      alter table {{ qa_prefix }}validation_audit_log add column if not exists duplicate_rows_count number;
      alter table {{ qa_prefix }}validation_audit_log add column if not exists match_count number;
      alter table {{ qa_prefix }}validation_audit_log add column if not exists anomaly_count number;
      alter table {{ qa_prefix }}validation_audit_log add column if not exists delta_new_count number;
      alter table {{ qa_prefix }}validation_audit_log add column if not exists delta_updated_count number;
      alter table {{ qa_prefix }}validation_anomaly_log add column if not exists delta_new_count number;
      alter table {{ qa_prefix }}validation_anomaly_log add column if not exists delta_updated_count number;
      alter table {{ qa_prefix }}validation_anomaly_log add column if not exists duplicate_no_match_count number;
      alter table {{ qa_prefix }}validation_anomaly_log add column if not exists duplicate_partial_match_count number;
      alter table {{ qa_prefix }}validation_anomaly_log add column if not exists duplicate_full_match_count number;

      create or replace view {{ qa_prefix }}v_validation_audit_log as
      select
        audit_ts,
        audit_run_id,
        execution_id,
        load_type,
        test_type,
        status,
        model_name,
        source_model,
        target_model,
        source_row_count,
        target_row_count_before,
        target_row_count_after,
        rows_compared,
        unique_keys_compared,
        duplicate_groups_count,
        duplicate_rows_count,
        match_count,
        anomaly_count,
        delta_new_count,
        delta_updated_count,
        execution_duration_ms,
        error_message
      from {{ qa_prefix }}validation_audit_log;

      create or replace view {{ qa_prefix }}v_validation_anomaly_log as
      select
        audit_ts,
        audit_run_id,
        execution_id,
        load_type,
        compare_mode,
        status,
        model_name,
        test_query_model,
        total_anomalies_count,
        delta_new_count,
        delta_updated_count,
        value_mismatch_count,
        missing_in_target_count,
        missing_in_source_count,
        duplicate_no_match_count,
        duplicate_partial_match_count,
        duplicate_full_match_count,
        source_duplicate_count,
        target_duplicate_count
      from {{ qa_prefix }}validation_anomaly_log;
    {% endset %}
    {% do run_query(init_audit_tables_sql) %}
  {% endif %}

  {% if execute and config_result and config_result.rows | length > 0 %}
    {% set ns = namespace(ran=0) %}
    {% for row in config_result.rows %}
      {% set execution_id = audit_run_id ~ '_' ~ loop.index %}
      {% set started_at = modules.datetime.datetime.utcnow() %}

      {% set model_name = row.get('MODEL_NAME') or row.get('model_name') %}
      {% set test_type = row.get('TEST_TYPE') or row.get('test_type') %}
      {% set key_column = row.get('KEY_COLUMN') or row.get('key_column') %}
      {% set test_query_model = row.get('TEST_QUERY_MODEL') or row.get('test_query_model') %}
      {% set compare_columns_ref = row.get('COMPARE_COLUMNS_REF') or row.get('compare_columns_ref') %}
      {% set compare_columns_str = row.get('COMPARE_COLUMNS') or row.get('compare_columns') %}
      {% set base_model = row.get('BASE_MODEL') or row.get('base_model') %}
      {% set clone_suffix = row.get('CLONE_SUFFIX') or row.get('clone_suffix') %}
      {% set config_set = row.get('CONFIG_SET') or row.get('config_set') %}

      {% set ns_key_cols = namespace(key_cols=[]) %}
      {% if key_column %}
        {% for k in key_column.split(',') %}
          {% if k | trim %}
            {% set ns_key_cols.key_cols = ns_key_cols.key_cols + [k | trim] %}
          {% endif %}
        {% endfor %}
      {% endif %}
      {% set key_cols = ns_key_cols.key_cols %}
      {% set key_arg = key_cols[0] if key_cols | length == 1 else key_cols %}

      {% if key_cols | length == 0 %}
        {% do exceptions.raise_compiler_error(
          'run_validation_tests: KEY_COLUMN is empty for model_name=' ~ (model_name | string) ~ ', test_type=' ~ (test_type | string)
        ) %}
      {% endif %}

      {% set ns_compare_list = namespace(compare_list=[]) %}
      {% if not compare_columns_str and compare_columns_ref %}
        {% set compare_columns_config_sql %}
          select compare_columns
          from {{ ref('compare_columns_config') }}
          where compare_columns_ref = '{{ compare_columns_ref }}'
          limit 1
        {% endset %}
        {% set compare_columns_config_result = run_query(compare_columns_config_sql) %}
        {% if compare_columns_config_result and compare_columns_config_result.rows | length > 0 %}
          {% set compare_columns_str = compare_columns_config_result.rows[0]['COMPARE_COLUMNS'] if 'COMPARE_COLUMNS' in compare_columns_config_result.rows[0].keys() else compare_columns_config_result.rows[0]['compare_columns'] %}
        {% endif %}
      {% endif %}
      {% if compare_columns_str %}
        {% for x in compare_columns_str.split(',') %}
          {% if x | trim %}
            {% set ns_compare_list.compare_list = ns_compare_list.compare_list + [x | trim] %}
          {% endif %}
        {% endfor %}
      {% endif %}
      {% set compare_list = ns_compare_list.compare_list %}
      {% set compare_arg = compare_list if compare_list | length > 0 else none %}

      {% set resolved_base_model = base_model %}
      {% if not resolved_base_model and model_name.endswith('_delta_slice') %}
        {% set resolved_base_model = model_name.replace('_delta_slice', '') %}
      {% endif %}

      {% set load_type = config_set if config_set else ('DELTA' if test_type == 'delta_compare' else 'FULL_LOAD') %}

      {% set staging_count_sql %}select count(*) as c from {{ ref(model_name) }}{% endset %}
      {% set staging_count_res = run_query(staging_count_sql) %}
      {% set staging_row_count = staging_count_res.rows[0][0] if staging_count_res and staging_count_res.rows | length > 0 else 'null' %}

      {% set audit_source_model = model_name %}
      {% set audit_source_row_count = staging_row_count %}
      {% set audit_target_model = none %}
      {% set before_delta_count = 'null' %}
      {% set after_delta_count = 'null' %}
      {% set audit_target_row_count_before = 'null' %}
      {% set audit_target_row_count_after = 'null' %}

      {% set total_anomalies_count = 0 %}
      {% set missing_in_target_count = 0 %}
      {% set missing_in_source_count = 0 %}
      {% set value_mismatch_count = 0 %}
      {% set source_duplicate_count = 0 %}
      {% set target_duplicate_count = 0 %}
      {% set delta_new_count = 0 %}
      {% set delta_updated_count = 0 %}
      {% set test_status = 'MATCH' %}
      {% set error_message = none %}
      {% set rows_compared = 0 %}
      {% set unique_keys_compared = 'null' %}
      {% set duplicate_groups_count = 'null' %}
      {% set duplicate_rows_count = 'null' %}

      {% if test_type == 'comprehensive_compare' %}
        {% if not test_query_model %}
          {% do exceptions.raise_compiler_error(
            'run_validation_tests: TEST_QUERY_MODEL is required for comprehensive_compare, model_name=' ~ (model_name | string)
          ) %}
        {% endif %}

        {% set target_count_sql %}select count(*) as c from {{ ref(test_query_model) }}{% endset %}
        {% set target_count_res = run_query(target_count_sql) %}
        {% set target_query_row_count = target_count_res.rows[0][0] if target_count_res and target_count_res.rows | length > 0 else 'null' %}

        {% set audit_source_model = test_query_model %}
        {% set audit_source_row_count = target_query_row_count %}
        {% set audit_target_model = model_name %}
        {% set audit_target_row_count_before = 'null' %}
        {% set audit_target_row_count_after = staging_row_count %}

        {% if resolved_base_model and model_name.endswith('_delta_slice') %}
          {% set before_count_sql %}select count(*) as c from {{ get_before_delta_table_ref(resolved_base_model, clone_suffix if clone_suffix else '_BEFORE_DELTA') }}{% endset %}
          {% set before_count_res = run_query(before_count_sql) %}
          {% if before_count_res and before_count_res.rows | length > 0 %}
            {% set before_delta_count = before_count_res.rows[0][0] %}
          {% endif %}

          {% set after_count_sql %}select count(*) as c from {{ ref(resolved_base_model) }}{% endset %}
          {% set after_count_res = run_query(after_count_sql) %}
          {% if after_count_res and after_count_res.rows | length > 0 %}
            {% set after_delta_count = after_count_res.rows[0][0] %}
          {% endif %}

          {% set audit_target_model = resolved_base_model %}
          {% set audit_target_row_count_before = before_delta_count %}
          {% set audit_target_row_count_after = after_delta_count %}
        {% endif %}

        {% set failures_table %}{{ qa_prefix }}comprehensive_compare_{{ model_name }}_failures{% endset %}
        {% set compare_subquery %}
          {{ comprehensive_compare(
              model=ref(model_name),
              test_query=ref(test_query_model),
              key_columns=key_arg,
              compare_columns=compare_arg,
              fail_on_duplicates=True,
              target_name=test_query_model
          ) }}
        {% endset %}
        {% set compare_failures_subquery %}
          select *
          from ({{ compare_subquery }}) s
          where anomaly_type not in (
            'DUPLICATE_IN_SOURCE_INFO',
            'DUPLICATE_IN_TARGET_INFO'
          )
        {% endset %}

        {% set create_failures_sql %}
          create table if not exists {{ failures_table }} as
          select
            '{{ execution_id }}' as execution_id,
            '{{ audit_run_id }}' as audit_run_id,
            '{{ model_name }}' as model_name,
            current_timestamp() as run_at,
            s.*
          from ({{ compare_failures_subquery }}) s
          limit 0
        {% endset %}

        {% set insert_failures_sql %}
          insert into {{ failures_table }}
          select
            '{{ execution_id }}' as execution_id,
            '{{ audit_run_id }}' as audit_run_id,
            '{{ model_name }}' as model_name,
            current_timestamp() as run_at,
            s.*
          from ({{ compare_failures_subquery }}) s
        {% endset %}

        {% set anomaly_count_sql %}
          select
            sum(case when anomaly_type not in ('DUPLICATE_IN_SOURCE_INFO', 'DUPLICATE_IN_TARGET_INFO') then 1 else 0 end) as total_anomalies_count,
            sum(case when anomaly_type = 'MISSING_IN_TARGET'  then 1 else 0 end) as missing_in_target_count,
            sum(case when anomaly_type = 'MISSING_IN_SOURCE'  then 1 else 0 end) as missing_in_source_count,
            sum(case when anomaly_type = 'VALUE_MISMATCH'     then 1 else 0 end) as value_mismatch_count,
            sum(case when anomaly_type in ('DUPLICATE_IN_SOURCE', 'DUPLICATE_IN_SOURCE_INFO') then 1 else 0 end) as source_duplicate_count,
            sum(case when anomaly_type in ('DUPLICATE_IN_TARGET', 'DUPLICATE_IN_TARGET_INFO') then 1 else 0 end) as target_duplicate_count
          from ({{ compare_subquery }}) c
        {% endset %}

        {% set failures_view %}{{ qa_prefix }}v_comprehensive_compare_{{ model_name }}_failures{% endset %}
        {% set create_failures_view_sql %}
          create or replace view {{ failures_view }} as
          select
            run_at,
            audit_run_id,
            execution_id,
            model_name,
            source_model,
            target_model,
            {% for key in key_cols %}
            {{ adapter.quote(key) }},
            {% endfor %}
            anomaly_type,
            anomaly_description,
            source_key_count,
            target_key_count,
            matched_row_count,
            total_anomalies_count,
            anomaly_type_count,
            detected_at,
            source_hash,
            target_hash
            {% for col in compare_list %}
            , source_{{ col }}
            , target_{{ col }}
            {% endfor %}
          from {{ failures_table }}
        {% endset %}

        {% do run_query(create_failures_sql) %}
        {% do run_query(create_failures_view_sql) %}
        {% do run_query(insert_failures_sql) %}
        {% set anomaly_count_res = run_query(anomaly_count_sql) %}

        {% if anomaly_count_res and anomaly_count_res.rows | length > 0 %}
          {% set total_anomalies_count   = anomaly_count_res.rows[0][0] or 0 %}
          {% set missing_in_target_count = anomaly_count_res.rows[0][1] or 0 %}
          {% set missing_in_source_count = anomaly_count_res.rows[0][2] or 0 %}
          {% set value_mismatch_count    = anomaly_count_res.rows[0][3] or 0 %}
          {% set source_duplicate_count  = anomaly_count_res.rows[0][4] or 0 %}
          {% set target_duplicate_count  = anomaly_count_res.rows[0][5] or 0 %}
        {% endif %}

        {# Count unique vs duplicate keys from source to produce accurate match_count #}
        {% set key_stats_sql %}
          with key_counts as (
            select
              {{ _comprehensive_compare_key_hash_v9(key_cols) }} as row_key_hash,
              count(*) as cnt
            from {{ ref(model_name) }}
            group by row_key_hash
          )
          select
            sum(case when cnt = 1 then 1 else 0 end)   as unique_keys_count,
            sum(case when cnt > 1 then 1 else 0 end)   as duplicate_groups_count,
            sum(case when cnt > 1 then cnt else 0 end)  as duplicate_rows_count
          from key_counts
        {% endset %}
        {% set key_stats_res = run_query(key_stats_sql) %}
        {% if key_stats_res and key_stats_res.rows | length > 0 %}
          {% set unique_keys_compared    = key_stats_res.rows[0][0] or 0 %}
          {% set duplicate_groups_count  = key_stats_res.rows[0][1] or 0 %}
          {% set duplicate_rows_count    = key_stats_res.rows[0][2] or 0 %}
        {% endif %}

        {% set rows_compared = staging_row_count %}
        {% set test_status = 'MISMATCH' if total_anomalies_count > 0 else 'MATCH' %}
        {% set ns.ran = ns.ran + 1 %}

      {% elif test_type == 'delta_compare' %}
        {% if not resolved_base_model %}
          {% do exceptions.raise_compiler_error(
            'run_validation_tests: BASE_MODEL is required for delta_compare, model_name=' ~ (model_name | string)
          ) %}
        {% endif %}

        {% set before_count_sql %}select count(*) as c from {{ get_before_delta_table_ref(resolved_base_model, clone_suffix if clone_suffix else '_BEFORE_DELTA') }}{% endset %}
        {% set before_count_res = run_query(before_count_sql) %}
        {% if before_count_res and before_count_res.rows | length > 0 %}
          {% set before_delta_count = before_count_res.rows[0][0] %}
        {% endif %}

        {% set after_count_sql %}select count(*) as c from {{ ref(resolved_base_model) }}{% endset %}
        {% set after_count_res = run_query(after_count_sql) %}
        {% if after_count_res and after_count_res.rows | length > 0 %}
          {% set after_delta_count = after_count_res.rows[0][0] %}
        {% endif %}

        {% set audit_source_model = model_name %}
        {% set audit_source_row_count = staging_row_count %}
        {% set audit_target_model = resolved_base_model %}
        {% set audit_target_row_count_before = before_delta_count %}
        {% set audit_target_row_count_after = after_delta_count %}

        {% set failures_table %}{{ qa_prefix }}delta_compare_{{ model_name }}_failures{% endset %}
        {% set delta_subquery %}
          {{ delta_compare(
              model=ref(model_name),
              base_model=resolved_base_model,
              clone_suffix=(clone_suffix if clone_suffix else '_BEFORE_DELTA'),
              key_columns=key_arg,
              compare_columns=compare_arg
          ) }}
        {% endset %}

        {% set create_failures_sql %}
          create table if not exists {{ failures_table }} as
          select
            '{{ execution_id }}' as execution_id,
            '{{ audit_run_id }}' as audit_run_id,
            '{{ model_name }}' as model_name,
            current_timestamp() as run_at,
            s.*
          from ({{ delta_subquery }}) s
          limit 0
        {% endset %}

        {% set insert_failures_sql %}
          insert into {{ failures_table }}
          select
            '{{ execution_id }}' as execution_id,
            '{{ audit_run_id }}' as audit_run_id,
            '{{ model_name }}' as model_name,
            current_timestamp() as run_at,
            s.*
          from ({{ delta_subquery }}) s
        {% endset %}

        {% set delta_count_sql %}
          select
            count(*) as total_anomalies_count,
            sum(case when change_type = 'NEW' then 1 else 0 end) as delta_new_count,
            sum(case when change_type = 'UPDATED' then 1 else 0 end) as delta_updated_count
          from ({{ delta_subquery }}) d
        {% endset %}

        {% set failures_view %}{{ qa_prefix }}v_delta_compare_{{ model_name }}_failures{% endset %}
        {% set create_failures_view_sql %}
          create or replace view {{ failures_view }} as
          select
            run_at,
            audit_run_id,
            execution_id,
            model_name,
            {% for key in key_cols %}
            {{ adapter.quote(key) }},
            {% endfor %}
            change_type,
            key_classification,
            columns_changed,
            total_changes_count,
            change_type_count,
            after_delta_row_hash,
            before_delta_row_hash
            {% for col in compare_list %}
            , {{ adapter.quote('after_delta_'  ~ col) }}
            , {{ adapter.quote('before_delta_' ~ col) }}
            {% endfor %}
          from {{ failures_table }}
        {% endset %}

        {% do run_query(create_failures_sql) %}
        {% do run_query(create_failures_view_sql) %}
        {% do run_query(insert_failures_sql) %}
        {% set delta_count_res = run_query(delta_count_sql) %}

        {% if delta_count_res and delta_count_res.rows | length > 0 %}
          {% set total_anomalies_count = delta_count_res.rows[0][0] or 0 %}
          {% set delta_new_count = delta_count_res.rows[0][1] or 0 %}
          {% set delta_updated_count = delta_count_res.rows[0][2] or 0 %}
        {% endif %}

        {% set rows_compared = staging_row_count %}
        {% set test_status = 'MISMATCH' if total_anomalies_count > 0 else 'MATCH' %}
        {% set ns.ran = ns.ran + 1 %}

      {% elif test_type == 'hard_delete_check' %}
        {% if not resolved_base_model %}
          {% do exceptions.raise_compiler_error(
            'run_validation_tests: BASE_MODEL is required for hard_delete_check, model_name=' ~ (model_name | string)
          ) %}
        {% endif %}

        {# ── Count rows in BEFORE_DELTA clone ───────────────────────────────── #}
        {% set before_count_sql %}select count(*) as c from {{ get_before_delta_table_ref(resolved_base_model, clone_suffix if clone_suffix else '_BEFORE_DELTA') }}{% endset %}
        {% set before_count_res = run_query(before_count_sql) %}
        {% if before_count_res and before_count_res.rows | length > 0 %}
          {% set before_delta_count = before_count_res.rows[0][0] %}
        {% endif %}

        {# ── Count rows in current target (after delta) ──────────────────────── #}
        {% set after_count_sql %}select count(*) as c from {{ ref(resolved_base_model) }}{% endset %}
        {% set after_count_res = run_query(after_count_sql) %}
        {% if after_count_res and after_count_res.rows | length > 0 %}
          {% set after_delta_count = after_count_res.rows[0][0] %}
        {% endif %}

        {% set audit_source_model = resolved_base_model %}
        {% set audit_source_row_count = before_delta_count %}
        {% set audit_target_model = resolved_base_model %}
        {% set audit_target_row_count_before = before_delta_count %}
        {% set audit_target_row_count_after = after_delta_count %}

        {% set failures_table %}{{ qa_prefix }}hard_delete_{{ model_name }}_failures{% endset %}
        {% set delete_subquery %}
          {{ hard_delete_compare(
              model=ref(resolved_base_model),
              base_model=resolved_base_model,
              clone_suffix=(clone_suffix if clone_suffix else '_BEFORE_DELTA'),
              key_columns=key_arg,
              compare_columns=compare_arg
          ) }}
        {% endset %}

        {% set create_failures_sql %}
          create table if not exists {{ failures_table }} as
          select
            '{{ execution_id }}' as execution_id,
            '{{ audit_run_id }}' as audit_run_id,
            '{{ model_name }}' as model_name,
            current_timestamp() as run_at,
            s.*
          from ({{ delete_subquery }}) s
          limit 0
        {% endset %}

        {% set insert_failures_sql %}
          insert into {{ failures_table }}
          select
            '{{ execution_id }}' as execution_id,
            '{{ audit_run_id }}' as audit_run_id,
            '{{ model_name }}' as model_name,
            current_timestamp() as run_at,
            s.*
          from ({{ delete_subquery }}) s
        {% endset %}

        {% set delete_count_sql %}
          select count(*) as total_deletes_count
          from ({{ delete_subquery }}) d
        {% endset %}

        {% set failures_view %}{{ qa_prefix }}v_hard_delete_{{ model_name }}_failures{% endset %}
        {% set create_failures_view_sql %}
          create or replace view {{ failures_view }} as
          select
            run_at,
            audit_run_id,
            execution_id,
            model_name,
            {% for key in key_cols %}
            {{ adapter.quote(key) }},
            {% endfor %}
            change_type,
            total_deletes_count
            {% for col in compare_list %}
            , before_delta_{{ col }}
            , after_delta_{{ col }}
            {% endfor %}
          from {{ failures_table }}
        {% endset %}

        {% do run_query(create_failures_sql) %}
        {% do run_query(create_failures_view_sql) %}
        {% do run_query(insert_failures_sql) %}
        {% set delete_count_res = run_query(delete_count_sql) %}

        {% if delete_count_res and delete_count_res.rows | length > 0 %}
          {% set total_anomalies_count  = delete_count_res.rows[0][0] or 0 %}
          {# Map hard deletes → missing_in_target for match_count computation in audit insert #}
          {% set missing_in_target_count = total_anomalies_count %}
        {% endif %}

        {# unique_keys_compared = before_delta_count so audit match_count = before - deletes #}
        {% set unique_keys_compared = before_delta_count %}
        {% set rows_compared = before_delta_count %}
        {% set test_status = 'MISMATCH' if total_anomalies_count > 0 else 'MATCH' %}
        {% set ns.ran = ns.ran + 1 %}

      {% else %}
        {% do exceptions.raise_compiler_error(
          'run_validation_tests: unsupported test_type=' ~ (test_type | string)
          ~ ' for model_name=' ~ (model_name | string)
          ~ '. Allowed: comprehensive_compare, delta_compare, hard_delete_check.'
        ) %}
      {% endif %}

      {% set ended_at = modules.datetime.datetime.utcnow() %}
      {% set execution_duration_ms = ((ended_at - started_at).total_seconds() * 1000) | round(0, 'floor') %}

      {% set audit_insert_sql %}
        insert into {{ qa_prefix }}validation_audit_log (
          audit_run_id, execution_id, audit_ts, model_name, test_type, load_type, status,
          source_model, source_row_count, target_model, target_row_count_before, target_row_count_after,
          rows_compared, unique_keys_compared, duplicate_groups_count, duplicate_rows_count,
          match_count, anomaly_count, delta_new_count, delta_updated_count, execution_duration_ms, error_message
        )
        values (
          '{{ audit_run_id }}',
          '{{ execution_id }}',
          current_timestamp(),
          '{{ model_name }}',
          '{{ test_type }}',
          '{{ load_type }}',
          '{{ test_status }}',
          {{ "'" ~ audit_source_model ~ "'" if audit_source_model else 'null' }},
          {{ audit_source_row_count if audit_source_row_count != 'null' else 'null' }},
          {{ "'" ~ audit_target_model ~ "'" if audit_target_model else 'null' }},
          {{ audit_target_row_count_before if audit_target_row_count_before != 'null' else 'null' }},
          {{ audit_target_row_count_after if audit_target_row_count_after != 'null' else 'null' }},
          {{ rows_compared if rows_compared != 'null' else 'null' }},
          {{ unique_keys_compared if unique_keys_compared != 'null' else 'null' }},
          {{ duplicate_groups_count if duplicate_groups_count != 'null' else 'null' }},
          {{ duplicate_rows_count if duplicate_rows_count != 'null' else 'null' }},
          {{ (unique_keys_compared - (missing_in_target_count + missing_in_source_count + value_mismatch_count)) if unique_keys_compared != 'null' else 'null' }},
          {{ total_anomalies_count }},
          {{ delta_new_count }},
          {{ delta_updated_count }},
          {{ execution_duration_ms }},
          {{ "'" ~ error_message | replace("'", "''") ~ "'" if error_message else 'null' }}
        )
      {% endset %}

      {% set anomaly_insert_sql %}
        insert into {{ qa_prefix }}validation_anomaly_log (
          total_anomalies_count, value_mismatch_count, missing_in_target_count, missing_in_source_count,
          source_duplicate_count, target_duplicate_count,
          duplicate_no_match_count, duplicate_partial_match_count, duplicate_full_match_count,
          delta_new_count, delta_updated_count,
          execution_id, audit_run_id, audit_ts, status, model_name, test_query_model, compare_mode, load_type
        )
        values (
          {{ total_anomalies_count }},
          {{ value_mismatch_count }},
          {{ missing_in_target_count }},
          {{ missing_in_source_count }},
          {{ source_duplicate_count }},
          {{ target_duplicate_count }},
          0,
          0,
          0,
          {{ delta_new_count }},
          {{ delta_updated_count }},
          '{{ execution_id }}',
          '{{ audit_run_id }}',
          current_timestamp(),
          '{{ test_status }}',
          '{{ model_name }}',
          {{ "'" ~ test_query_model ~ "'" if test_query_model else 'null' }},
          '{{ test_type }}',
          '{{ load_type }}'
        )
      {% endset %}

      {% do run_query(audit_insert_sql) %}
      {% do run_query(anomaly_insert_sql) %}
    {% endfor %}
    {% do log("run_validation_tests: completed " ~ (ns.ran | string) ~ " validation execution(s) in " ~ qa_schema ~ " (audit_run_id=" ~ audit_run_id ~ ")", info=True) %}
  {% else %}
    {% do log("run_validation_tests: no rows found in config seed " ~ config_seed, info=True) %}
  {% endif %}
{% endmacro %}

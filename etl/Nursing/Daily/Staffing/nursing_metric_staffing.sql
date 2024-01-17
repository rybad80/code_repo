{{ config(meta = {
    'critical': false
}) }}

/* nursing_metric_staffing */


{% set refs = [
    ref('stg_nursing_staff_w1_current_fte'),
    ref('stg_nursing_staff_w2_budget'),
    ref('stg_nursing_staff_w3_adjust_budget'),
    ref('stg_nursing_staff_w4_vacancy'),
    ref('stg_nursing_staff_w5_upcoming'),
    ref('stg_nursing_staff_w6_functional'),
    ref('stg_nursing_staff_w7_upcoming_func'),
    ref('stg_nursing_staff_w8_other'),
    ref('stg_nursing_staff_w9_employment')
] %}

with all_columns_unioned as (
{{ dbt_utils.union_relations(
    relations = refs,
    column_override = {
        "METRIC_ABBREVIATION": "varchar(40)",
        "WORKER_ID": "varchar(10)",
        "COST_CENTER_ID": "varchar(5)",
        "COST_CENTER_SITE_ID": "varchar(8)",
        "JOB_CODE": "varchar(15)",
        "JOB_GROUP_ID": "varchar(60)",
        "METRIC_GROUPER": "varchar(100)",
        "NUMERATOR": "float(15)",
        "DENOMINATOR": "float(15)",
        "ROW_METRIC_CALCULATION": "float(15)"},
    source_column_name = "source_stg_table") }}
),

all_columns as (
    select
        substr(all_columns_unioned.source_stg_table,
            instr(all_columns_unioned.source_stg_table, '.', 1, 2) + 1,
            length(all_columns_unioned.source_stg_table)
        ) as source_relation_table,
        all_columns_unioned.metric_abbreviation,
        all_columns_unioned.metric_dt_key,
        all_columns_unioned.worker_id,
        all_columns_unioned.cost_center_id,
        all_columns_unioned.cost_center_site_id,
        all_columns_unioned.job_code,
        all_columns_unioned.job_group_id,
        all_columns_unioned.metric_grouper,
        all_columns_unioned.numerator,
        all_columns_unioned.denominator,
        all_columns_unioned.row_metric_calculation
        from
        all_columns_unioned
)

select
    all_columns.metric_abbreviation,
    all_columns.metric_dt_key,
    all_columns.worker_id,
    all_columns.cost_center_id,
    all_columns.cost_center_site_id,
    all_columns.job_code,
    all_columns.job_group_id,
    all_columns.metric_grouper,
    all_columns.numerator,
    all_columns.denominator,
    all_columns.row_metric_calculation,
    coalesce(by_met_set.dbt_source_abbreviation,
        lookup_nursing_stage_source.dbt_source_abbreviation,
        'TBD') as dbt_source_relation
from all_columns
left join {{ ref('lookup_nursing_stage_source') }} as by_met_set
    on all_columns.source_relation_table = by_met_set.source_table_name
    and all_columns.metric_abbreviation = by_met_set.metric_abbreviation
left join {{ ref('lookup_nursing_stage_source') }} as lookup_nursing_stage_source
    on all_columns.source_relation_table = lookup_nursing_stage_source.source_table_name
        and lookup_nursing_stage_source.metric_abbreviation in ('ALL', 'rest')

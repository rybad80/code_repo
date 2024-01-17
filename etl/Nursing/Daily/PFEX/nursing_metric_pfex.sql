{{ config(meta = {
    'critical': false
}) }}

/* nursing_metric_pfex
Pull the waves of Patient & Family Experience metrics for nursing into one table
with consistent dimensions, where applicable
*/

{% set refs = [
    ref('stg_nursing_pfex_w1_metric')
] %}

with all_columns_unioned as (
{{ dbt_utils.union_relations(
    relations = refs,
    column_override = {
        "METRIC_ABBREVIATION": "varchar(40)",
        "DEPT_KEY": "bigint",
        "NURSING_PFEX_SURVEY_ID": "varchar(20)",
        "NURSING_PFEX_QUESTION_ID": "varchar(20)",
        "METRIC_GROUPER": "varchar(100)",
        "SCORE_VAL": "varchar(5)",
        "NUMERATOR": "float(15)",
        "DENOMINATOR": "float(15)",
        "ROW_METRIC_CALCULATION": "float(15)",
        "DISTINCT_COUNT_FIELD": "float(15)"},
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
        dim_dept.dept_id as department_id,
        all_columns_unioned.nursing_pfex_survey_id,
        all_columns_unioned.nursing_pfex_question_id,
        all_columns_unioned.metric_grouper,
        all_columns_unioned.score_val,
        all_columns_unioned.numerator,
        all_columns_unioned.denominator,
        all_columns_unioned.row_metric_calculation,
        all_columns_unioned.distinct_count_field
    from
        all_columns_unioned
        left join {{ source('cdw', 'department') }} as dim_dept
            on all_columns_unioned.dept_key = dim_dept.dept_key
)

select
    all_columns.metric_abbreviation,
    all_columns.metric_dt_key,
    all_columns.department_id,
    all_columns.nursing_pfex_survey_id,
    all_columns.nursing_pfex_question_id,
    all_columns.metric_grouper,
    all_columns.score_val,
    all_columns.numerator,
    all_columns.denominator,
    all_columns.row_metric_calculation,
    all_columns.distinct_count_field,
    coalesce(by_met_set.dbt_source_abbreviation,
        lookup_nursing_stage_source.dbt_source_abbreviation,
        'TBD') as dbt_source_relation,
    {{ dbt_utils.surrogate_key([
        'all_columns.metric_abbreviation',
        'all_columns.metric_dt_key',
        'all_columns.department_id',
        'all_columns.nursing_pfex_survey_id',
        'all_columns.nursing_pfex_question_id',
        'all_columns.metric_grouper',
        'all_columns.distinct_count_field'
    ]) }} as pfex_metric_key
from all_columns
left join {{ ref('lookup_nursing_stage_source') }} as by_met_set
    on all_columns.source_relation_table = by_met_set.source_table_name
    and all_columns.metric_abbreviation = by_met_set.metric_abbreviation
left join {{ ref('lookup_nursing_stage_source') }} as lookup_nursing_stage_source
    on all_columns.source_relation_table = lookup_nursing_stage_source.source_table_name
        and lookup_nursing_stage_source.metric_abbreviation in ('ALL', 'rest')

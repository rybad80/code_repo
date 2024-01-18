{{ config(
    post_hook=["truncate table {{ref('stg_nursing_engage_p2_question_aggregate')}}",
    "truncate table {{ref('stg_nursing_engage_w2_aggregate')}}",
    "truncate table {{ref('stg_nursing_engage_w3_rollup')}}",
    "truncate table {{ref('stg_nursing_engage_w4_category')}}",
    "truncate table {{ref('stg_nursing_engage_w5_yoy')}}",
    "truncate table {{ref('stg_nursing_engage_w6_ind_kpi')}}"
    ]
) }}
/* nursing_metric_engagement
final pull together of the stage union sets
for RN Excellence (nursing staff engagement) survey
data
followed (post-hook) by dropping the stage tables that contain survey result data
*/


{% set refs = [
    ref('stg_nursing_engage_w1_benchmark'),
    ref('stg_nursing_engage_w2_aggregate'),
    ref('stg_nursing_engage_w3_rollup'),
    ref('stg_nursing_engage_w4_category'),
    ref('stg_nursing_engage_w5_yoy'),
    ref('stg_nursing_engage_w6_ind_kpi')
] %}

with all_columns_unioned as (
{{ dbt_utils.union_relations(
    relations = refs,
    column_override = {
        "METRIC_ABBREVIATION": "varchar(40)",
        "METRIC_YEAR": "integer",
        "RN_DIMENSION_ID": "integer",
        "NURSE_COHORT_ID": "integer",
        "JOB_GROUP_ID": "varchar(50)",
        "ENGAGEMENT_QUESTION_ID": "integer",
        "UNIT_GROUP_ID": "integer",
        "DIMENSION_RESULT_CATEGORY_ID": "integer",
        "METRIC_GROUPER": "varchar(100)",
        "NUMERATOR": "float(15)",
        "DENOMINATOR": "float(15)",
        "ROW_METRIC_CALCULATION": "float(15)"},
        source_column_name = "source_stg_table") }}
),

all_columns as ( /* drop off the chop_analytics.ADMIN. */
    select
        substr(all_columns_unioned.source_stg_table,
            instr(all_columns_unioned.source_stg_table, '.', 1, 2) + 1,
            length(all_columns_unioned.source_stg_table)
        ) as source_relation_table,
        all_columns_unioned.metric_abbreviation,
        all_columns_unioned.metric_year,
        all_columns_unioned.rn_dimension_id,
        all_columns_unioned.nurse_cohort_id,
        all_columns_unioned.job_group_id,
        all_columns_unioned.engagement_question_id,
        all_columns_unioned.unit_group_id,
        all_columns_unioned.dimension_result_category_id, 
        all_columns_unioned.metric_grouper,
        all_columns_unioned.numerator,
        all_columns_unioned.denominator,
        all_columns_unioned.row_metric_calculation
        from
        all_columns_unioned
)

select
    all_columns.metric_abbreviation,
    all_columns.metric_year,
    all_columns.rn_dimension_id,
    all_columns.nurse_cohort_id,
    all_columns.job_group_id,
    all_columns.engagement_question_id,
    all_columns.unit_group_id,
    all_columns.dimension_result_category_id, 
    all_columns.metric_grouper,
    all_columns.numerator,
    all_columns.denominator,
    all_columns.row_metric_calculation,
    coalesce(by_met_set.dbt_source_abbreviation,
        lookup_nursing_stage_source.dbt_source_abbreviation,
        'TBD') as dbt_source_relation,
        {{ dbt_utils.surrogate_key([
                'all_columns.metric_abbreviation',
                'all_columns.metric_year',
                'all_columns.rn_dimension_id',
                'all_columns.nurse_cohort_id',
                'all_columns.job_group_id',
                'all_columns.engagement_question_id',
                'all_columns.unit_group_id',
                'all_columns.dimension_result_category_id', 
                'all_columns.metric_grouper'
            ]) }} as engagement_metric_key
from all_columns
left join {{ ref('lookup_nursing_stage_source') }} as by_met_set
    on all_columns.source_relation_table = by_met_set.source_table_name
    and all_columns.metric_abbreviation = by_met_set.metric_abbreviation
left join {{ ref('lookup_nursing_stage_source') }} as lookup_nursing_stage_source
    on all_columns.source_relation_table = lookup_nursing_stage_source.source_table_name
        and lookup_nursing_stage_source.metric_abbreviation in ('ALL', 'rest')

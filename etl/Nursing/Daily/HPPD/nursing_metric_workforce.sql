/* nursing_metric_workforce
final pull together of the stage union sets
set_a - HPPD stages
set_b - budgets and targets for workforce
set_c - time hours collections and sums
set_d - aggregates and gaps for time
*/
{% set refs = [
    ref('stg_nursing_metric_workforce_set_a'),
    ref('stg_nursing_metric_workforce_set_b'),
    ref('stg_nursing_metric_workforce_set_c'),
    ref('stg_nursing_metric_workforce_set_d')
] %}

with all_columns as (
{{ dbt_utils.union_relations(
    relations = refs,
    column_override = {
        "METRIC_ABBREVIATION": "varchar(40)",
        "WORKER_ID": "varchar(10)",
        "COST_CENTER_ID": "varchar(5)",
        "COST_CENTER_SITE_ID": "varchar(8)",
        "JOB_CODE": "varchar(15)",
        "JOB_GROUP_ID": "varchar(50)",
        "METRIC_GROUPER": "varchar(100)",
        "NUMERATOR": "float(15)",
        "DENOMINATOR": "float(15)",
        "ROW_METRIC_CALCULATION": "float(15)",
        "DBT_SOURCE_RELATION": "varchar(25)"}
        ) }}
)

select
    metric_abbreviation,
    metric_dt_key,
    worker_id,
    cost_center_id,
    cost_center_site_id,
    job_code,
    job_group_id,
    metric_grouper,
    numerator,
    denominator,
    row_metric_calculation,
    dbt_source_relation
from all_columns

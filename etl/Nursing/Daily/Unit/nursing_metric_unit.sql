{% set refs = [
    ref('stg_nursing_unit_w1_patient_days'),
    ref('stg_nursing_unit_w2_contact')
] %}

with all_columns as (
{{ dbt_utils.union_relations(
    relations = refs,
    column_override = {
        "METRIC_ABBREVIATION": "varchar(40)",
        "WORKER_ID": "varchar(10)",
        "COST_CENTER_ID": "varchar(5)",
        "COST_CENTER_SITE_ID": "varchar(8)",
        "DEPARTMENT_ID": "bigint",
        "JOB_CODE": "varchar(15)",
        "JOB_GROUP_ID": "varchar(50)",
        "METRIC_GROUPER": "varchar(100)",
        "NUMERATOR": "float(15)",
        "DENOMINATOR": "float(15)",
        "ROW_METRIC_CALCULATION": "float(15)"},
    source_column_name = "dbt_source_relation") }}
)

select
    metric_abbreviation,
    metric_dt_key,
    worker_id,
    cost_center_id,
    cost_center_site_id,
    department_id,
    job_code,
    job_group_id,
    metric_grouper,
    numerator,
    denominator,
    row_metric_calculation,
    case 
    when lower(dbt_source_relation)
        like '%unit_w1%' then 'unit_w1'
    when lower(dbt_source_relation)
        like '%unit_w2%' then 'unit_w2'
    end as dbt_source_relation
from
    all_columns

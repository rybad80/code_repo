{% set refs = [
    ref('stg_nursing_profile_w1_ind'),
    ref('stg_nursing_profile_w2_name'),
    ref('stg_nursing_profile_w3_educ_time')
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
        "JOB_GROUP_ID": "varchar(60)",
        "PROFILE_NAME": "varchar(200)",
        "METRIC_GROUPER": "varchar(100)",
        "NUMERATOR": "float(15)",
        "DENOMINATOR": "float(15)",
        "ROW_METRIC_CALCULATION": "float(15)"},
    source_column_name = "dbt_source_relation") }}
),

worker_cc_job_lvl4 as (
    select
         worker.worker_id,
         worker.cost_center_id,
         get_rollup_grp.nursing_job_rollup       
    from   
        {{ ref('worker') }} as worker
        left join {{ ref('stg_nursing_job_code_group_statistic') }} as j_grp
            on worker.job_code = j_grp.job_code
        left join {{ ref('job_group_levels_nursing') }} as get_rollup_grp
            on j_grp.use_job_group_id = get_rollup_grp.job_group_id
)

select
    metric_abbreviation,
    metric_dt_key,
    all_columns.worker_id,
    case
        when worker_cc_job_lvl4.worker_id is not null
        then worker_cc_job_lvl4.cost_center_id
    end as cost_center_id,
    null as cost_center_site_id,
    null as job_code,
    case
        when worker_cc_job_lvl4.worker_id is not null
        then worker_cc_job_lvl4.nursing_job_rollup
     end as job_group_id,
    profile_name,
    metric_grouper,
    numerator,
    null as denominator,
    numerator as row_metric_calculation,
    case
    when lower(dbt_source_relation)
        like '%profile_w1%' 
        and metric_abbreviation in (
            'CertAdvPracCnt',
            'CertMagnetCnt',
            'RNlicenseCnt')
        then 'profile_w1' || '-'
        || metric_abbreviation
    when lower(dbt_source_relation)
        like '%profile_w1%' then 'profile_w1'
    when lower(dbt_source_relation)
        like '%profile_w2%' 
        and metric_abbreviation in (
            'rnCertName',
            'rnEducDegree',
            'rnEducDegreeOther')
        then 'profile_w2' || '-'
        || metric_abbreviation
    when lower(dbt_source_relation)
        like '%profile_w2%' then 'profile_w2'
    when lower(dbt_source_relation)
        like '%profile_w3%' then 'profile_w3'
end as dbt_source_relation
from all_columns
    left join worker_cc_job_lvl4
        on all_columns.worker_id = worker_cc_job_lvl4.worker_id

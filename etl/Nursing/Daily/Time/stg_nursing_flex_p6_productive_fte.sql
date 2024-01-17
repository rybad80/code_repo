{{ config(meta = {
    'critical': true
}) }}

/* stg_nursing_flex_p6_productive_fte
rollup the FTEs for cost center productive time totals
and at the job_rollup level to support productive flex
variance calculations
special adjustment: ambulatory RN subsets of the StaffNurse
need to be pulled out separately (jr = job rollup)
*/
with job_rollup_fte as (
    select
        'FlexFTEjr' as metric_abbreviation,
        flex_fte.metric_dt_key,
        flex_fte.cost_center_id,
        null as metric_grouper,
        get_rollup.nursing_job_rollup as job_group_id,
        sum(flex_fte.numerator) as actual_prdctv_fte
    from
        {{ ref('stg_nursing_time_w3_fte') }} as flex_fte
        left join {{ ref('job_group_levels_nursing') }} as get_rollup
            on flex_fte.job_group_id = get_rollup.job_group_id
    where
        flex_fte.metric_abbreviation = 'FlexFTE'
    group by
        flex_fte.metric_dt_key,
        flex_fte.cost_center_id,
        get_rollup.nursing_job_rollup
),

prdctv_amb_rn_fte as (
    select
        metric_dt_key,
        cost_center_id,
        'AmbulatoryRN' as job_group_id,
        numerator as actual_prdctv_fte_amb_rn
    from
        {{ ref('stg_nursing_time_w3_fte') }}
    where
        metric_abbreviation = 'PrdctvFTEambRN'
)

/* cost center totals */
select
    'FlexFTEcc' as metric_abbreviation,
    metric_dt_key,
    cost_center_id,
    'CC Role Total' as metric_grouper,
    null as job_group_id,
    sum(numerator) as actual_prdctv_fte
from
    {{ ref('stg_nursing_time_w3_fte') }}
where
    metric_abbreviation = 'FlexFTE'
group by
    metric_dt_key,
    cost_center_id

union all
/* all except the Staff Nurse at the cc/job rollup level */
select
    metric_abbreviation,
    metric_dt_key,
    cost_center_id,
    metric_grouper,
    job_group_id,
    actual_prdctv_fte
from
    job_rollup_fte
where
    coalesce(job_group_id, '') != 'StaffNurse'

union all
/* adjust the cc's Staff Nurse FTE total to subtract out the Ambulatory RN part */
select
    job_rollup_fte.metric_abbreviation,
    job_rollup_fte.metric_dt_key,
    job_rollup_fte.cost_center_id,
    null as metric_grouper,
    job_rollup_fte.job_group_id,
    job_rollup_fte.actual_prdctv_fte
        - coalesce(prdctv_amb_rn_fte.actual_prdctv_fte_amb_rn, 0) as actual_prdctv_fte
from
    job_rollup_fte
	left join prdctv_amb_rn_fte
        on job_rollup_fte.metric_dt_key = prdctv_amb_rn_fte.metric_dt_key
        and job_rollup_fte.cost_center_id = prdctv_amb_rn_fte.cost_center_id
where
    job_rollup_fte.job_group_id = 'StaffNurse'

union all
/* Ambulatory RN FTE total for the cc is called out separately for Flex variance determination */
select
    'FlexFTEjr' as metric_abbreviation,
    metric_dt_key,
    cost_center_id,
    null as metric_grouper,
    job_group_id, /* AmbulatoryRN */
    actual_prdctv_fte_amb_rn as actual_prdctv_fte
from
    prdctv_amb_rn_fte

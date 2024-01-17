{{ config(meta = {
    'critical': true
}) }}

/* stg_nursing_job_code_group_statistic
for grouping purposes when processing job code granularity data, such
as for Flex metric target/productive hours' sets, create a
use_job_group_id and include basic other attributes
and taag variable jobs which flex with hospital unit volumes
*/
with variable_job as (
    select
        lookup_job_group_set.usage,
        lookup_job_group_set.set_desc,
        lookup_job_group_set.job_group_id
    from
        {{ ref('lookup_job_group_set') }} as lookup_job_group_set
        inner join {{ ref('nursing_pay_period') }} as nursing_pay_period
            on lookup_job_group_set.effective_thru_fiscal_year >= nursing_pay_period.fiscal_year
            and nursing_pay_period.latest_pay_period_ind = 1
    where
        lookup_job_group_set.usage = 'flex component'
        and lookup_job_group_set.set_desc = 'variable'
),

get_job_group as (
    select
        stg_nursing_job_code_group.job_code,
        stg_nursing_job_code_group.job_title_display,
        coalesce(stg_nursing_job_code_group.nursing_job_grouper,
            provider_job_group_id) as fall_thru_value,
        case
            when stg_nursing_job_code_group.nursing_job_grouper like 'XXXother job category%'
            then replace(stg_nursing_job_code_group.nursing_job_grouper,
                'XXXother job category', 'XX jbCatg:')
            when stg_nursing_job_code_group.nursing_job_grouper is null
            then case
                when job_profile_report.inactive = 1
                then 'WorkdayInactiveNoJobGrp'
                when coalesce(job_cnt.job_active_worker_count, 0) < 1
                then 'NoActvPeopleNoJobGrp'
                else 'WHY?'
                end
            else stg_nursing_job_code_group.nursing_job_grouper
        end as use_job_group_id,
        stg_nursing_job_code_group.rn_alt_job_group_id,
        stg_nursing_job_code_group.fixed_rn_override_ind,
        job_cnt.job_active_worker_count,
        stg_nursing_job_code_group.rn_job_ind,
        stg_nursing_job_code_group.nursing_category,
        stg_nursing_job_code_group.have_next_best_ind,
        stg_nursing_job_code_group.other_job_group_string,
        stg_nursing_job_code_group.additional_job_group_info
    from
        {{ ref('stg_nursing_job_code_group') }} as stg_nursing_job_code_group
        inner join {{ source('workday_ods', 'job_profile_report') }} as job_profile_report
            on stg_nursing_job_code_group.job_code = job_profile_report.job_code
        left join {{ ref('stg_job_active_worker_current_statistic') }} as job_cnt
            on stg_nursing_job_code_group.job_code = job_cnt.job_code
)

select
    get_job_group.job_code,
    get_job_group.job_title_display,
    get_job_group.fall_thru_value,
    get_job_group.use_job_group_id,
    get_job_group.job_active_worker_count,
    get_job_group.rn_job_ind,
    get_job_group.nursing_category,
    get_job_group.have_next_best_ind,
    get_job_group.other_job_group_string,
    get_job_group.additional_job_group_info,
    get_job_group.rn_alt_job_group_id,
    get_job_group.fixed_rn_override_ind,
    case
        when get_job_group.fixed_rn_override_ind = 1
        then 0
        when variable_job.job_group_id is null
        then 0
        else 1 end as variable_ind
from
    get_job_group
	left join variable_job
        on get_job_group.use_job_group_id = variable_job.job_group_id

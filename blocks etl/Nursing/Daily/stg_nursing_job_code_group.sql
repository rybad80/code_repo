{{ config(meta = {
    'critical': true
}) }}

/* stg_nursing_job_code_group
sets metric_grouper to be the provider_job_group_id or nursing_next_best_job_group_id,
or the xxx value and flags the job codes that are Ambulatory RNs -- generally for the
purpose of NOT counting them for HPPD or for variable adjustment for the productive flex
target
*/

select
    job_code_profile.job_title_display,
    job_code_profile.job_code,
    job_code_nursing_key_groups.nursing_default_job_group_name,
    coalesce(
        job_code_nursing_key_groups.provider_or_other_job_group_id,
        job_code_profile.provider_job_group_id )
    as nursing_job_grouper,
    job_code_profile.provider_job_group_id,
    job_code_profile.rn_alt_job_group_id,
    case job_code_profile.rn_alt_job_group_id
        when 'AmbulatoryRN'
	then 1 else 0
    end as fixed_rn_override_ind,
    job_code_nursing_key_groups.nursing_next_best_job_group_id,
    job_code_nursing_key_groups.provider_or_other_job_group_id,
    job_code_nursing_key_groups.rn_alt_or_other_job_group_id,
    job_code_nursing_key_groups.job_group_name_granularity_path,
    job_code_profile.rn_job_ind,
    job_code_profile.nursing_category,
    case
	when job_code_nursing_key_groups.nursing_next_best_job_group_id is null
	then 0 else 1 end as have_next_best_ind,
    case
        when have_next_best_ind = 1
	then ' '
        else '' end as separator,
    case
        when job_code_profile.rn_alt_job_group_id = 'AcuteRN'
        and have_next_best_ind = 1
	then job_code_profile.rn_alt_job_group_id || separator
        else '' end
    || coalesce(
        job_code_nursing_key_groups.nursing_next_best_job_group_id,
        job_code_profile.rn_alt_job_group_id,
        '')
    as other_job_group_string,
    case
    when nursing_job_grouper = other_job_group_string
    then ''
    else other_job_group_string
    end as additional_job_group_info
from {{ ref('job_code_profile') }} as job_code_profile
	left join {{ ref('job_code_nursing_key_groups') }} as job_code_nursing_key_groups
        on job_code_profile.job_code = job_code_nursing_key_groups.job_code

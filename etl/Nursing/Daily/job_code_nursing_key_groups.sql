{{ config(meta = {
    'critical': true
}) }}

with provider_job_groups as (
    select
        job_group_levels.job_group_name as provider_group_name,
        job_group_levels.root_job_hierarchy,
        job_group_levels.end_level,
        job_group_levels.leaf_node_ind,
        job_code_profile.job_code,
        job_code_profile.provider_job_group_id,
        job_code_profile.rn_alt_job_group_id
    from
        {{ ref('job_code_profile') }} as job_code_profile
    inner join {{ ref('job_group_levels') }} as job_group_levels
        on job_code_profile.provider_job_group_id  = job_group_levels.job_group_id
),

additional_top_nursing_group as (
    select
        job_group_levels.job_group_name as top_additional_nursing_job_group_name,
        job_group_levels.root_job_hierarchy,
        additional_job_group.job_code,
        additional_job_group.job_group_id as nursing_next_best_job_group_id
    from
        {{ ref('job_code_role_clarity_job_group') }} as additional_job_group
    left join {{ ref('job_group_levels') }} as job_group_levels
        on additional_job_group.job_group_id  = job_group_levels.job_group_id
    where
        top_nursing_group_ind = 1
),

resolve_nursing_metric_grouper as (
    select
        coalesce(
            provider_job_groups.job_code,
            additional_top_nursing_group.job_code)
        as job_code,
        provider_job_groups.provider_job_group_id,
        provider_job_groups.rn_alt_job_group_id,
        additional_top_nursing_group.nursing_next_best_job_group_id,
        coalesce(
            case
                when provider_job_groups.provider_job_group_id not like 'XXX%'
                then provider_job_groups.provider_job_group_id end,
            additional_top_nursing_group.nursing_next_best_job_group_id)
        as  provider_or_other_job_group_id,
        coalesce(
            provider_job_groups.rn_alt_job_group_id,
            additional_top_nursing_group.nursing_next_best_job_group_id)
        as rn_alt_or_other_job_group_id,
        coalesce(
            provider_or_other_job_group_id,
            rn_alt_or_other_job_group_id,
            'unkNursingJobGroup')
        as default_nursing_job_group_id
	from
        provider_job_groups as provider_job_groups
	full outer join additional_top_nursing_group as additional_top_nursing_group
        on provider_job_groups.job_code = additional_top_nursing_group.job_code
)

select
    resolve_nursing_metric_grouper.job_code,
    resolve_nursing_metric_grouper.default_nursing_job_group_id as nursing_default_job_group_id,
    job_group_levels.job_group_name as nursing_default_job_group_name,
    job_group_levels.root_job_hierarchy as nursing_default_root_job_hierarchy,
    resolve_nursing_metric_grouper.provider_job_group_id,
    resolve_nursing_metric_grouper.rn_alt_job_group_id,
    resolve_nursing_metric_grouper.nursing_next_best_job_group_id,
    resolve_nursing_metric_grouper.provider_or_other_job_group_id,
    resolve_nursing_metric_grouper.rn_alt_or_other_job_group_id,
    job_group_levels.job_group_name_granularity_path,
    job_code_profile.job_title_display
from
    resolve_nursing_metric_grouper
left join {{ ref('job_group_levels') }} as job_group_levels
    on resolve_nursing_metric_grouper.default_nursing_job_group_id  = job_group_levels.job_group_id
inner join {{ ref('job_code_profile') }} as job_code_profile
    on resolve_nursing_metric_grouper.job_code  = job_code_profile.job_code

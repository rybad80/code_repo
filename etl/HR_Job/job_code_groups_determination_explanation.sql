{{ config(meta = {
    'critical': true
}) }}

/* job_code_groups_determination_explanation for a job_code the provider, nursing alternate
and nursing default job groups (when applicable and if needed)) and how determined */
with worker_counts as (
    select
        job_code,
        sum(active_ind * fte_percentage) / 100 as active_fte_total,
        sum(active_ind) as active_worker_cnt,
        sum(active_ind * rn_job_ind) as active_rn_worker_cnt,
        case
            when active_worker_cnt = 0
            then max(termination_date) end
        as max_term_dt,
        case
            when active_worker_cnt = 0
            then 888 else 5 end
        as order_has_workers_first
    from {{ ref('worker') }}
    group by job_code
)

select
    alvls.job_group_name as alt_rn_job_group_name,
    opt.level_1_id,
    plvls.job_group_name as provider_job_group_name,
    plvls.job_group_granularity_path as prvdr_job_group_granularity_path,
    coalesce(opt.possible_how_determined,
        case
            when prfl.provider_job_group_id like 'XXXother job category%'
            then 'XXX default from job_category'
            else case
                when coalesce(cnts.active_worker_cnt, 0) = 0
                then 'n/a' else 'TBD how' end
            end)
    as how_determined,
    opt.possible_values_used as with_values,
    cnts.active_worker_cnt as active_worker_count,
    cnts.active_fte_total,
    cnts.active_rn_worker_cnt as active_rn_worker_count,
    max_term_dt,
    prfl.provider_job_group_id,
    prfl.rn_alt_job_group_id,
    prfl.job_code,
    case
        when prfl.provider_job_group_id is null
        then coalesce(n_job_grp.nursing_default_job_group_id,
            n_job_grp.nursing_next_best_job_group_id)
        when  prfl.provider_job_group_id = n_job_grp.nursing_default_job_group_id
        then n_job_grp.nursing_next_best_job_group_id
        when prfl.provider_job_group_id
            != coalesce(n_job_grp.nursing_default_job_group_id, n_job_grp.nursing_next_best_job_group_id)
        then coalesce(n_job_grp.nursing_default_job_group_id, n_job_grp.nursing_next_best_job_group_id)
    end as other_nursing_job_group,
    case
        when prfl.provider_job_group_id is null
        then case when other_nursing_job_group is null then 0 else 1 end
        when  prfl.provider_job_group_id = n_job_grp.nursing_default_job_group_id
        then 0
        when prfl.provider_job_group_id
            != coalesce(n_job_grp.nursing_default_job_group_id, n_job_grp.nursing_next_best_job_group_id)
        then 1 else 0
    end as used_nursing_job_group_ind,
    prfl.job_title,
    prfl.job_title_display,
    prfl.job_classification_name,
    prfl.job_category_name,
    prfl.nursing_category_abbreviation,
    prfl.magnet_reporting_name,
    prfl.magnet_reporting_ind,
    prfl.rn_job_ind,
    prfl.nccs_direct_care_staff_ind,
    prfl.bedside_rn_ind,
    prfl.job_family_group,
    prfl.job_family,
    prfl.management_level,
    alvls.job_group_granularity_path as alt_rn_job_group_granularity_path,
    prfl.job_classification_sort_num,
    prfl.job_category_sort_num,
    prfl.nursing_category_sort_num,
    prfl.nursing_category,
    coalesce(cnts.order_has_workers_first, 999) as order_has_workers_first,
    case when prfl.provider_job_group_id = n_job_grp.nursing_default_job_group_id
        then n_next_best_opt.possible_how_determined
        else n_opt.possible_how_determined end
    as other_nursing_how_determined,
    case when  prfl.provider_job_group_id = n_job_grp.nursing_default_job_group_id
        then n_next_best_opt.possible_values_used
        else n_opt.possible_values_used end
    as other_nursing_with_values,
    coalesce(plvls.job_group_granularity_path,
        case
            when prfl.provider_job_group_id is null
            then 'zz9999' else 'zz8888 ' || prfl.provider_job_group_id end
        )
    as prvdr_job_group_granularity_sort

from {{ ref('job_code_profile') }} as prfl
    left join {{ ref('job_code_group_determination_option') }} as opt
        on prfl.job_code = opt.job_code
        and prfl.provider_job_group_id = opt.job_group_id
        and opt.take_this_job_group_id_ind = 1
    left join {{ ref('job_code_nursing_key_groups') }} as n_job_grp
        on prfl.job_code = n_job_grp.job_code

    /* nursing default */
    left join {{ ref('job_code_group_determination_option') }} as n_opt
        on n_job_grp.job_code  = n_opt.job_code
        and n_job_grp.nursing_default_job_group_id = n_opt.job_group_id
    /* nursing next best */
    left join {{ ref('job_code_group_determination_option') }} as n_next_best_opt
        on n_job_grp.job_code = n_next_best_opt.job_code
        and n_job_grp.nursing_next_best_job_group_id = n_next_best_opt.job_group_id

    left join worker_counts as cnts on prfl.job_code = cnts.job_code
    left join {{ ref('job_group_levels') }} as plvls
        on prfl.provider_job_group_id = plvls.job_group_id
    left join {{ ref('job_group_levels') }} as alvls
        on prfl.rn_alt_job_group_id = alvls.job_group_id

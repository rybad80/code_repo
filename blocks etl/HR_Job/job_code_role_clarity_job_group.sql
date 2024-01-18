{{ config(meta = {
    'critical': true
}) }}

with just_tbd_process_3s as (
    select
        stg_job_to_group_pass.job_code,
        stg_job_to_group_pass.provider_alignment_use_ind,
        stg_job_to_group_pass.order_num,
        stg_job_to_group_pass.process_rank,
        stg_job_to_group_pass.job_group_id,
        stg_job_to_group_pass.plurality_ind,
        stg_job_to_group_pass.take_this_job_group_id_ind
    from
        {{ ref('stg_job_to_group_pass') }} as stg_job_to_group_pass
    where
	stg_job_to_group_pass.process_rank = 3    /* TBD: provider_or_not */
),

job_code_with_provider_job_group as (
    select
	job_code_profile.job_code as job_code_that_has_group,
	job_code_profile.provider_job_group_id
    from
	{{ ref('job_code_profile') }} as job_code_profile
    where
	job_code_profile.provider_job_group_id is not null
	and job_code_profile.provider_job_group_id not like 'XXX%'
),

process_rank_3_only_if_no_provider_id_yet as (
    select
        just_3s.job_code
    from
	just_tbd_process_3s as just_3s
	left join job_code_with_provider_job_group as job_code_with_provider_job_group
		on just_3s.job_code = job_code_with_provider_job_group.job_code_that_has_group
    where
	job_code_with_provider_job_group.job_code_that_has_group is null
),

singular_result_rows_rank_3 as (
    select
        just_3s.job_code,
        just_3s.provider_alignment_use_ind,
        just_3s.order_num,
        just_3s.process_rank,
        just_3s.take_this_job_group_id_ind,
        just_3s.job_group_id
    from
        process_rank_3_only_if_no_provider_id_yet
        inner join  just_tbd_process_3s as just_3s
		on just_3s.job_code = process_rank_3_only_if_no_provider_id_yet.job_code
),

singular_result_rows_non_rank_3 as (
    select
        singular_process_4_and_up.job_code,
        singular_process_4_and_up.provider_alignment_use_ind,
        singular_process_4_and_up.order_num,
        singular_process_4_and_up.process_rank,
        singular_process_4_and_up.take_this_job_group_id_ind,
        singular_process_4_and_up.job_group_id
    from
        {{ ref('stg_job_to_group_pass') }} as singular_process_4_and_up
	where
        /* any of the roots not processed by job_code_profile
        and not the provider_or_not scenario but still needs fall through, first match processing */
        singular_process_4_and_up.process_rank > 3
        and singular_process_4_and_up.plurality_ind = 0
),

singular_result_rows_to_process as (
    select
        singular_result_rows_rank_3.job_code,
        singular_result_rows_rank_3.provider_alignment_use_ind,
        singular_result_rows_rank_3.order_num,
        singular_result_rows_rank_3.take_this_job_group_id_ind,
        singular_result_rows_rank_3.process_rank,
        singular_result_rows_rank_3.job_group_id
    from
        singular_result_rows_rank_3

    union all

    select
        singular_result_rows_non_rank_3.job_code,
        singular_result_rows_non_rank_3.provider_alignment_use_ind,
        singular_result_rows_non_rank_3.order_num,
        singular_result_rows_non_rank_3.take_this_job_group_id_ind,
        singular_result_rows_non_rank_3.process_rank,
        singular_result_rows_non_rank_3.job_group_id
    from
        singular_result_rows_non_rank_3
),

singular_result_rows_to_keep as (
    select
        find_only_lowest_order_num.job_code,
        find_only_lowest_order_num.job_group_id,
        find_only_lowest_order_num.process_rank
    from
        singular_result_rows_to_process as find_only_lowest_order_num
    where
        find_only_lowest_order_num.take_this_job_group_id_ind = 1
),

all_plurality_process_rows as (
    select
        all_job_to_group_rows.job_code,
        all_job_to_group_rows.provider_alignment_use_ind,
        all_job_to_group_rows.order_num,
        all_job_to_group_rows.process_rank,
        all_job_to_group_rows.job_group_id
    from
        {{ ref('stg_job_to_group_pass') }} as all_job_to_group_rows
    where
        all_job_to_group_rows.plurality_ind = 1
),

more_rows_to_proceed as (
    select
        singular_result_rows_to_keep.job_code,
        singular_result_rows_to_keep.job_group_id as job_group_id,
        singular_result_rows_to_keep.process_rank
    from
        singular_result_rows_to_keep

    union all

    select
        all_plurality_process_rows.job_code,
        all_plurality_process_rows.job_group_id,
        all_plurality_process_rows.process_rank
    from
        all_plurality_process_rows
),

rows_plus_sort_and_top_ind as (
    select
        more_rows_to_proceed.job_code,
        more_rows_to_proceed.process_rank,
        more_rows_to_proceed.job_group_id,
        row_number() over(
                partition by
                    more_rows_to_proceed.job_code
                order by
                    coalesce(more_rows_to_proceed.process_rank, 999),
                    more_rows_to_proceed.job_group_id
            ) as nursing_sort_num,
        case
        when
            nursing_sort_num = 1 then 1
        else 0
        end as top_nursing_group_ind
    from
        more_rows_to_proceed
)

select
    rows_plus_sort_and_top_ind.job_code,
    job_group_levels.job_group_name,
    rows_plus_sort_and_top_ind.job_group_id,
    job_code_profile.job_title_display,
    rows_plus_sort_and_top_ind.nursing_sort_num,
    rows_plus_sort_and_top_ind.top_nursing_group_ind,
    job_group_levels.job_group_granularity_path
from
    rows_plus_sort_and_top_ind as rows_plus_sort_and_top_ind
    left join {{ ref('job_group_levels') }} as job_group_levels
        on rows_plus_sort_and_top_ind.job_group_id = job_group_levels.job_group_id
    left join {{ ref('job_code_profile') }} as job_code_profile
        on rows_plus_sort_and_top_ind.job_code = job_code_profile.job_code

/*
	examples for ref:
23244 - Lead Clinical Support Assoc  only nursing cares about specifics so regular
        provider is Ancillary but not sure where to put Ancillary or are all CSAs a subset of AncillaryPatCare
51259 - Research Psychologist
43505 - Inpatient Clerk, but also in BH
*/

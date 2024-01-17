{{ config(meta = {
    'critical': true
}) }}

/*  stg_job_profile_provider_alignment
set the provider_job_group_id and rn_alternate_job_group_id for each CHOP job_code:
a. data driven for mappings from job code, family, family_group, category, and
rn_job_ind/magnet_reporting/nursing_category combinations
b. take the minumum order_num row for each job_code for the two job_group values:
-> provider_job_group_id for the enterprise job group bucket
and for now sometimes will be filled with a TBD granularity tree "leaf" value
-> rn_alternate_job_group_id, only if it applies (that the job requires an RN license)
*/

with find_only_lowest_order_num_for_rank_one as (
    select
        job_code,
        job_group_id as provider_job_group_id,
        process_rank,
		case
        when
            order_num = 10 then 1
        else 0
        end as job_code_alignment_process_first,
		case
        when
            row_number() over(
                partition by
                    job_code
            /*  for now need to include process_num 3 as well,
            therfore excluding the process_num in the partition */
                order by
                    provider_alignment_use_ind desc,
                    coalesce(order_num, 999),
                    job_group_id
            ) = 1 then 1
        else 0
        end
        /* use take_this_job_group_id_ind from stg_job_to_group_pass
	once the process 3 (TBD) is all addressed at CHOP */
        as take_provider_job_group_id_ind
    from
        {{ref('stg_job_to_group_pass')}}
    where
        process_rank = 1
        /* for now we need to also do the fall through to the "provider or not (TBD)"
           so the provider work group can review not finalized job groups in FY 23  */
        or process_rank = 3
),

only_lowest_order_num_for_rank_one as (
    select
        job_code,
        provider_job_group_id,
        process_rank,
        job_code_alignment_process_first
    from find_only_lowest_order_num_for_rank_one
    where
        take_provider_job_group_id_ind = 1
),

find_only_lowest_order_num_for_rank_two as (
    select
        job_code,
        job_group_id as rn_alt_job_group_id,
        process_rank,
		case
        when
            order_num = 10 then 1
        else 0
        end as job_code_alignment_process_first,
		take_this_job_group_id_ind  as take_rn_alt_job_group_id_ind
    from
        {{ref('stg_job_to_group_pass')}}
    where
        process_rank = 2
),

only_lowest_order_num_for_rank_two as (
    select
        job_code,
        rn_alt_job_group_id,
        process_rank,
        job_code_alignment_process_first
    from find_only_lowest_order_num_for_rank_two
    where
        take_rn_alt_job_group_id_ind = 1
)

select
    coalesce(
        only_lowest_order_num_for_rank_one.provider_job_group_id,
        'XXXother job category ' || workday_plus_nursing.job_category_name
        ) as provider_job_group_id,
	only_lowest_order_num_for_rank_two.rn_alt_job_group_id,
    workday_plus_nursing.job_code,
    workday_plus_nursing.job_title_display,
    workday_plus_nursing.job_classification_name,
    workday_plus_nursing.job_category_name,
    workday_plus_nursing.nursing_category,
    workday_plus_nursing.job_family_group,
    workday_plus_nursing.job_family,
    workday_plus_nursing.management_level,
    workday_plus_nursing.job_classification_sort_num,
    workday_plus_nursing.job_category_sort_num

from {{ref('stg_job_profile_workday_plus_nursing')}}  as workday_plus_nursing
left join only_lowest_order_num_for_rank_one
    on workday_plus_nursing.job_code = only_lowest_order_num_for_rank_one.job_code
left join only_lowest_order_num_for_rank_two
    on workday_plus_nursing.job_code = only_lowest_order_num_for_rank_two.job_code

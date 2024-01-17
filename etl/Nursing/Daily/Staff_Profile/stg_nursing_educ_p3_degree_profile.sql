/* stg_nursing_educ_p3_degree_profile
using the nursing majors records and degree ordering
set the highest and entry nursing degrees the
CHOP employee has completed
and also determine if the person has a nursing
bachelor's or advanced degree
and finally show if any degree and what advanced nursing
degree the person may be working toward
*/
with
rn_worker as (
select
    worker_id,
    legal_reporting_name as worker_name,
    active_ind as worker_active_ind,
    nursing_category  as worker_nursing_category
from
    {{ ref('worker') }}
where
    magnet_reporting_ind = 1
    and rn_job_ind = 1
),

worker_inds as (
select
    rn_worker.worker_id,
    max(stg_nursing_educ_p1_worker_degree.nursing_degree_ind)
        as worker_nursing_degree_ind,
    max(stg_nursing_educ_p1_worker_degree.nursing_bachelors_ind)
        as worker_nursing_bachelors_ind,
    max(stg_nursing_educ_p1_worker_degree.nursing_advanced_degree_ind)
        as worker_nursing_advanced_degree_ind
from
    rn_worker
    inner join {{ ref('stg_nursing_educ_p1_worker_degree') }}
        as stg_nursing_educ_p1_worker_degree
        on rn_worker.worker_id = stg_nursing_educ_p1_worker_degree.worker_id
where
    stg_nursing_educ_p1_worker_degree.graduated = 'Yes'
group by
    rn_worker.worker_id
),

advancing_degree_gather_data as (
select
    worker_id,
    degree as highest_attending_advanced_nursing_degree,
    degree_sort_num as advancing_degree_sort_num,
    row_number() over(
    partition by worker_id
    order by coalesce(degree_sort_num, 999),
        institution_type,
        major,
        school_name) as highest_row_seq_num,
    case
        when highest_row_seq_num = 1
        then 1
        else 0
        end as highest_attending_degree_ind,
    education_end_year as attending_advanced_nursing_goal_year
from
    {{ ref('stg_nursing_educ_p1_worker_degree') }}
where
    include_major_for_nursing_degree_level = 1
    and degree_in_progress_ind = 'Yes'
    and advanced_degree_ind = 1
),

find_advancing_degree as (
select
    worker_id,
    highest_attending_advanced_nursing_degree,
    advancing_degree_sort_num,
    attending_advanced_nursing_goal_year
from
    advancing_degree_gather_data
where
    highest_attending_degree_ind = 1
),

highest_any_degree_gather_data as (
select
    worker_id,
    degree as highest_any_degree,
    degree_sort_num + 1000 as highest_any_degree_sort_num, /* sort to the bottom */
    row_number() over(
    partition by worker_id
    order by coalesce(degree_sort_num, 999),
        institution_type,
        major,
        school_name) as highest_row_seq_num,
    case
        when highest_row_seq_num = 1
        then 1
        else 0
        end as highest_any_ind,
    year_degree_received as highest_any_degree_year
from
    {{ ref('stg_nursing_educ_p1_worker_degree') }}
where
    graduated = 'Yes'
),

find_highest_any_degree as (
select
    worker_id,
    highest_any_degree,
    highest_any_degree_sort_num,
    highest_any_degree_year
from
    highest_any_degree_gather_data
where
    highest_any_ind = 1
)

-----------------------------------------
select
    coalesce(entry.degree_sort_num, 9999) as entry_nursing_degree_sort,
    coalesce(entry.degree, 'Missing!') as entry_nursing_degree,
    entry.year_degree_received as entry_nursing_year,

    coalesce(highest.degree_sort_num, 9999) as highest_nursing_degree_sort,
    coalesce(highest.degree, 'Missing!') as highest_nursing_degree,
    highest.year_degree_received as highest_nursing_year,

    coalesce(find_highest_any_degree.highest_any_degree_sort_num, 9999)
        as highest_any_degree_sort,
    coalesce(
        highest_any_degree, 'no Workday degree')
        || case
            when coalesce(find_highest_any_degree.highest_any_degree_sort_num, 9999)
            - 1000 /* highest_any_degree_sort  */
            < coalesce(highest_nursing_degree_sort, 9999) /* highest_Nursing_Degree_Sort */
            and coalesce(highest_any_degree, 'no Workday degree') != 'no Workday degree'
            /* add on Non-Nursing if the highest any degree is not for a nursing major */
            then ' (Non-Nursing)'
            else '' end
        as highest_any_degree,
    find_highest_any_degree.highest_any_degree_year,

    case
        when find_advancing_degree.highest_attending_advanced_nursing_degree is null
        then 0 else 1
        end as attending_advanced_nursing_degree_ind,
    coalesce(find_advancing_degree.advancing_degree_sort_num, 9999)
        as advancing_degree_sort_num,
    coalesce(find_advancing_degree.highest_attending_advanced_nursing_degree, 'n/a')
        as highest_attending_advanced_nursing_degree,
    find_advancing_degree.attending_advanced_nursing_goal_year,

    rn_worker.worker_id,
    rn_worker.worker_name,
    rn_worker.worker_active_ind,
    rn_worker.worker_nursing_category,
    coalesce(worker_inds.worker_nursing_degree_ind, 0)
        as worker_nursing_degree_ind,
    coalesce(worker_inds.worker_nursing_bachelors_ind, 0)
        as worker_nursing_bachelors_ind,
    coalesce(worker_inds.worker_nursing_advanced_degree_ind, 0)
        as worker_nursing_advanced_degree_ind

from
    rn_worker
    left join worker_inds
        on rn_worker.worker_id = worker_inds.worker_id
    left join {{ ref('stg_nursing_educ_p2_degree_hi_lo') }} as entry
        on rn_worker.worker_id = entry.worker_id
        and entry.entry_ind = 1
    left join {{ ref('stg_nursing_educ_p2_degree_hi_lo') }} as highest
        on rn_worker.worker_id = highest.worker_id
        and highest.highest_ind = 1
    left join find_highest_any_degree
        on rn_worker.worker_id = find_highest_any_degree.worker_id
    left join find_advancing_degree
        on rn_worker.worker_id = find_advancing_degree.worker_id

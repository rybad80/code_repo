/* stg_nursing_educ_p2_degree_hi_lo
part 2
ordering to support entry and highest nursing degree levels
and include years for the other metrics around time of education
*/
select
    worker_id,
    degree_in_progress_ind,
    order_position,
    degree,
    graduated,
    institution_type,
    major,
    school_id,
    education_start_year,
    education_end_year,
    include_major_for_nursing_degree_level,
    nursing_degree_ind,
    advanced_degree_ind,
    bachelors_ind,
    degree_sort_num,
    include_for_degree_achieved_ind,
    year_degree_received,
    row_number() over(
        partition by worker_id
        order by coalesce(degree_sort_num, 999),
            institution_type, major, school_id
        ) as highest_row_seq_num,
    row_number() over(
        partition by worker_id
        order by coalesce(degree_sort_num, 0) desc,
            institution_type, major, school_id
        ) as entry_row_seq_num,
    case
        when highest_row_seq_num = 1
        then 1
        else 0
        end as highest_ind,
    case
        when entry_row_seq_num = 1
        then 1
		else 0
		end as entry_ind,
    max(bachelors_ind) over(
        partition by worker_id
        ) as worker_nursing_bachelors_ind,
    max(advanced_degree_ind) over
        (partition by worker_id
        ) as worker_nursing_advanced_degree_ind
from
    {{ ref('stg_nursing_educ_p1_worker_degree') }}
where
    nursing_degree_ind = 1

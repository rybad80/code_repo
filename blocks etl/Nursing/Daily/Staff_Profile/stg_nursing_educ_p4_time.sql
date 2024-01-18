/* stg_nursing_educ_p4_time
Obtaining number of years since entry, highest or advancing (future goal) degree
with metric grouper setting high end of ranges & if nursing 2nd degree
this will feed the stg_nursing_profile_w3_educ_time table
*/
with
gather_non_nursing_degree as (
    select
        worker_id,
        degree_sort_num as min_non_nursing_degree_sort,
        year_degree_received as non_n_degree_initial_year,
        row_number() over(
            partition by worker_id
            order by coalesce(degree_sort_num, 0) desc,
                institution_type, major, school_id
        ) as non_n_sequence_num,
		major,
		degree
    from
        {{ ref('stg_nursing_educ_p1_worker_degree') }}

    where
        include_major_for_nursing_degree_level = 0
        and include_for_degree_achieved_ind = 1
        and degree != 'High School Diploma/GED'
	and graduated = 'Yes'
),

earliest_non_nursing_degree as (
    select
        worker_id,
        min_non_nursing_degree_sort,
        non_n_degree_initial_year,
	major,
	degree
    from
        gather_non_nursing_degree
    where
        non_n_sequence_num = 1
),

degree_years as (
    select
        worker_id,
        entry_nursing_degree_sort,
        entry_nursing_year,
        highest_nursing_degree_sort,
        highest_nursing_year,
        attending_advanced_nursing_goal_year,
        attending_advanced_nursing_degree_ind,
        worker_nursing_degree_ind
    from
        {{ ref('stg_nursing_educ_p3_degree_profile') }}
    where
        worker_nursing_degree_ind = 1
        or attending_advanced_nursing_degree_ind = 1
),

years_since_rows as (
    select
        'YrsSinceNeducEntry' as metric_abbreviation,
        worker_id,
        null as profile_name,
        year(current_date) - entry_nursing_year as numerator
    from
        degree_years
    where
        worker_nursing_degree_ind = 1

    union all

    select
        'YrsSinceNeducHighest' as metric_abbreviation,
        worker_id,
        null as profile_name,
        year(current_date) - highest_nursing_year as numerator
    from
        degree_years
    where
        worker_nursing_degree_ind = 1
)

select
    'Nursing2ndDeg' as metric_abbreviation,
    earliest_non_nursing_degree.worker_id,
    'After ' || earliest_non_nursing_degree.major as profile_name,
    null as metric_grouper,
    1 as numerator
from
    earliest_non_nursing_degree
    inner join degree_years on earliest_non_nursing_degree.worker_id = degree_years.worker_id
where
    degree_years.entry_nursing_year > earliest_non_nursing_degree.non_n_degree_initial_year
    and degree_years.worker_nursing_degree_ind = 1

union all

select
    metric_abbreviation,
    worker_id,
    profile_name,
    case
        when numerator < 1 then '00'
        when numerator < 3 then '02'
        when numerator < 5 then '04'
        when numerator < 11 then '10'
        else '99'
    end as metric_grouper,
    numerator
from
    years_since_rows

union all

select
    'YrsToAdvDegGoal' as metric_abbreviation,
    degree_years.worker_id,
    null as profile_name,
    null as metric_grouper,
    attending_advanced_nursing_goal_year - year(current_date) as numerator
from
    degree_years
where
    attending_advanced_nursing_degree_ind = 1

/* stg_nursing_educ_p1_worker_degree
part 1
gathering education data & setting indicator for the nursing majors
*/
select
    worker_education.worker_id,
    worker_education.degree_in_progress_ind,
    worker_education.order_position,
    worker_education.degree,
    worker_education.education_state,
    worker_education.graduated,
    worker_education.institution_type,
    worker_education.major,
    worker_education.school_name,
    worker_education.school_id,
    worker_education.education_start_year,
    worker_education.education_end_year,
    case when worker_education.major in (
        'Adult Nurse Practitioner',
        'Nursing',
        'Nursing Administration',
        'Nursing Practice',
        'Nursing Science',
        'Perinatal Nursing')
    then 1
    else 0 end as include_major_for_nursing_degree_level,
    worker_education.advanced_degree_ind,
    worker_education.advanced_degree_ind
    * include_major_for_nursing_degree_level as nursing_advanced_degree_ind,
    worker_education.bachelors_ind,
    case
        when graduated = 'Yes'
        then worker_education.include_for_degree_achieved_ind
            * include_major_for_nursing_degree_level
        else 0
        end as nursing_degree_ind,
    worker_education.bachelors_ind
    * include_major_for_nursing_degree_level as nursing_bachelors_ind,
    worker_education.degree_sort_num,
    worker_education.include_for_degree_achieved_ind,
    worker_education.year_degree_received
from
    {{ ref('worker_education') }} as worker_education

select
    stg_worker_education.worker_id,
    case
        when stg_worker_education.degree = 'Degree In Progress' then 'Yes'
        when stg_worker_education.year_degree_received is null
        and stg_worker_education.education_end_year > year(current_date) then 'Yes'
        else 'No'
    end as degree_in_progress_ind,
    dense_rank() over (
        partition by stg_worker_education.worker_id
        order by
            stg_worker_education.education_start_year asc
    ) as order_position,
    stg_worker_education.degree,
    case
        when stg_worker_education.edu_country = 'United States of America' then stg_worker_education.edu_state
        else 'N/A - Outside of US'
    end as education_state,
    stg_worker_education.graduated,
    stg_worker_education.institution_type,
    stg_worker_education.major,
    stg_worker_education.school_name,
    stg_worker_education.school_id,
    stg_worker_education.education_start_year,
    stg_worker_education.education_end_year,
    stg_worker_education.year_degree_received,
    stg_worker_education.degree_id,
    case
        when stg_worker_education.degree in (
            'Technical Diploma',
            'High School Diploma/GED',
            'Associate''s Degree', --noqa: PRS, L048
            'Bachelor''s Degree',
            'Certificate'
        ) then 0
        when stg_worker_education.degree in (
            'PhD',
            'Doctorate',
            'Master''s Degree', --noqa: PRS, L048
            'Juris Doctor (JD)'
        ) then 1
        else null
    end as advanced_degree_ind,
    case --noqa: PRS, L048
        when stg_worker_education.degree = 'Bachelor''s Degree' then 1 --noqa: PRS, L048
        else 0
    end as bachelors_ind,
    case
        when stg_worker_education.degree = 'PhD' then 10
        when stg_worker_education.degree = 'Doctorate' then 15
        when stg_worker_education.degree = 'Juris Doctor (JD)' then 32
        when stg_worker_education.degree = 'Master''s Degree' then 35
        when stg_worker_education.degree = 'Bachelor''s Degree' then 40
        when stg_worker_education.degree = 'Associate''s Degree' then 50
        when stg_worker_education.degree = 'Certificate' then 70
        when stg_worker_education.degree = 'High School Diploma/GED' then 100
        when stg_worker_education.degree = 'Technical Diploma' then 105
        when stg_worker_education.degree = 'Degree In Progress' then 888
    end as degree_sort_num,
    case
        when stg_worker_education.degree in (
            'High School Diploma/GED',
            'Degree In Progress',
            'Certificate'
        ) then 0
        when stg_worker_education.degree in (
            'PhD',
            'Doctorate',
            'Juris Doctor (JD)',
            'Master''s Degree',
            'Bachelor''s Degree',
            'Associate''s Degree',
            'Technical Diploma'
        ) then 1
    end as include_for_degree_achieved_ind
from
    {{ ref('stg_worker_education') }} as stg_worker_education

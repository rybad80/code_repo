select
    stg_patient.mrn,
    rsh_research_info.research_study_name,
    rsh_research_info.study_code,
    enroll_info.enroll_id,
    enroll_info.research_study_id,
    enroll_info.enroll_start_dt,
    enroll_info.enroll_end_dt,
    case
        when enroll_info.enroll_start_dt >= '2021-01-01' then enroll_info.enroll_start_dt
        when enroll_info.enroll_end_dt >= '2021-01-01' then enroll_info.enroll_end_dt
        else null end
    as cy21_initial_date,
    case
        when enroll_info.enroll_start_dt >= '2022-07-01' then enroll_info.enroll_start_dt
        when enroll_info.enroll_end_dt >= '2022-07-01' then enroll_info.enroll_end_dt
        else null end
    as fy23_initial_date,
    1 as clinical_trial_ind
from {{ source('workday_ods', 'enroll_info') }} as enroll_info
inner join {{ source('workday_ods', 'rsh_research_info') }} as rsh_research_info
    on enroll_info.research_study_id = rsh_research_info.research_id
inner join {{ ref('stg_patient') }} as stg_patient
    on enroll_info.pat_id = stg_patient.pat_id
inner join {{ ref('lookup_frontier_program_definitions') }} as lookup_frontier_program_definitions
    on rsh_research_info.study_code = lookup_frontier_program_definitions.code
where
    ((enroll_info.enroll_end_dt is null and enroll_info.enroll_start_dt >= '2021-01-01')
        or (enroll_info.enroll_end_dt >= '2021-01-01'))

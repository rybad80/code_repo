{{ config(materialized='table', dist='encounter_key') }}

with encounter_dx_link as (
    select
        stg_encounter.encounter_key,
        problem_list.problem_list_id,
        coalesce(
            problem_list.noted_date, stg_encounter.encounter_date, stg_encounter.dob)
        as noted_date,
        date(coalesce(
            problem_list.noted_date - 1, stg_encounter.encounter_date, stg_encounter.dob
        )) as effective_noted_date,
        coalesce(
            problem_list.resolved_date, current_date)
        as effective_resolved_date,
        case
            when
                stg_encounter_inpatient.visit_key is not null --IP visit case
                and (
                    date(stg_encounter.hospital_admit_date) between effective_noted_date
                        and effective_resolved_date
                    or effective_noted_date between date(stg_encounter.hospital_admit_date)
                        and coalesce(date(stg_encounter.hospital_discharge_date), current_date)
                )
                then 1
            when
                stg_encounter_inpatient.visit_key is null --non-IP visit case
                and stg_encounter.encounter_date
                between effective_noted_date and effective_resolved_date
                then 1
            else 0
        end as include_dx_ind
    from
        {{ref('stg_encounter')}} as stg_encounter
        left join {{ref('stg_encounter_inpatient')}} as stg_encounter_inpatient
            on stg_encounter_inpatient.visit_key = stg_encounter.visit_key
        inner join {{source('clarity_ods','problem_list')}} as problem_list
            on problem_list.pat_id = stg_encounter.pat_id
)

select
    stg_encounter.encounter_key,
    stg_encounter.csn as pat_enc_csn_id,
    problem_list.dx_id,
    min(encounter_dx_link.noted_date) as noted_date,
    min(problem_list.resolved_date) as resolved_date
from
    encounter_dx_link
    inner join {{ref('stg_encounter')}} as stg_encounter
        on stg_encounter.encounter_key = encounter_dx_link.encounter_key
    inner join {{source('clarity_ods','problem_list')}} as problem_list
        on problem_list.problem_list_id = encounter_dx_link.problem_list_id
where
    encounter_dx_link.include_dx_ind = 1
    and problem_list.problem_status_c != 3 -- Deleted
    and problem_list.pat_id is not null
    and stg_encounter.appointment_status_id not in (3, 4, 5) -- Exclude no show/left without being seen/cancelled
    and stg_encounter.encounter_type_id not in (
        150, -- ABSTRACT
        5, -- CANCELED
        7, -- CONTACT MOVED
        10, -- EMPTY
        1011, -- END
        1066, -- ERROR
        80, -- EXTERNAL CONTACT
        104, -- EXTERNAL HOSPITAL ADMISSION
        109, -- HISTORY
        303, -- HOSPITAL ABSTRACTION
        1063, -- INPATIENT ABSTRACT
        114, -- MEDS VOID (WEB)
        202, -- NO SHOW
        31, -- PCP/CLINIC CHANGE
        118, -- PHARMACY VISIT
        77, -- PLAN OF CARE DOCUMENTATION
        98, -- POST MORTEM DOCUMENTATION
        301, -- PVA ABSTRACT
        11, -- RESEARCH ENCOUNTER
        115 -- RESOLUTE PROFESSIONAL BILLING HOSPITAL PROF FEE
    )
group by
    stg_encounter.encounter_key,
    problem_list.dx_id,
    pat_enc_csn_id

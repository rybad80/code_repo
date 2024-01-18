with stage as (
    select
        visit_key,
        1 as inpatient_census_ind,
        0 as office_visit_ind,
        0 as notes_ind,
        case when lower(service) = 'neurology' then 1 else 0 end as neurology_ind,
        case when lower(service) = 'neurosurgery' then 1 else 0 end as neurosurgery_ind,
        case when lower(service) = 'leukodystrophy' then 1 else 0 end as leukodystrophy_ind
    from {{ ref('capacity_ip_hourly_census')}}
    where
        lower(service) in ('neurology', 'neurosurgery', 'leukodystrophy')
    group by
        visit_key,
        service
    union all
    select
        stg_encounter.visit_key,
        0 as inpatient_census_ind,
        1 as office_visit_ind,
        0 as notes_ind,
        case when lower(specialty) = 'neurology' then 1 else 0 end as neurology_ind,
        case when lower(specialty) = 'neurosurgery' then 1 else 0 end as neurosurgery_ind,
        case when lower(specialty) = 'leukodystropy' then 1 else 0 end as leukodystrophy_ind
    from {{ ref('stg_encounter')}} as stg_encounter
    inner join {{ source('cdw', 'department')}} as department
        on stg_encounter.department_id = department.dept_id
    where
        encounter_type_id in (50, 101) -- appointment, office visit
        and appointment_status_id in (2, 6) --filtering for completed visits
        and lower(specialty) in ('neurology', 'neurosurgery', 'leukodystropy') -- spelled leukodystropy in table
    group by
        stg_encounter.visit_key,
        specialty
    union all
    select
        encounter_inpatient.visit_key,
        0 as inpatient_census_ind,
        0 as office_visit_ind,
        1 as notes_ind,
        case when lower(version_author_service_name) = 'neurology' then 1 else 0 end as neurology_ind,
        case when lower(version_author_service_name) = 'neurosurgery' then 1 else 0 end as neurosurgery_ind,
        case when lower(version_author_service_name) = 'leukodystrophy' then 1 else 0 end as leukodystrophy_ind
    from {{ ref('encounter_inpatient')}} as encounter_inpatient
    inner join {{ ref('note_edit_metadata_history')}} as note_edit_metadata_history
        on encounter_inpatient.visit_key = note_edit_metadata_history.visit_key
    where lower(version_author_service_name) in ('neurology', 'neurosurgery', 'leukodystrophy')
        and lower(note_status) != 'deleted' -- note must not be deleted
        and lower(note_type) = 'consult note' -- must be a consult note
    group by
        encounter_inpatient.visit_key,
        version_author_service_name
)

select
    stg_encounter.visit_key,
    stg_encounter.mrn,
    stg_encounter.pat_key,
    stg_encounter.patient_name,
    stg_encounter.dob,
    stg_encounter.encounter_date,
    max(inpatient_census_ind) as inpatient_census_ind,
    max(office_visit_ind) as office_visit_ind,
    max(notes_ind) as notes_ind,
    max(neurology_ind) as neurology_ind,
    max(neurosurgery_ind) as neurosurgery_ind,
    max(leukodystrophy_ind) as leukodystrophy_ind
from stage
    inner join {{ ref('stg_encounter')}} as stg_encounter
        on stage.visit_key = stg_encounter.visit_key
group by
    stg_encounter.visit_key,
    stg_encounter.mrn,
    stg_encounter.pat_key,
    stg_encounter.patient_name,
    stg_encounter.dob,
    stg_encounter.encounter_date

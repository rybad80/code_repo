select
    stg_encounter.visit_key as primary_key,
    stg_encounter.encounter_date,
    case
        when patient_all.mychop_activation_ind = 1
            then patient_all.pat_key
        else null end as mychop_activation_pat_key,
    patient_all.pat_key,
    case
        when patient_all.mychop_activation_ind = 1
        then 1 else 0 end as mychop_visit_activation_ind
from
    {{ref('stg_encounter_outpatient_raw')}} as stg_encounter
    inner join {{ source('cdw', 'department')}} as department on department.dept_key = stg_encounter.dept_key
    inner join {{ source('cdw', 'location')}} as location on department.rev_loc_key = location.loc_key
    inner join {{ref('patient_all')}} as patient_all on stg_encounter.pat_key = patient_all.pat_key
where
    stg_encounter.encounter_date < current_date
    and stg_encounter.encounter_date >= '01/01/2019'
    and stg_encounter.cancel_noshow_ind = 0
    and appointment_status_id in (6, 2, -2) --'arrived','completed','na'
    and stg_encounter.encounter_type_id in (
        3, -- hosp enc
        50, --appointment
        101, --office visit
        151, --inpatient
        152, --outpatient
        153, --emergency
        204) --sunday office visit
    and specialty_care_ind = 1
    and lower(location.rpt_grp_6) = 'chca' --revenue_location

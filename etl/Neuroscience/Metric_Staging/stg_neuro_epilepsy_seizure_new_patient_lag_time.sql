select
    'Median New Epilepsy/Seizure Patient Lag Time (Days)' as metric_name,
    scheduling_specialty_care_appointments.appointment_made_date,
    scheduling_specialty_care_appointments.npv_appointment_lag_days as num,
    'median' as num_calculation,
    'median' as metric_type,
    'down' as desired_direction,
    scheduling_specialty_care_appointments.visit_key as primary_key,
    scheduling_specialty_care_appointments.department_name,
    initcap(coalesce(department_care_network.department_center,
        scheduling_specialty_care_appointments.department_name)) as department_center,
    scheduling_specialty_care_appointments.provider_name
from
    {{ ref('scheduling_specialty_care_appointments')}} as scheduling_specialty_care_appointments
    inner join
        {{ref('department_care_network')}} as department_care_network
        on scheduling_specialty_care_appointments.dept_key = department_care_network.dept_key
    inner join
        {{ ref('diagnosis_encounter_all')}} as diagnosis_encounter_all
        on scheduling_specialty_care_appointments.visit_key = diagnosis_encounter_all.visit_key
    inner join
        {{ ref('lookup_neuro_dx_grouping')}} as lookup_neuro_dx_grouping -- dx lookup validated by neuro team
        on diagnosis_encounter_all.icd10_code like lookup_neuro_dx_grouping.dx_cd
    inner join
        {{ ref('stg_encounter')}} as stg_encounter
        on scheduling_specialty_care_appointments.visit_key = stg_encounter.visit_key
    inner join {{source('cdw','provider')}} as provider
        on provider.prov_key = stg_encounter.prov_key
where
    scheduling_specialty_care_appointments.npv_lag_incl_ind = 1
    and lower(scheduling_specialty_care_appointments.specialty_name) = 'neurology'
    and lower(lookup_neuro_dx_grouping.dx_grouping) = 'epilepsy/seizure'
    and lower(stg_encounter.chop_market_raw) in ('primary', 'secondary')
    and diagnosis_encounter_all.visit_primary_ind = 1
    and stg_encounter.department_id != 101001157 -- BGR EEG LAB
    and provider.prov_id
       not in ('1000034', '1000998', '532411', '6325') --EEG providers
group by
    scheduling_specialty_care_appointments.appointment_made_date,
    num,
    primary_key,
    scheduling_specialty_care_appointments.department_name,
    department_care_network.department_center,
    scheduling_specialty_care_appointments.department_name,
    scheduling_specialty_care_appointments.provider_name

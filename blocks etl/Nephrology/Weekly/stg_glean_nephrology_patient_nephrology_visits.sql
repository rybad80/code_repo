with patient_nephrology_visits as ( -- all in-person nephrology visits each patient in cohort, in the approximate
                                    -- 18 months preceding the patient's most recent nephrology visit - used to
                                    -- identify each patient's primary nephrology site
    select
        registry.pat_key,
        registry.mrn,
        registry.last_nephrology_department_name,
        registry.last_neph_visit,
        stg_encounter_outpatient_raw.encounter_date,
        stg_encounter_outpatient_raw.visit_key,
        stg_encounter_outpatient_raw.department_id,
        stg_encounter_outpatient_raw.department_name,
        provider.prov_type
   from
        {{ ref('stg_glean_nephrology_registry')}} as registry
        left join {{ ref('stg_encounter_outpatient_raw') }} as stg_encounter_outpatient_raw
            on registry.pat_key = stg_encounter_outpatient_raw.pat_key
        left join {{source('cdw', 'provider')}} as provider
            on stg_encounter_outpatient_raw.prov_key = provider.prov_key
    where
        stg_encounter_outpatient_raw.specialty_care_ind = 1 -- in-person office visits, also includes video visits
        and lower(stg_encounter_outpatient_raw.visit_type) not like '%video%' -- excludes video visits from above
        -- and (extract(epoch from registry.last_neph_visit) 
        --  - extract (epoch from stg_encounter_outpatient_raw.encounter_date))
        -- / 86400.0 between 0.0 and 548.0 -- previous way of identifying visits within last 18 months
        -- changing for precision.
        and stg_encounter_outpatient_raw.encounter_date
            between add_months(registry.last_neph_visit, -18) and registry.last_neph_visit
            -- visits in last 18 months
        and lower(stg_encounter_outpatient_raw.department_name) like '%nephrology%'
        and stg_encounter_outpatient_raw.appointment_status_id in ('-2', -- not applicable
                                                    '2', -- completed
                                                    '6') -- arrived                                               
)

select
    *
from
    patient_nephrology_visits

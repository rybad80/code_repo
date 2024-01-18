with encounter_dates as (
    select
        stg_encounter.dept_key,
        max(stg_encounter_outpatient_raw.primary_care_ind) as primary_care_ind,
        max(stg_encounter_outpatient_raw.specialty_care_ind) as specialty_care_ind,
        min(case
            when stg_encounter.appointment_status_id in('2', '6')
                then stg_encounter.encounter_date --Care Network
            when stg_encounter.hospital_discharge_date is not null
                then stg_encounter.encounter_date --Inpatient
            when stg_encounter.encounter_type_id = '91'
                and stg_encounter.encounter_date < current_date
                then stg_encounter.encounter_date --Home Care
            else null
        end) as first_completed_encounter_date,
        max(case
                when stg_encounter.appointment_status_id in('2', '6')
                    then stg_encounter.encounter_date --Care Network
                when stg_encounter.hospital_discharge_date is not null
                    then stg_encounter.encounter_date --Inpatient
                when stg_encounter.encounter_type_id = '91' -- Home Care
                    and stg_encounter.encounter_date < current_date
                    then stg_encounter.encounter_date else null
        end) as last_completed_encounter_date
    from
        {{ref('stg_encounter')}} as stg_encounter
        inner join {{ref('stg_department_all')}} as stg_department_all
            on stg_department_all.dept_key = stg_encounter.dept_key
        left join {{ref('stg_encounter_outpatient_raw')}} as stg_encounter_outpatient_raw
            on stg_encounter_outpatient_raw.visit_key = stg_encounter.visit_key
    where
        stg_encounter_outpatient_raw.primary_care_ind = 1
        or stg_encounter_outpatient_raw.specialty_care_ind = 1
        or stg_encounter_outpatient_raw.urgent_care_ind = 1
        or stg_department_all.scc_ind = 1
    group by
        stg_encounter.dept_key
)

select
    stg_department_all.dept_key,
    stg_department_all.department_name,
    stg_department_all.department_id,
    stg_department_all.specialty_name,
    stg_department_all.intended_use_name,
    stg_department_all.intended_use_id,
    stg_department_all.revenue_location_group,
    stg_department_all.location_name,
    stg_department_all.location_id,
    stg_department_all.department_center,
    stg_department_all.scc_abbreviation,
    stg_department_all.mailing_city,
    stg_department_all.mailing_state,
    stg_department_all.mailing_zip,
    encounter_dates.first_completed_encounter_date,
    encounter_dates.last_completed_encounter_date,
    stg_department_all.scc_ind,
    stg_department_all.professional_billing_ind,
    case
        when stg_department_all.intended_use_id = 1009 -- Outpatient Specialty Care
             and lower(stg_department_all.specialty_name) in ('adolescent',
                                                              'allergy',
                                                              'cardiology',
                                                              'dermatology',
                                                              'developmental pediatric rehab',
                                                              'developmental pediatrics',
                                                              'endocrinology',
                                                              'gastroenterology',
                                                              'general pediatrics',
                                                              'genetics',
                                                              'hematology',
                                                              'immunology',
                                                              'infectious disease',
                                                              'neonatology',
                                                              'nephrology',
                                                              'neurology',
                                                              'oncology',
                                                              'pulmonary',
                                                              'rheumatology')
             then 1
        when stg_department_all.intended_use_id = 1009 -- Outpatient Specialty Care
             and stg_department_all.department_id in (101012165, -- BGR AADP MULTI D CLNC
                                                      101012135, -- BGR BONE HLTH MULTI D
                                                      101012171, -- BGR CURED MULTI D CLNC
                                                      101012176, -- BGR CVAP MULTI D PGM
                                                      101012157, -- BGR EB MULTI D CLINIC
                                                      101012173, -- BGR HIFP MULTID CLN
                                                      101012169, -- BGR IDFP MDC
                                                      101012101, -- BGR INTGRTV FOOD REACT
                                                      101012162, -- BGR LUPUS MULTID CLN
                                                      101012174, -- BGR PAPA MULTI D CLN
                                                      101022063, -- VIRTUA BONE HLTH MULTI
                                                      10101170) -- WOOD IDFP MDC
             then 1
             else 0
    end as pediatrics_department_ind,
    case
        when year(add_months(encounter_dates.last_completed_encounter_date, 6)) = year(add_months(current_date, 6))
            then 1 else 0
    end as encounter_current_fy_ind,
    encounter_dates.primary_care_ind,
    encounter_dates.specialty_care_ind,
    stg_department_all.record_status_active_ind
from
    {{ref('stg_department_all')}} as stg_department_all
    inner join encounter_dates on encounter_dates.dept_key = stg_department_all.dept_key

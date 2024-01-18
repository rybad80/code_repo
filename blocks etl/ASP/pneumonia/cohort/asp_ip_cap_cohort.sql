with cohort as (
    select
        stg_asp_ip_cap_cohort.visit_key,
        --did patient expire within 48 hours of admission?
        max(case when stg_patient.death_date <= stg_asp_ip_cap_cohort.inpatient_admit_date + interval('48 hours')
            then 1 else 0 end) as deceased_first_48_hrs_ind,
        --was patient administered antibiotic within the first 48 hours of admission?
        max(case when stg_asp_ip_cap_cohort_abx.first_abx_time
                <= stg_asp_ip_cap_cohort.inpatient_admit_date + interval('48 hours')
            then 1 else 0 end) as abx_first_48_hrs_ind,
        --was patient administered a second antibiotic within 22 (inclusive) to 50 (exclusive) hrs of first dose?
        max(case when stg_asp_ip_cap_cohort_abx.hrs_since_first_abx >= 22
            and stg_asp_ip_cap_cohort_abx.hrs_since_first_abx < 50
            then 1 else 0 end) as abx_administered_22_to_50_hrs_ind,
        --was patient discharged in first 48 hours of admission with a discharge antibiotic?
        max(case when stg_asp_ip_cap_cohort.hospital_discharge_date
                <= stg_asp_ip_cap_cohort.inpatient_admit_date + interval('48 hours')
            and stg_asp_ip_cap_cohort_abx.outpatient_med_ind = 1
            then 1 else 0 end) as discharged_with_abx_first_48_hrs_ind,
        --did patient have a pulmonary condition within the last year?
        min(case when stg_asp_ip_cap_cohort.hospital_admit_date
            <= stg_asp_ip_cap_cohort_pulmonary.hospital_admit_date + interval('1 year')
            then 0 else 1 end) as no_pulmonary_condition_last_yr_ind
    from
        {{ref('stg_asp_ip_cap_cohort')}} as stg_asp_ip_cap_cohort
        inner join {{ref('stg_asp_ip_cap_cohort_abx')}} as stg_asp_ip_cap_cohort_abx
            on stg_asp_ip_cap_cohort.visit_key = stg_asp_ip_cap_cohort_abx.visit_key
        inner join {{ref('stg_asp_ip_cap_cohort_imaging')}} as stg_asp_ip_cap_cohort_imaging
            on stg_asp_ip_cap_cohort.visit_key = stg_asp_ip_cap_cohort_imaging.visit_key
        inner join {{ref('stg_patient')}} as stg_patient
            on stg_asp_ip_cap_cohort.pat_key = stg_patient.pat_key
        --did patient have a pulmonary condition within the last year?
        left join {{ref('stg_asp_ip_cap_cohort_pulmonary')}} as stg_asp_ip_cap_cohort_pulmonary
            on stg_asp_ip_cap_cohort.pat_key = stg_asp_ip_cap_cohort_pulmonary.pat_key
            and stg_asp_ip_cap_cohort_pulmonary.hospital_admit_date < stg_asp_ip_cap_cohort.hospital_admit_date
    where
        --Antibiotic administered inpatient or prescribed outpatient
        stg_asp_ip_cap_cohort_abx.ip_or_rx_ind = 1
    group by
        stg_asp_ip_cap_cohort.visit_key
)
select
    stg_asp_ip_cap_cohort.visit_key,
    stg_asp_ip_cap_cohort.pat_key,
    stg_asp_ip_cap_cohort.hospital_admit_date,
    stg_asp_ip_cap_cohort.hospital_discharge_date,
    stg_asp_ip_cap_cohort.inpatient_admit_date,
    stg_asp_ip_cap_cohort.inpatient_los_days,
    stg_asp_ip_cap_cohort.patient_name,
    stg_asp_ip_cap_cohort.mrn,
    stg_asp_ip_cap_cohort.csn,
    stg_asp_ip_cap_cohort.dob,
    stg_asp_ip_cap_cohort.sex,
    stg_asp_ip_cap_cohort.admission_campus,
    stg_asp_ip_cap_cohort.admission_service,
    stg_asp_ip_cap_cohort.discharge_service,
    stg_asp_ip_cap_cohort.los_7_days_ind,
    stg_asp_ip_cap_cohort.icu_los_days,
    stg_asp_ip_cap_cohort.age_years,
    stg_asp_ip_cap_cohort.complicated_pneumonia_ind,
    stg_asp_ip_cap_cohort.other_dx_ind
from
    {{ref('stg_asp_ip_cap_cohort')}} as stg_asp_ip_cap_cohort
    inner join cohort
        on stg_asp_ip_cap_cohort.visit_key = cohort.visit_key
where
    --patient does not have concurrent infection on the same encounter
    stg_asp_ip_cap_cohort.ci_ind = 0
    --patient did not expire within 48 hours of admission
    and cohort.deceased_first_48_hrs_ind = 0
    --first abx dose within 48 hrs
    and cohort.abx_first_48_hrs_ind = 1
    --another dose within [22-50) hrs of first dose or discharged within 48 hrs with antibiotic
    and cohort.abx_administered_22_to_50_hrs_ind
        + cohort.discharged_with_abx_first_48_hrs_ind != 0
    and cohort.no_pulmonary_condition_last_yr_ind = 1

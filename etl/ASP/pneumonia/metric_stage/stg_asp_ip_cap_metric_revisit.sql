--encounters that led to revisits in ED or Primary Care, or readmissions within 1-2 weeks
--revisits must include an ICD-10 code for CAP / CP and an outpatient antibiotic prescription
--readmissions must include an ICD-10 code for CAP / CP and any antibiotic treatment (IP or OP)
select
    asp_ip_cap_cohort.visit_key,
    max(case when
        stg_encounter_ed.ed_arrival_date < asp_ip_cap_cohort.hospital_discharge_date + interval('7 days')
        or stg_encounter_outpatient.encounter_date
            < asp_ip_cap_cohort.hospital_discharge_date + interval('7 days')
        then stg_asp_ip_cap_cohort_abx.outpatient_med_ind else 0 end) as revisit_7_day_ind,
    max(case when stg_encounter_ed.ed_arrival_date is not null
        or stg_encounter_outpatient.encounter_date is not null
        then stg_asp_ip_cap_cohort_abx.outpatient_med_ind else 0 end) as revisit_14_day_ind,
    max(encounter_readmission.readmit_7_day_ind) as readmit_7_day_ind,
    max(encounter_readmission.readmit_14_day_ind) as readmit_14_day_ind
from
    {{ref('asp_ip_cap_cohort')}} as asp_ip_cap_cohort
    inner join {{ref('stg_asp_ip_cap_cohort_all')}} as stg_asp_ip_cap_cohort_all
        on asp_ip_cap_cohort.pat_key = stg_asp_ip_cap_cohort_all.pat_key
    inner join {{ref('stg_asp_ip_cap_cohort_abx')}} as stg_asp_ip_cap_cohort_abx
        on stg_asp_ip_cap_cohort_all.visit_key = stg_asp_ip_cap_cohort_abx.visit_key
    left join {{ref('stg_encounter_ed')}} as stg_encounter_ed
        on stg_asp_ip_cap_cohort_abx.visit_key = stg_encounter_ed.visit_key
        and stg_encounter_ed.ed_patients_seen_ind = 1
        and stg_encounter_ed.ed_arrival_date
            between asp_ip_cap_cohort.hospital_discharge_date
            and asp_ip_cap_cohort.hospital_discharge_date + interval('14 days')
    left join {{ref('stg_encounter_outpatient')}} as stg_encounter_outpatient
        on stg_asp_ip_cap_cohort_abx.visit_key = stg_encounter_outpatient.visit_key
        and stg_encounter_outpatient.primary_care_ind = 1
    left join {{ref('encounter_readmission')}} as encounter_readmission
        on stg_asp_ip_cap_cohort_abx.visit_key = encounter_readmission.readmit_visit_key
        and encounter_readmission.readmit_14_day_ind = 1
where
    stg_asp_ip_cap_cohort_abx.encounter_date between
        date(asp_ip_cap_cohort.hospital_discharge_date)
        and asp_ip_cap_cohort.hospital_discharge_date + interval('14 days')
    --ICD-10 code for CAP or Complicated Pneumonia
    and (
        stg_asp_ip_cap_cohort_all.cohort_ind = 1
        or stg_asp_ip_cap_cohort_all.complicated_pneumonia_ind = 1
    )
group by
    asp_ip_cap_cohort.visit_key

with first_icu_transfer as (
    --region when did patient first enter ICU
    select
        visit_key,
        1 as icu_first_48_hrs_ind
    from {{ref('adt_department_group')}}
    where
        --patient was in ICU unit in first 48 hours of admission
        bed_care_group in (
            'PHL ICU',
            'PHL ICU FLEX', --Overflow
            'KOPH ICU'
        )
        and enter_date < hospital_admit_date + interval('48 hours')
        and initial_service = 'Critical Care'
    group by
        visit_key
),

inpatient_services as (
    --was patient in the following services during encounter?
    select
        visit_key,
        max(case when service = 'General Pediatrics'
            then 1 else 0 end) as gen_peds_service_ind,
        max(case when service = 'Critical Care'
            then 1 else 0 end) as critical_care_service_ind,
        max(case when service = 'Complex Care'
            then 1 else 0 end) as complex_care_service_ind
    from {{ref('adt_service')}}
    where
        service in (
            'General Pediatrics',
            'Critical Care',
            'Complex Care'
        )
    group by
        visit_key
)

select
    asp_ip_cap_cohort.visit_key,
    stg_patient.race,
    stg_patient.ethnicity,
    case when asp_ip_cap_cohort.age_years <= 0.5 then '<6 months'
        when asp_ip_cap_cohort.age_years < 6 then '6 months to 6 years'
        when asp_ip_cap_cohort.age_years < 12 then '6 years to 12 years'
        when asp_ip_cap_cohort.age_years < 18 then '12 years to 18 years'
        else '18+' end as age_group,
    stg_asp_ip_cap_metric_care_team.admission_team,
    stg_asp_ip_cap_metric_care_team.discharge_team,
    coalesce(inpatient_services.gen_peds_service_ind, 0) as gen_peds_service_ind,
    coalesce(inpatient_services.critical_care_service_ind, 0) as critical_care_service_ind,
    coalesce(inpatient_services.complex_care_service_ind, 0) as complex_care_service_ind,
    --count patients as outside hospital transfer if they were transported inpatient
    case when transport_encounter_all.accepting_department != 'ED'
        then 1 else 0 end as transport_ind,
    --was patient admitted to ICU within 48 hours of admission and remained in ICU for 2+ days?
    case when first_icu_transfer.icu_first_48_hrs_ind = 1
        and asp_ip_cap_cohort.icu_los_days >= 2
        then 1 else 0 end as icu_48_hrs_ind,
    coalesce(stg_asp_ip_cap_metric_revisit.revisit_7_day_ind, 0) as revisit_7_day_ind,
    coalesce(stg_asp_ip_cap_metric_revisit.revisit_14_day_ind, 0) as revisit_14_day_ind,
    coalesce(stg_asp_ip_cap_metric_revisit.readmit_7_day_ind, 0) as readmit_7_day_ind,
    coalesce(stg_asp_ip_cap_metric_revisit.readmit_14_day_ind, 0) as readmit_14_day_ind,
    coalesce(
        stg_asp_ip_cap_metric_procedure.cap_pathway_48_hrs_ind,
        0
    ) as cap_pathway_48_hrs_ind
from
    {{ref('asp_ip_cap_cohort')}} as asp_ip_cap_cohort
    inner join {{ref('stg_patient')}} as stg_patient
        on asp_ip_cap_cohort.pat_key = stg_patient.pat_key
    left join {{ref('stg_asp_ip_cap_metric_care_team')}} as stg_asp_ip_cap_metric_care_team
        on asp_ip_cap_cohort.visit_key = stg_asp_ip_cap_metric_care_team.visit_key
    left join {{ref('transport_encounter_all')}} as transport_encounter_all
        on asp_ip_cap_cohort.visit_key = transport_encounter_all.admit_visit_key
        and transport_encounter_all.transport_type = 'Inbound'
        and transport_encounter_all.final_status = 'completed'
    left join inpatient_services
        on asp_ip_cap_cohort.visit_key = inpatient_services.visit_key
    left join first_icu_transfer
        on asp_ip_cap_cohort.visit_key = first_icu_transfer.visit_key
    left join {{ref('stg_asp_ip_cap_metric_revisit')}} as stg_asp_ip_cap_metric_revisit
        on asp_ip_cap_cohort.visit_key = stg_asp_ip_cap_metric_revisit.visit_key
    left join {{ref('stg_asp_ip_cap_metric_procedure')}} as stg_asp_ip_cap_metric_procedure
        on asp_ip_cap_cohort.visit_key = stg_asp_ip_cap_metric_procedure.visit_key

-- purpose: organize location info & create patient day metrics for reporting 
-- granularity: one row per department per day

with fact as (
    select
        'LOCATION' as data_type,
        stg_asp_inpatient_cohort.visit_key,
        stg_asp_inpatient_adt.visit_event_key,
        stg_asp_inpatient_cohort.pat_key,
        stg_asp_inpatient_cohort.patient_status,
        stg_asp_inpatient_cohort.patient_age_category,
        stg_asp_inpatient_adt.cohort_date,
        -- used to calculate patient days
        {{
            dbt_utils.surrogate_key([
                'stg_asp_inpatient_cohort.visit_key',
                'stg_asp_inpatient_adt.cohort_date'
            ])
        }}                                           as patient_day_key,
        stg_asp_inpatient_adt.department_group_name  as adt_department,
        stg_asp_inpatient_adt.department_center_abbr as adt_department_center,
        stg_asp_inpatient_cohort.adt_department_center_admit,
        stg_asp_inpatient_cohort.adt_department_center_discharge,
        stg_asp_inpatient_adt.adt_service,
        stg_asp_inpatient_adt.bmt_ind,
        stg_asp_inpatient_adt.visit_event_date_key

    from
        {{ref('stg_asp_inpatient_cohort')}}         as stg_asp_inpatient_cohort
        inner join {{ref('stg_asp_inpatient_adt')}} as stg_asp_inpatient_adt
            on stg_asp_inpatient_cohort.visit_key = stg_asp_inpatient_adt.visit_key

    where
        stg_asp_inpatient_adt.cohort_date >= date('2015-01-01')
)

select
    fact.data_type,
    fact.visit_key,
    fact.visit_event_key,
    fact.visit_event_date_key,
    fact.pat_key,
    fact.patient_status,
    fact.patient_age_category,
    patient.pat_mrn_id as mrn,
    patient.full_nm as full_name,
    fact.cohort_date,
    fact.patient_day_key,
    fact.adt_department,
    fact.adt_department_center,
    fact.adt_department_center_admit,
    fact.adt_department_center_discharge,
    fact.adt_service,
    fact.bmt_ind
    
from
    fact
    inner join {{source('cdw', 'patient')}} as patient on fact.pat_key = patient.pat_key

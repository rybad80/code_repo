-- purpose: organize medication info & create DOT / LOT metrics for reporting
-- granularity: one row per medication name per medication route per department per day

with abx_all as (
    select
        'MEDICATION' as data_type,
        stg_asp_inpatient_abx_all.visit_key,
        stg_asp_inpatient_abx_all.pat_key,
        stg_asp_inpatient_abx_all.abx_admin_date as cohort_date,
        stg_asp_inpatient_abx_all.abx_admin_datetime,
        stg_asp_inpatient_abx_all.abx_name       as generic_medication_name_clean,
        stg_asp_inpatient_abx_all.abx_route      as medication_route_group,
        stg_asp_inpatient_abx_all.drug_category  as medication_category,
        stg_asp_inpatient_abx_all.drug_class     as medication_class,
        stg_asp_inpatient_abx_all.drug_subclass  as medication_subclass,
        stg_asp_inpatient_abx_all.cdc_drug_ind,
        stg_asp_inpatient_abx_all.last_line_ind,
        stg_asp_inpatient_abx_all.targeted_ind,
        stg_asp_inpatient_abx_all.rule_out_48_hour_ind

    from
        {{ref('stg_asp_inpatient_abx_all')}} as stg_asp_inpatient_abx_all

    where
        stg_asp_inpatient_abx_all.cdc_drug_ind = 1
),

-- keeps only abx admins within IP department stays & provides key for joining to adt
-- a patient may have stayed in an IP department without getting abx there
-- visit_event_key joins results to adt without including extra departments
meds as (
    select
        abx_all.*,
        stg_asp_inpatient_departments.visit_event_key        as visit_event_key,
        stg_asp_inpatient_departments.department_group_name  as med_department,
        stg_asp_inpatient_departments.department_center_abbr as med_department_center

    from
        abx_all
        inner join {{ref('stg_asp_inpatient_departments')}} as stg_asp_inpatient_departments
            on abx_all.visit_key = stg_asp_inpatient_departments.visit_key

    where
        abx_all.abx_admin_datetime between stg_asp_inpatient_departments.enter_date 
            and coalesce(stg_asp_inpatient_departments.exit_date, current_date)
),

-- consolidate meds into one row per medication name per medication route per department per day
results as (
    select
        meds.data_type,
        meds.visit_key,
        meds.visit_event_key,
        meds.pat_key,
        meds.cohort_date,
        meds.generic_medication_name_clean,
        meds.medication_route_group,
        meds.medication_category,
        meds.medication_class,
        meds.medication_subclass,
        meds.med_department,
        meds.med_department_center,
        meds.last_line_ind,
        meds.targeted_ind,
        meds.rule_out_48_hour_ind

    from
        meds

    group by
        meds.data_type,
        meds.visit_key,
        meds.visit_event_key,
        meds.pat_key,
        meds.cohort_date,
        meds.generic_medication_name_clean,
        meds.medication_route_group,
        meds.medication_category,
        meds.medication_class,
        meds.medication_subclass,
        meds.med_department,
        meds.med_department_center,
        meds.last_line_ind,
        meds.targeted_ind,
        meds.rule_out_48_hour_ind
),

fact as (
    select
        results.data_type,
        results.visit_key,
        results.visit_event_key,
        results.pat_key,
        stg_asp_inpatient_cohort.patient_status,
        stg_asp_inpatient_cohort.patient_age_category,
        results.cohort_date,
        -- used to calculate admissions on antimicrobials
        results.medication_route_group,
        initcap(translate(lower(results.generic_medication_name_clean), '-', ' '))
            as generic_medication_name_group,
        results.medication_category,
        results.medication_class,
        results.medication_subclass,
        -- unique identifier of patient, location, date, abx, admin route
        {{
            dbt_utils.surrogate_key([
                'results.visit_key',
                'results.visit_event_key',
                'results.cohort_date',
                'generic_medication_name_group',
                'medication_route_group'
            ])
        }} as mar_visit_event_date_drug_route_key,
        -- used to calculate DOT
        {{
            dbt_utils.surrogate_key([
                'results.visit_key',
                'results.cohort_date',
                'generic_medication_name_group'
            ])
        }} as days_of_therapy_key,
        -- used to calculate LOT
        {{
            dbt_utils.surrogate_key([
                'results.visit_key',
                'results.cohort_date'
            ])
        }} as length_of_therapy_key,
        results.med_department as adt_department,
        results.med_department_center as adt_department_center,
        stg_asp_inpatient_cohort.adt_department_center_admit,
        stg_asp_inpatient_cohort.adt_department_center_discharge,
        stg_asp_inpatient_adt.adt_service,
        stg_asp_inpatient_adt.bmt_ind,
        results.last_line_ind,
        results.targeted_ind,
        results.rule_out_48_hour_ind

    from
        {{ref('stg_asp_inpatient_cohort')}} as stg_asp_inpatient_cohort
        inner join results
            on stg_asp_inpatient_cohort.visit_key = results.visit_key
        -- join to have location/service of medication administration
        inner join {{ref('stg_asp_inpatient_adt')}} as stg_asp_inpatient_adt
            on results.visit_event_key = stg_asp_inpatient_adt.visit_event_key
                and results.cohort_date = stg_asp_inpatient_adt.cohort_date

    where
        (   -- clean-up derived data from medication table, neomycin only included if not enteric
            results.generic_medication_name_clean != 'Neomycin'
            or results.medication_route_group != 'Oral/Enteric'
        )
        and results.cohort_date >= date('2013-07-01')
)

select
    fact.data_type,
    fact.visit_key,
    fact.visit_event_key,
    fact.pat_key,
    fact.patient_status,
    fact.patient_age_category,
    patient.pat_mrn_id as mrn,
    patient.full_nm as full_name,
    fact.cohort_date,
    fact.medication_route_group,
    fact.generic_medication_name_group,
    fact.medication_category,
    fact.medication_class,
    fact.medication_subclass,
    fact.mar_visit_event_date_drug_route_key,
    fact.days_of_therapy_key,
    fact.length_of_therapy_key,
    fact.adt_department,
    fact.adt_department_center,
    fact.adt_department_center_admit,
    fact.adt_department_center_discharge,
    fact.adt_service,
    fact.bmt_ind,
    fact.last_line_ind,
    fact.targeted_ind,
    fact.rule_out_48_hour_ind
from
    fact
    inner join {{source('cdw', 'patient')}} as patient on fact.pat_key = patient.pat_key

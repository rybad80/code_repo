{{ config(meta = {
    'critical': true
}) }}

with dx_status as (
    select
        stg_outbreak_covid_cohort.visit_key,
        max(case when stg_dx_status.covid_resolved_noted_date is not null
            then 1 else 0 end) as covid_resolved_ind,
        max(stg_dx_status.covid_resolved_noted_date)
        as covid_resolved_noted_date
    from {{ref('stg_outbreak_covid_cohort')}} as stg_outbreak_covid_cohort
    inner join {{ref('encounter_all')}} as encounter_all on
        stg_outbreak_covid_cohort.visit_key = encounter_all.visit_key
    inner join {{ref('stg_outbreak_covid_patient_encounter_dx_status')}}
        as stg_dx_status on
        stg_outbreak_covid_cohort.pat_key = stg_dx_status.pat_key
    where case
            when encounter_all.inpatient_ind = 1
                then coalesce(encounter_all.hospital_discharge_date,
                current_date)
            else  encounter_all.encounter_date end
        >= coalesce(stg_dx_status.covid_resolved_noted_date,
        encounter_all.encounter_date, encounter_all.dob)
    group by stg_outbreak_covid_cohort.visit_key
)

select
    stg_outbreak_covid_cohort.proc_ord_key,
    stg_outbreak_covid_cohort.result_seq_num,
    stg_outbreak_covid_cohort.patient_name,
    stg_outbreak_covid_cohort.last_nm,
    stg_outbreak_covid_cohort.first_nm,
    stg_outbreak_covid_cohort.mrn,
    stg_outbreak_covid_cohort.csn,
    stg_outbreak_covid_cohort.emp_tbl_link_ind,
    stg_outbreak_covid_cohort.pat_ind,
    stg_outbreak_covid_cohort.age_years,
    stg_outbreak_covid_cohort.patient_address_zip_code,
    stg_outbreak_covid_cohort.county,
    stg_outbreak_covid_cohort.race,
    stg_outbreak_covid_cohort.ethnicity,
    stg_outbreak_covid_cohort.race_ethnicity,
    stg_outbreak_covid_cohort.payor_group,
    stg_outbreak_covid_cohort.procedure_id,
    stg_outbreak_covid_cohort.procedure_order_id,
    stg_outbreak_covid_cohort.procedure_name,
    coalesce(
        stg_outbreak_covid_order_indication.new_order_indication,
        stg_outbreak_covid_cohort.order_indication,
        'No Indication Available'
    ) as order_indication,
    stg_outbreak_covid_cohort.department_name,
    stg_outbreak_covid_cohort.encounter_provider,
    stg_outbreak_covid_cohort.encounter_type,
    stg_outbreak_covid_cohort.sex,
    stg_outbreak_covid_cohort.patient_class,
    stg_outbreak_covid_cohort.visit_dept,
    stg_outbreak_covid_cohort.placed_date,
    stg_outbreak_covid_cohort.specimen_taken_date,
    stg_outbreak_covid_cohort.current_status,
    stg_outbreak_covid_cohort.result_desc,
    stg_outbreak_covid_cohort.abnormal_result_ind,
    stg_outbreak_covid_cohort.result_value,
    stg_outbreak_covid_cohort.result_date,
    stg_outbreak_covid_cohort.drive_thru_ind,
    stg_outbreak_covid_cohort.roberts_drive_thru_ind,
    stg_outbreak_covid_cohort.bucks_drive_thru_ind,
    stg_outbreak_covid_cohort.false_positive_manual_review_ind,
    stg_outbreak_covid_cohort.pat_key,
    stg_outbreak_covid_cohort.visit_key,
    coalesce(dx_status.covid_resolved_ind, 0) as covid_resolved_ind,
    case
        when stg_outbreak_covid_cohort.current_status = 3
            and coalesce(dx_status.covid_resolved_ind, 0) = 0
            and stg_outbreak_covid_cohort.false_positive_manual_review_ind = 0
        then 1 else 0
    end as covid_active_ind,
  dx_status.covid_resolved_noted_date
from
    {{ref('stg_outbreak_covid_cohort')}} as stg_outbreak_covid_cohort
    left join dx_status on
      stg_outbreak_covid_cohort.visit_key = dx_status.visit_key
    left join {{ref('stg_outbreak_covid_order_indication')}} as stg_outbreak_covid_order_indication
        on stg_outbreak_covid_cohort.proc_ord_key = stg_outbreak_covid_order_indication.proc_ord_key
where
   lower(pat_ind) in ('chop patient', 'likely chop patient')

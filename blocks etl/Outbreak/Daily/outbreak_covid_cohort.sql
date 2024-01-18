{{ config(meta = {
    'critical': true
}) }}

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
    stg_outbreak_covid_cohort.intended_use_name,
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
    stg_outbreak_covid_cohort.visit_key
from
    {{ref('stg_outbreak_covid_cohort')}} as stg_outbreak_covid_cohort
    left join {{ref('stg_outbreak_covid_order_indication')}} as stg_outbreak_covid_order_indication
        on stg_outbreak_covid_cohort.proc_ord_key = stg_outbreak_covid_order_indication.proc_ord_key

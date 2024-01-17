{{ config(meta = {
    'critical': true
}) }}

with pat_visits as (
    select stg_outbreak_covid_lab_results.pat_key,
        count(distinct visit.visit_key) as visit_count
    from
        {{ref('stg_outbreak_covid_lab_results')}} as stg_outbreak_covid_lab_results
        left join {{source('cdw', 'visit')}} as visit on visit.pat_key = stg_outbreak_covid_lab_results.pat_key
        group by
            stg_outbreak_covid_lab_results.pat_key
),

pat_status as (
    select distinct
        patient_staff_note.pat_key,
        bpa_locator_trig_nm,
        stg_patient.mrn
    from
        {{source('cdw', 'patient_staff_note')}} as patient_staff_note
        inner join {{source('cdw', 'dim_bpa_locator_trigger')}} as dim_bpa_locator_trigger
            on dim_bpa_locator_trigger.dim_bpa_locator_trig_key = patient_staff_note.dim_bpa_locator_trig_key
        inner join {{ref('stg_patient')}} as stg_patient on stg_patient.pat_key = patient_staff_note.pat_key
    where
        upper(bpa_locator_trig_nm) like '%DOH%'
        or upper(bpa_locator_trig_nm) like '%EMPLOYEE%'
),

unionset as (
    /* region uses unions to join lab results to employee names, using various name fields
    to identify patient/employee */
    select
        stg_outbreak_covid_lab_results.pat_key,
        employee.last_nm,
        employee.first_nm,
        employee.full_nm,
        job_family_grp_nm,
        job_family_nm,
        job_title_display,
        ad_login,
        worker_type,
        email,
        active_ind,
        upd_by,
        employee.emp_id,
        csn,
        enterprise_worker_latest_ind
    from
        {{ref('stg_outbreak_covid_lab_results')}} as stg_outbreak_covid_lab_results
        inner join {{source('cdw', 'employee')}} as employee
            on lower(regexp_replace(employee.full_nm, '['' ]', '')  ) --noqa: PRS
                = lower( regexp_replace(stg_outbreak_covid_lab_results.patient_name, '['' ]', '')  ) --noqa: PRS
    where
        stg_outbreak_covid_lab_results.age_years > 18.0
    union all
    select
        stg_outbreak_covid_lab_results.pat_key,
        employee.last_nm,
        employee.first_nm,
        employee.full_nm,
        job_family_grp_nm,
        job_family_nm,
        job_title_display,
        ad_login,
        worker_type,
        email,
        active_ind,
        upd_by,
        employee.emp_id,
        csn,
        enterprise_worker_latest_ind
    from
        {{ref('stg_outbreak_covid_lab_results')}} as stg_outbreak_covid_lab_results
        inner join {{source('cdw', 'employee')}} as employee
            on lower(regexp_replace( employee.legal_reporting_nm, '['' ]', '')) --noqa: PRS
                = lower(regexp_replace(stg_outbreak_covid_lab_results.patient_name, '['' ]', '') ) --noqa: PRS
    where
        stg_outbreak_covid_lab_results.age_years > 18.0
    union all
    select
            stg_outbreak_covid_lab_results.pat_key,
            employee.last_nm,
            employee.first_nm,
            employee.full_nm,
            job_family_grp_nm,
            job_family_nm,
            job_title_display,
            ad_login,
            worker_type,
            email,
            active_ind,
            upd_by,
            employee.emp_id,
            csn,
            enterprise_worker_latest_ind
        from
            {{ref('stg_outbreak_covid_lab_results')}} as stg_outbreak_covid_lab_results
        inner join {{source('cdw', 'employee')}} as employee
            on (
                lower(
                    regexp_replace(employee.first_nm, '['' ]', '')
                    ) = lower(regexp_replace(stg_outbreak_covid_lab_results.first_nm, '['' ]', ''))
                 --noqa: PRS
                and lower(
                     regexp_replace(employee.last_nm, '['' ]', '')
                 ) = lower(regexp_replace(stg_outbreak_covid_lab_results.last_nm, '['' ]', ''))
             )
        where
            stg_outbreak_covid_lab_results.age_years > 18.0
),

emp_tbl_temp as (
    select
        pat_key,
        last_nm,
        first_nm,
        full_nm,
        job_family_grp_nm,
        job_family_nm,
        job_title_display,
        ad_login,
        worker_type,
        email,
        active_ind,
        upd_by,
        emp_id,
        rank() over (
            partition by csn order by enterprise_worker_latest_ind desc, active_ind desc
        ) as ord
    from
        unionset
),

emp_tbl as (
    select
        *
    from
        emp_tbl_temp
    where
        ord = 1
    group by
        pat_key,
        last_nm,
        first_nm,
        full_nm,
        job_family_grp_nm,
        job_family_nm,
        job_title_display,
        ad_login,
        worker_type,
        email,
        active_ind,
        upd_by,
        emp_id,
        ord
)

select distinct
    stg_outbreak_covid_lab_results.proc_ord_key,
    stg_outbreak_covid_lab_results.result_seq_num,
    stg_outbreak_covid_lab_results.patient_name,
    upper(stg_outbreak_covid_lab_results.last_nm) as last_nm,
    upper(stg_outbreak_covid_lab_results.first_nm) as first_nm,
    stg_outbreak_covid_lab_results.mrn,
    stg_outbreak_covid_lab_results.csn,
    case when emp_tbl.pat_key is not null
      then 1
      else 0
    end as emp_tbl_link_ind,
    case
      when (
        pat_status.bpa_locator_trig_nm = 'EMPLOYEE'
        or stg_outbreak_covid_lab_results.occ_health_acct_ind = 1
        or emp_tbl_link_ind = 1)
      then 'EMPLOYEE'
      when stg_outbreak_covid_lab_results.age_years > '18'
          and pat_visits.visit_count < 5
      then 'ADULT (Possible Employee)' /* classify cases when no redcap mrn is present */
      when (stg_outbreak_covid_lab_results.department_name not in ('CHOP EMPLOYEE HEALTH', 'MAIN CLINICAL LAB')
        and stg_outbreak_covid_lab_results.drive_thru_ind != 1) then 'CHOP PATIENT'
      else 'LIKELY CHOP PATIENT'
    end as pat_ind,
    stg_outbreak_covid_lab_results.age_years,
    stg_outbreak_covid_lab_results.patient_address_zip_code,
    stg_outbreak_covid_lab_results.county,
    stg_outbreak_covid_lab_results.race,
    stg_outbreak_covid_lab_results.ethnicity,
    stg_outbreak_covid_lab_results.race_ethnicity,
    stg_outbreak_covid_lab_results.payor_group,
    stg_outbreak_covid_lab_results.procedure_id,
    stg_outbreak_covid_lab_results.procedure_order_id,
    stg_outbreak_covid_lab_results.procedure_name,
    stg_outbreak_covid_lab_results.order_indication,
    stg_outbreak_covid_lab_results.department_name,
    stg_outbreak_covid_lab_results.encounter_provider,
    stg_outbreak_covid_lab_results.encounter_type,
    stg_outbreak_covid_lab_results.sex,
    stg_outbreak_covid_lab_results.patient_class,
    stg_outbreak_covid_lab_results.visit_dept,
    stg_outbreak_covid_lab_results.intended_use_name,
    stg_outbreak_covid_lab_results.placed_date,
    stg_outbreak_covid_lab_results.specimen_taken_date,
    stg_outbreak_covid_lab_results.current_status,
    case
      when current_status = 0 then 'Invalid/Inconclusive'
      when current_status = 1 then 'Pending'
      when current_status = 2 then 'Negative'
      when current_status = 3 then 'Positive'
          end as result_desc,
    stg_outbreak_covid_lab_results.abnormal_result_ind,
    stg_outbreak_covid_lab_results.result_value,
    stg_outbreak_covid_lab_results.result_date,
    stg_outbreak_covid_lab_results.drive_thru_ind,
    stg_outbreak_covid_lab_results.roberts_drive_thru_ind,
    stg_outbreak_covid_lab_results.bucks_drive_thru_ind,
    stg_outbreak_covid_lab_results.false_positive_manual_review_ind,
    stg_outbreak_covid_lab_results.pat_key,
    stg_outbreak_covid_lab_results.visit_key
from
    {{ref('stg_outbreak_covid_lab_results')}} as stg_outbreak_covid_lab_results
    left join pat_status
        on pat_status.pat_key = stg_outbreak_covid_lab_results.pat_key
    left join pat_visits
        on pat_visits.pat_key = stg_outbreak_covid_lab_results.pat_key
    left join emp_tbl
        on emp_tbl.pat_key = stg_outbreak_covid_lab_results.pat_key

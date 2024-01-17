{{ config(materialized='incremental')}}
select
    flowsheet_measure.fs_rec_key,
    flowsheet_measure.seq_num,
    flowsheet_measure.occurance,
    coalesce(anesthesia_encounter_link.visit_key, visit.visit_key) as visit_key,
    stg_encounter.encounter_key,
    anesthesia_encounter_link.anes_visit_key,
    stg_encounter.patient_name,
    stg_encounter.mrn,
    stg_encounter.dob,
    stg_encounter.encounter_date,
    stg_encounter.csn,
    visit.hosp_admit_dt as hospital_admit_date,
    flowsheet.disp_nm as flowsheet_name,
    flowsheet.fs_nm as flowsheet_title,
    flowsheet.fs_id as flowsheet_id,
    flowsheet_record.fs_rec_id as flowsheet_record_id,
    flowsheet_measure.meas_val,
    flowsheet_measure.meas_val_num,
    flowsheet_measure.meas_cmt,
    flowsheet_measure.rec_dt as recorded_date,
    flowsheet_measure.entry_dt as entry_date,
    documented_by_employee.full_nm as documented_by_employee,
    taken_by_employee.full_nm as taken_by_employee,
    documented_by_employee.emp_key as documented_by_employee_key,
    taken_by_employee.emp_key as taken_by_employee_key,
    visit_stay_info.vsi_key,
    flowsheet.fs_key,
    visit.pat_key,
    stg_encounter.patient_key,
    flowsheet_measure.upd_dt as block_last_update_date
from
    {{source('cdw', 'visit')}} as visit
    inner join {{source('cdw', 'visit_stay_info')}} as visit_stay_info
        on visit_stay_info.visit_key = visit.visit_key
    inner join {{ref('stg_encounter')}} as stg_encounter
        on stg_encounter.visit_key = visit.visit_key
    left join {{source('cdw', 'anesthesia_encounter_link')}} as anesthesia_encounter_link
        on anesthesia_encounter_link.anes_visit_key = visit.visit_key
    inner join {{source('cdw', 'flowsheet_record')}} as flowsheet_record
        on flowsheet_record.vsi_key = visit_stay_info.vsi_key
    inner join {{source('cdw', 'flowsheet_measure')}} as flowsheet_measure
        on flowsheet_measure.fs_rec_key = flowsheet_record.fs_rec_key
    inner join {{source('cdw', 'flowsheet')}} as flowsheet
        on flowsheet.fs_key = flowsheet_measure.fs_key
    inner join {{source('cdw', 'employee')}} as documented_by_employee
        on documented_by_employee.emp_key = flowsheet_measure.entry_emp_key
    inner join {{source('cdw', 'employee')}} as taken_by_employee
        on taken_by_employee.emp_key = flowsheet_measure.taken_emp_key
where
    visit.visit_key > 0
    and {{ limit_dates_for_dev(ref_date = 'stg_encounter.encounter_date') }}
{%- if is_incremental() %}
    and flowsheet_measure.upd_dt > (select max(block_last_update_date) from {{ this }})
{%- endif %}

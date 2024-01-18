-- purpose: union legacy theradoc data with epic bugsy data as base of IPC reporting
-- granularity: one row per infection event (inf_surv_id)

with theradoc as (
    select
        {{
            dbt_utils.surrogate_key([
            'c54_td_ica_surv_id'
            ])
        }} as inf_surv_key,
        patient.pat_key,
        min(case
            when td_custom_infect_vw.c07_attributed_location is not null and department.dept_abbr is not null then department.dept_key
            when td_custom_infect_vw.c07_attributed_location is not null and department.dept_abbr is null then -1
            when td_custom_infect_vw.c07_attributed_location is null then 0
        end) as dept_key,
        lpad(ltrim(rtrim(td_custom_infect_vw.c02_mrn)), 8, '0') as c02_mrn,
        td_custom_infect_vw.c05_infection_date,
        td_custom_infect_vw.c07_attributed_location,
        td_custom_infect_vw.c09_nhsn_export_location_code,
        td_custom_infect_vw.c10_assigned_to_icp,
        td_custom_infect_vw.c11_confirmation_date,
        upper(td_custom_infect_vw.c12_infection_acquisition_type) as c12_infection_acquisition_type,
        td_custom_infect_vw.c13_infection_onset,
        case
            when td_custom_infect_vw.c14_present_on_admission = 'Yes' then 1
            when td_custom_infect_vw.c14_present_on_admission = 'No' then 0
            when td_custom_infect_vw.c14_present_on_admission is null then -2
        end as c14_present_on_admission,
        max(case
            when td_custom_infect_vw.c38_outpatient = 1 then 1
            when td_custom_infect_vw.c38_outpatient = 0 then 0
            when td_custom_infect_vw.c38_outpatient is null then -2
        end) as c38_outpatient,
        max(case
            when td_custom_infect_vw.c39_emergency = 1 then 1
            when td_custom_infect_vw.c39_emergency = 0 then 0
            when td_custom_infect_vw.c39_emergency is null then -2
        end) as c39_emergency,
        max(case
            when td_custom_infect_vw.c40_general_anesthesia = 1 then 1
            when td_custom_infect_vw.c40_general_anesthesia = 0 then 0
            when td_custom_infect_vw.c40_general_anesthesia is null then -2
        end) as c40_general_anesthesia,
        max(case
            when td_custom_infect_vw.c41_trauma = 1 then 1
            when td_custom_infect_vw.c41_trauma = 0 then 0
            when td_custom_infect_vw.c41_trauma is null then -2
        end) as c41_trauma,
        max(case
            when td_custom_infect_vw.c42_endoscope = 1 then 1
            when td_custom_infect_vw.c42_endoscope = 0 then 0
            when td_custom_infect_vw.c42_endoscope is null then -2
        end) as c42_endoscope,
        max(case
            when td_custom_infect_vw.c43_transplant = 1 then 1
            when td_custom_infect_vw.c43_transplant = 0 then 0
            when td_custom_infect_vw.c43_transplant is null then -2
        end) as c43_transplant,
        td_custom_infect_vw.c54_td_ica_surv_id,
        upper(td_custom_infect_vw.c57_work_list_status) as c57_work_list_status,
        td_custom_infect_vw.c58_ica_surv_type,
        current_date as create_dt,
        'THERADOC' as create_by,
        current_date as upd_dt,
        'THERADOC' as upd_by

    from
        {{source('clarity_ods', 'td_custom_infect_vw')}} as td_custom_infect_vw
    inner join {{source('cdw', 'patient')}} as patient
        on lpad(ltrim(rtrim(td_custom_infect_vw.c02_mrn)), 8, '0') = patient.pat_mrn_id
    left join {{source('cdw', 'department')}} as department
        on td_custom_infect_vw.c07_attributed_location = department.dept_abbr

    group by
        patient.pat_key,
        department.dept_abbr,
        td_custom_infect_vw.c02_mrn,
        td_custom_infect_vw.c05_infection_date,
        td_custom_infect_vw.c07_attributed_location,
        td_custom_infect_vw.c09_nhsn_export_location_code,
        td_custom_infect_vw.c10_assigned_to_icp,
        td_custom_infect_vw.c11_confirmation_date,
        td_custom_infect_vw.c12_infection_acquisition_type,
        td_custom_infect_vw.c13_infection_onset,
        td_custom_infect_vw.c14_present_on_admission,
        td_custom_infect_vw.c54_td_ica_surv_id,
        td_custom_infect_vw.c57_work_list_status,
        td_custom_infect_vw.c58_ica_surv_type
),

bugsy as (
    select
        {{
            dbt_utils.surrogate_key([
            'c54_td_ica_surv_id'
            ])
        }} as inf_surv_key,
        patient.pat_key,
        min(case
            when bugsy_custom_infect_vw.c07_attributed_location is not null
                and department.dept_abbr is not null then department.dept_key
            when bugsy_custom_infect_vw.c07_attributed_location is not null
                and department.dept_abbr is null then -1
            when bugsy_custom_infect_vw.c07_attributed_location is null then 0
        end) as dept_key,
        lpad(ltrim(rtrim(bugsy_custom_infect_vw.c02_mrn)), 8, '0') as c02_mrn,
        bugsy_custom_infect_vw.c05_infection_date,
        bugsy_custom_infect_vw.c07_attributed_location,
        bugsy_custom_infect_vw.c09_nhsn_export_location_code,
        bugsy_custom_infect_vw.c10_assigned_to_icp,
        bugsy_custom_infect_vw.c11_confirmation_date,
        upper(bugsy_custom_infect_vw.c12_infection_acquisition_type) as c12_infection_acquisition_type,
        bugsy_custom_infect_vw.c13_infection_onset,
        case
            when bugsy_custom_infect_vw.c14_present_on_admission = 'Yes' then 1
            when bugsy_custom_infect_vw.c14_present_on_admission = 'No' then 0
            when bugsy_custom_infect_vw.c14_present_on_admission is null then -2
        end as c14_present_on_admission,
        max(case
            when bugsy_custom_infect_vw.c38_outpatient = 1 then 1
            when bugsy_custom_infect_vw.c38_outpatient = 0 then 0
            when bugsy_custom_infect_vw.c38_outpatient is null then -2
        end) as c38_outpatient,
        max(case
            when bugsy_custom_infect_vw.c39_emergency = 1 then 1
            when bugsy_custom_infect_vw.c39_emergency = 0 then 0
            when bugsy_custom_infect_vw.c39_emergency is null then -2
        end) as c39_emergency,
        max(case
            when bugsy_custom_infect_vw.c40_general_anesthesia = 1 then 1
            when bugsy_custom_infect_vw.c40_general_anesthesia = 0 then 0
            when bugsy_custom_infect_vw.c40_general_anesthesia is null then -2
        end) as c40_general_anesthesia,
        max(case
            when bugsy_custom_infect_vw.c41_trauma = 1 then 1
            when bugsy_custom_infect_vw.c41_trauma = 0 then 0
            when bugsy_custom_infect_vw.c41_trauma is null then -2
        end) as c41_trauma,
        max(case
            when bugsy_custom_infect_vw.c42_endoscope = 1 then 1
            when bugsy_custom_infect_vw.c42_endoscope = 0 then 0
            when bugsy_custom_infect_vw.c42_endoscope is null then -2
        end) as c42_endoscope,
        max(case
            when bugsy_custom_infect_vw.c43_transplant = 1 then 1
            when bugsy_custom_infect_vw.c43_transplant = 0 then 0
            when bugsy_custom_infect_vw.c43_transplant is null then -2
        end) as c43_transplant,
        bugsy_custom_infect_vw.c54_td_ica_surv_id,
        upper(bugsy_custom_infect_vw.c57_work_list_status) as c57_work_list_status,
        bugsy_custom_infect_vw.c58_ica_surv_type,
        current_date as create_dt,
        'BUGSY' as create_by,
        current_date as upd_dt,
        'BUGSY' as upd_by

    from
        {{ref('bugsy_custom_infect_vw')}} as bugsy_custom_infect_vw
    inner join {{source('cdw', 'patient')}} as patient
        on lpad(ltrim(rtrim(bugsy_custom_infect_vw.c02_mrn)), 8, '0') = patient.pat_mrn_id
    left join {{source('cdw', 'department')}} as department
        on bugsy_custom_infect_vw.c07_attributed_location = department.dept_abbr

    group by
        patient.pat_key,
        department.dept_abbr,
        bugsy_custom_infect_vw.c02_mrn,
        bugsy_custom_infect_vw.c05_infection_date,
        bugsy_custom_infect_vw.c07_attributed_location,
        bugsy_custom_infect_vw.c09_nhsn_export_location_code,
        bugsy_custom_infect_vw.c10_assigned_to_icp,
        bugsy_custom_infect_vw.c11_confirmation_date,
        bugsy_custom_infect_vw.c12_infection_acquisition_type,
        bugsy_custom_infect_vw.c13_infection_onset,
        bugsy_custom_infect_vw.c14_present_on_admission,
        bugsy_custom_infect_vw.c54_td_ica_surv_id,
        bugsy_custom_infect_vw.c57_work_list_status,
        bugsy_custom_infect_vw.c58_ica_surv_type
)

select
    inf_surv_key,
    pat_key,
    dept_key,
    c54_td_ica_surv_id as inf_surv_id,
    c09_nhsn_export_location_code::varchar(50) as nhsn_exp_loc_cd,
    c12_infection_acquisition_type::varchar(100) as inf_acq_type,
    c13_infection_onset::varchar(100) as inf_onset,
    c10_assigned_to_icp::varchar(100) as assigned_to_icp,
    c05_infection_date as inf_dt,
    c11_confirmation_date as conf_dt,
    c58_ica_surv_type::varchar(50) as surv_type,
    c57_work_list_status::varchar(50) as work_status,
    c14_present_on_admission::bigint as pres_on_admit_ind,
    c38_outpatient::bigint as op_ind,
    c39_emergency::bigint as ed_ind,
    c40_general_anesthesia::bigint as anes_ind,
    c41_trauma::bigint as trauma_ind,
    c42_endoscope::bigint as endoscope_ind,
    c43_transplant::bigint as transplant_ind,
    create_dt::timestamp as create_dt,
    create_by::varchar(20) as create_by,
    upd_dt::timestamp as upd_dt,
    upd_by::varchar(20) as upd_by

from
    theradoc

union all

select
    inf_surv_key,
    pat_key,
    dept_key,
    c54_td_ica_surv_id as inf_surv_id,
    c09_nhsn_export_location_code::varchar(50) as nhsn_exp_loc_cd,
    c12_infection_acquisition_type::varchar(100) as inf_acq_type,
    c13_infection_onset::varchar(100) as inf_onset,
    c10_assigned_to_icp::varchar(100) as assigned_to_icp,
    c05_infection_date as inf_dt,
    c11_confirmation_date as conf_dt,
    c58_ica_surv_type::varchar(50) as surv_type,
    c57_work_list_status::varchar(50) as work_status,
    c14_present_on_admission::bigint as pres_on_admit_ind,
    c38_outpatient::bigint as op_ind,
    c39_emergency::bigint as ed_ind,
    c40_general_anesthesia::bigint as anes_ind,
    c41_trauma::bigint as trauma_ind,
    c42_endoscope::bigint as endoscope_ind,
    c43_transplant::bigint as transplant_ind,
    create_dt::timestamp as create_dt,
    create_by::varchar(20) as create_by,
    upd_dt::timestamp as upd_dt,
    upd_by::varchar(20) as upd_by

from
    bugsy

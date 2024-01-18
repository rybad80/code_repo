with
in_progress_lang as (

select
    cl_qanswer_qnr_progress.form_answer_id,
    cl_qanswer_qnr_progress.compld_form_locale as language_locale
from
    {{source('clarity_ods', 'cl_qanswer_qnr_progress')}} as cl_qanswer_qnr_progress
where
    cl_qanswer_qnr_progress.compld_form_locale is not null
group by
    cl_qanswer_qnr_progress.form_answer_id,
    cl_qanswer_qnr_progress.compld_form_locale
),

submitted_qnr as (

select
    stg_encounter_form_answer.pat_enc_csn_id,
    stg_encounter_form_answer.form_id,
    stg_encounter_form_answer.question_answer_id,
    stg_encounter_form_answer.survey_status as submitted_qnr_status,
    stg_encounter_form_answer.answer_change_date as submitted_qnr_date,
    employee.full_nm as submitted_qnr_user,
    employee.emp_key as submitted_qnr_user_emp_key,
    case when upper(employee.full_nm) like '%MYCHART%' then 'MyCHOP' 
         when upper(employee.full_nm) like '%KIOSK%' then 'Kiosk/Welcome'
         when employee.ad_login is not null then 'Epic Hyperspace'
        else 'Other'
    end as submission_method,
    in_progress_lang.language_locale
from
    {{ref('stg_encounter_form_answer')}} as stg_encounter_form_answer
    inner join {{source('cdw', 'employee')}} as employee
        on employee.emp_id = stg_encounter_form_answer.stat_change_emp_id
        and employee.upd_by = 'CLARITY'
    left join in_progress_lang on in_progress_lang.form_answer_id = stg_encounter_form_answer.question_answer_id
group by
    stg_encounter_form_answer.pat_enc_csn_id,
    stg_encounter_form_answer.form_id,
    stg_encounter_form_answer.question_answer_id,
    stg_encounter_form_answer.survey_status,
    stg_encounter_form_answer.answer_change_date,
    employee.full_nm,
    employee.emp_key,
    employee.ad_login,
    in_progress_lang.language_locale
),

appt_qnr_assigned as (

select
    visit.visit_key,
    visit.enc_id,
    visit.pat_key, 
    myc_appt_quesr_id as form_id,
    myc_quesr_start_dt as assigned_qnr_start_date,
    zc_pat_appt_qnr_src.name as assigned_qnr_source,
    zc_pat_appt_qnr_stat.name as assigned_qnr_status,
    null as series_enable_date,
    null as series_nm,
    null as series_id,
    null as series_assign_key,
    null as question_answer_id
from
    {{source('clarity_ods', 'myc_appt_qnr_data')}} as myc_appt_qnr_data
    inner join {{source('cdw', 'visit')}} as visit 
        on visit.enc_id = myc_appt_qnr_data.pat_enc_csn_id
    inner join {{source('clarity_ods', 'zc_pat_appt_qnr_stat')}} as zc_pat_appt_qnr_stat
        on zc_pat_appt_qnr_stat.pat_appt_qnr_stat_c = myc_appt_qnr_data.pat_appt_qnr_stat_c
    inner join {{source('clarity_ods', 'zc_pat_appt_qnr_src')}} as zc_pat_appt_qnr_src
        on zc_pat_appt_qnr_src.pat_appt_qnr_src_c = myc_appt_qnr_data.pat_appt_qnr_src_c
),

series_source as (

select
    series_log_info.series_ans_id,
    series_log_info.srs_action_date as series_enable_date,
    zc_srs_action.name
from
    {{source('clarity_ods', 'series_log_info')}} as series_log_info
    inner join {{source('clarity_ods', 'zc_srs_action')}} as zc_srs_action
         on zc_srs_action.srs_action_c = series_log_info.srs_action_c
where
   upper(zc_srs_action.name) like '%ENABLE SERIES%'
group by
    series_ans_id,
    series_log_info.srs_action_date,
    zc_srs_action.name
),

series_qnr_assigned as (

 select
     series_answer.visit_key, 
     series_answer.pat_key, 
     series_log_info.srs_act_quesr_id as form_id,
     series_log_info.srs_action_date as assigned_qnr_start_date,
     series_source.name as assigned_qnr_source,
     zc_srs_action.name as assigned_qnr_status,
     series_source.series_enable_date,
     series_info.series_nm,
     series_info.series_id,
     series_log_info.series_ans_id||'-'||series_log_info.line as series_assign_key,
     form_answer.answer_id as question_answer_id
 from
    {{source('cdw', 'series_answer')}} as series_answer 
    inner join {{source('cdw', 'series_info')}} as series_info
        on series_info.series_key = series_answer.series_key
    inner join {{source('clarity_ods', 'series_log_info' )}} as series_log_info
        on series_log_info.series_ans_id = series_answer.series_ansr_id
    inner join {{source('clarity_ods', 'zc_srs_action' )}} as zc_srs_action
        on zc_srs_action.srs_action_c = series_log_info.srs_action_c
    inner join series_source 
        on series_source.series_ans_id = series_log_info.series_ans_id
    left join {{source('clarity_ods', 'series_answer_id' )}} as series_answer_id
        on series_answer_id.series_ans_id = series_log_info.series_ans_id
        and series_answer_id.contact_date = series_log_info.srs_action_date
    left join {{source('cdw', 'form_answer' )}} as form_answer
        on form_answer.answer_id = series_answer_id.answer_id
where
   zc_srs_action.srs_action_c = 40 -- 'Enable Questionnaire'
),

all_assigned as (

select
    appt_qnr_assigned.visit_key,
    appt_qnr_assigned.pat_key,
    appt_qnr_assigned.form_id,
    appt_qnr_assigned.assigned_qnr_start_date,
    appt_qnr_assigned.assigned_qnr_source,
    appt_qnr_assigned.assigned_qnr_status,
    appt_qnr_assigned.series_enable_date,
    appt_qnr_assigned.series_nm,
    appt_qnr_assigned.series_id,
    appt_qnr_assigned.series_assign_key,
    submitted_qnr.question_answer_id,
    submitted_qnr.submitted_qnr_status,
    submitted_qnr.submitted_qnr_date,
    submitted_qnr.submitted_qnr_user,
    submitted_qnr.submission_method,
    submitted_qnr.language_locale,
    submitted_qnr.submitted_qnr_user_emp_key 
from
    appt_qnr_assigned
    left join submitted_qnr 
            on submitted_qnr.pat_enc_csn_id = appt_qnr_assigned.enc_id
            and cast(submitted_qnr.form_id as numeric) = appt_qnr_assigned.form_id

union all

select
    series_qnr_assigned.visit_key,
    series_qnr_assigned.pat_key, 
    series_qnr_assigned.form_id,
    series_qnr_assigned.assigned_qnr_start_date,
    series_qnr_assigned.assigned_qnr_source,
    series_qnr_assigned.assigned_qnr_status,
    series_qnr_assigned.series_enable_date,
    series_qnr_assigned.series_nm,
    series_qnr_assigned.series_id,
    series_qnr_assigned.series_assign_key,
    submitted_qnr.question_answer_id,
    submitted_qnr.submitted_qnr_status,
    submitted_qnr.submitted_qnr_date,
    submitted_qnr.submitted_qnr_user,
    submitted_qnr.submission_method,
    submitted_qnr.language_locale,
    submitted_qnr.submitted_qnr_user_emp_key
from
    series_qnr_assigned
    left join submitted_qnr 
            on submitted_qnr.question_answer_id = series_qnr_assigned.question_answer_id
)

select
    {{dbt_utils.surrogate_key(['all_assigned.series_assign_key','stg_encounter.encounter_key','master_form.form_id'])}}
        as qnr_assigned_key,
    stg_encounter.visit_key,
    case when 
        all_assigned.series_id is null -- Not a Series
        or all_assigned.assigned_qnr_start_date <= stg_encounter.encounter_date 
        then stg_encounter.visit_key
        else {{dbt_utils.surrogate_key(['all_assigned.assigned_qnr_start_date','stg_encounter.encounter_key'])}} 
    end as pro_episode_key,
    stg_patient.mrn,
    stg_patient.patient_name,
    stg_encounter.csn,
    coalesce(stg_encounter.encounter_date, all_assigned.series_enable_date) as encounter_date,
    case when 
        all_assigned.series_id is null-- Not a Series
        or all_assigned.assigned_qnr_start_date <= stg_encounter.encounter_date 
        then stg_encounter.encounter_date
        else all_assigned.assigned_qnr_start_date
    end as pro_episode_date,
    master_form.form_nm,
    master_form.form_id,
    all_assigned.series_nm,
    all_assigned.series_id,
    all_assigned.assigned_qnr_start_date,
    all_assigned.assigned_qnr_source,
    case when all_assigned.assigned_qnr_source = 'Manually Assigned' then 1 else 0 end as manually_assigned_ind,
    all_assigned.assigned_qnr_status,
    case when all_assigned.submitted_qnr_date is not null then 1 else 0 end as submitted_ind,
    all_assigned.submitted_qnr_status,
    all_assigned.submitted_qnr_date,
    all_assigned.submitted_qnr_user,
    all_assigned.submission_method,
    case when all_assigned.submission_method = 'MyCHOP' then 1 else 0 end as submitted_with_mychop_ind,
    all_assigned.language_locale,
    stg_patient.preferred_language,
    stg_encounter.department_name,
    stg_encounter.department_id,
    stg_encounter.visit_type,
    stg_encounter.visit_type_id,
    coalesce(stg_encounter.age_years, ((date(all_assigned.series_enable_date) - date(stg_patient.dob)) / 365.25))
        as age_years,
    stg_patient.pat_key,
    stg_encounter.dept_key,
    all_assigned.submitted_qnr_user_emp_key,
    master_form.form_key
from
    all_assigned
    inner join {{source('cdw', 'master_form')}} as master_form
        on master_form.form_id = all_assigned.form_id
    inner join {{ref('stg_patient')}} as stg_patient 
        on stg_patient.pat_key = all_assigned.pat_key
    left join {{ref('stg_encounter')}} as stg_encounter
       on stg_encounter.visit_key = all_assigned.visit_key

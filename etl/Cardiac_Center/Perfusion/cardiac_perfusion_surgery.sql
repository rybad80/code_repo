with emp1 as (
    select distinct
        stg_cardiac_perfusion_surgery_patients.anes_log_key,
        employee.emp_id,
        employee.full_nm as perfusionist_name,
        employee.title as perfusionist_title,
        employee.ad_login,
        master_event_type.event_id,
        row_number() over (
            partition by stg_cardiac_perfusion_surgery_patients.anes_log_key
            order by visit_ed_event.seq_num
        ) as emp_order
    from
        stg_cardiac_perfusion_surgery_patients
        left join {{source('cdw', 'visit_ed_event')}} as visit_ed_event
            on visit_ed_event.visit_key = stg_cardiac_perfusion_surgery_patients.anes_visit_key
        left join {{source('cdw', 'employee')}} as employee
            on employee.emp_key = visit_ed_event.event_init_emp_key
        left join {{source('cdw', 'master_event_type')}} as master_event_type
            on visit_ed_event.event_type_key = master_event_type.event_type_key
    where
        master_event_type.event_id = 112700001
),
emp2 as (
    select distinct
        stg_cardiac_perfusion_surgery_patients.anes_log_key,
        employee.emp_id,
        employee.full_nm as perfusionist_name,
        employee.title as perfusionist_title,
        employee.ad_login,
        master_event_type.event_id,
        row_number() over (
            partition by stg_cardiac_perfusion_surgery_patients.anes_log_key
            order by visit_ed_event.seq_num
        ) as emp_order
    from
        {{ref('stg_cardiac_perfusion_surgery_patients')}} as stg_cardiac_perfusion_surgery_patients
        left join {{source('cdw', 'visit_ed_event')}} as visit_ed_event
            on visit_ed_event.visit_key = stg_cardiac_perfusion_surgery_patients.anes_visit_key
        left join {{source('cdw', 'employee')}} as employee
            on employee.emp_key = visit_ed_event.event_init_emp_key
        left join {{source('cdw', 'master_event_type')}} as master_event_type
            on visit_ed_event.event_type_key = master_event_type.event_type_key
    where
        master_event_type.event_id = 112700038
),
pat_wt as (
    select
        stg_cardiac_perfusion_surgery_patients.log_key,
        stg_cardiac_perfusion_surgery_vitals.weight_kg,
        row_number() over (
            partition by stg_cardiac_perfusion_surgery_patients.log_key
            order by recorded_date - perf_rec_begin_tm desc
        ) as doc_order
    from
        {{ref('stg_cardiac_perfusion_surgery_vitals')}} as stg_cardiac_perfusion_surgery_vitals
        inner join {{ref('stg_cardiac_perfusion_surgery_patients')}} as stg_cardiac_perfusion_surgery_patients
            on stg_cardiac_perfusion_surgery_patients.pat_key = stg_cardiac_perfusion_surgery_vitals.pat_key
    where
        weight_kg is not null
        and recorded_date <= perf_rec_begin_tm
),
dose_wt as (
    select
        stg_cardiac_perfusion_surgery_patients.log_key,
        cast(meas_val_num / 35.274 as numeric (6, 1)) as dose_weight_kg,
        row_number() over (
            partition by stg_cardiac_perfusion_surgery_patients.log_key
            order by recorded_date - perf_rec_begin_tm desc
        ) as doc_order
    from
        {{ref('flowsheet_all')}} as flowsheet_all
        inner join {{ref('stg_cardiac_perfusion_surgery_patients')}} as stg_cardiac_perfusion_surgery_patients
            on stg_cardiac_perfusion_surgery_patients.pat_key = flowsheet_all.pat_key
    where
        flowsheet_id = 40022107
        and recorded_date <= perf_rec_begin_tm
        and meas_val_num is not null
),
pat_ht as (
    select
        stg_cardiac_perfusion_surgery_patients.log_key,
        height_cm,
        row_number() over (
            partition by stg_cardiac_perfusion_surgery_patients.log_key
            order by recorded_date - perf_rec_begin_tm desc
        ) as doc_order
    from
        {{ref('stg_cardiac_perfusion_surgery_vitals')}} as stg_cardiac_perfusion_surgery_vitals
        inner join {{ref('stg_cardiac_perfusion_surgery_patients')}} as stg_cardiac_perfusion_surgery_patients
            on stg_cardiac_perfusion_surgery_patients.pat_key = stg_cardiac_perfusion_surgery_vitals.pat_key
    where
        height_cm is not null
        and recorded_date <= perf_rec_begin_tm
),
pat_bsa as (
    select
        stg_cardiac_perfusion_surgery_patients.log_key,
        bsa,
        row_number() over (
            partition by stg_cardiac_perfusion_surgery_patients.log_key
            order by recorded_date - perf_rec_begin_tm desc
        ) as doc_order
    from
        {{ref('stg_cardiac_perfusion_surgery_vitals')}} as stg_cardiac_perfusion_surgery_vitals
        inner join {{ref('stg_cardiac_perfusion_surgery_patients')}} as stg_cardiac_perfusion_surgery_patients
            on stg_cardiac_perfusion_surgery_patients.pat_key = stg_cardiac_perfusion_surgery_vitals.pat_key
    where
        bsa is not null
        and recorded_date <= perf_rec_begin_tm
)
select
    stg_cardiac_perfusion_surgery_patients.anes_visit_key,
    stg_cardiac_perfusion_surgery_patients.mrn,
    stg_cardiac_perfusion_surgery_patients.patient_name,
    stg_cardiac_perfusion_surgery_patients.sex,
    stg_cardiac_perfusion_surgery_patients.dob,
    case
        when age(perf_rec_begin_tm, dob) < interval '24 hour' then '< 24 hours'
        else substring(
            cast(age(perf_rec_begin_tm, dob) as varchar(255)),
            1,
            length(cast(age(perf_rec_begin_tm, dob) as varchar(255))
            ) - 9)
    end as current_age,
    coalesce(weight_kg, dose_weight_kg) as weight_kg,
    height_cm,
    round(coalesce(bsa, .024265 * ((weight_kg)^.5378) * ((height_cm)^.3964)), 2) as bsa, --noqa: LXR,PRS,L006,L071
    coalesce(emp1.perfusionist_name, emp2.perfusionist_name) as perfusionist_name,
    coalesce(emp1.ad_login, emp2.ad_login) as perfusionist_email,
    cast(perf_rec_begin_tm as date) as perfusion_date,
    stg_cardiac_perfusion_surgery_patients.cdi_recal_date,
    stg_cardiac_perfusion_surgery_patients.pat_key,
    stg_cardiac_perfusion_surgery_patients.visit_key,
    stg_cardiac_perfusion_surgery_patients.csn,
    stg_cardiac_perfusion_surgery_patients.anes_log_key,
    stg_cardiac_perfusion_surgery_patients.log_key,
    stg_cardiac_perfusion_surgery_patients.log_id,
    stg_cardiac_perfusion_surgery_patients.anes_key,
    stg_cardiac_perfusion_surgery_patients.anes_event_visit_key,
    stg_cardiac_perfusion_surgery_patients.or_case_key,
    stg_cardiac_perfusion_surgery_patients.or_log_visit_key,
    stg_cardiac_perfusion_surgery_patients.proc_visit_key,
    stg_cardiac_perfusion_surgery_patients.anes_vsi_key,
    stg_cardiac_perfusion_surgery_patients.hsp_vai_key
from
    {{ref('stg_cardiac_perfusion_surgery_patients')}} as stg_cardiac_perfusion_surgery_patients
    left join pat_wt
        on pat_wt.log_key = stg_cardiac_perfusion_surgery_patients.log_key
        and pat_wt.doc_order = 1
    left join dose_wt
        on dose_wt.log_key = stg_cardiac_perfusion_surgery_patients.log_key
        and dose_wt.doc_order = 1
    left join pat_ht
        on pat_ht.log_key = stg_cardiac_perfusion_surgery_patients.log_key
        and pat_ht.doc_order = 1
    left join pat_bsa
        on pat_bsa.log_key = stg_cardiac_perfusion_surgery_patients.log_key
        and pat_bsa.doc_order = 1
    left join emp1
        on emp1.anes_log_key = stg_cardiac_perfusion_surgery_patients.anes_log_key
        and emp1.emp_order = 1
    left join emp2
        on emp2.anes_log_key = stg_cardiac_perfusion_surgery_patients.anes_log_key
        and emp2.emp_order = 1

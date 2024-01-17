{{ config(meta = {
    'critical': true
}) }}

with nicu_repiratory_patients as (
    select
        visit_key,
        mrn,
        patient_name,
        hospital_admit_date,
        hospital_discharge_date,
        last_high_fio2_datetime,
        invasive_support_days,
        non_invasive_support_days,
        total_support_days
    from
        {{ ref('neo_nicu_respiratory_summary') }} 
),
chnd_resp as (
    select
        {{
            dbt_utils.surrogate_key([
                'nicu_repiratory_patients.mrn',
                'nicu_repiratory_patients.hospital_admit_date',
                'neo_nicu_respiratory_category.recorded_date'
            ])
        }} as chnd_resp_key,
        nicu_repiratory_patients.visit_key,
        nicu_repiratory_patients.hospital_admit_date,
        nicu_repiratory_patients.mrn,
        nicu_repiratory_patients.patient_name,
        nicu_repiratory_patients.last_high_fio2_datetime,
        nicu_repiratory_patients.invasive_support_days,
        nicu_repiratory_patients.non_invasive_support_days,
        nicu_repiratory_patients.total_support_days,
        neo_nicu_respiratory_history.respiratory_support_category as category,
        neo_nicu_respiratory_history.resp_support_start_datetime as start_time,
        neo_nicu_respiratory_history.resp_support_stop_datetime as stop_time,
        neo_nicu_respiratory_category.recorded_date as recording_time,
        neo_nicu_respiratory_category.resp_o2_device,
        neo_nicu_respiratory_category.mode,
        neo_nicu_respiratory_category.invasive_device,
        neo_nicu_respiratory_category.hfjv_pip_set,
        neo_nicu_respiratory_category.hfov_amplitude_actual,
        neo_nicu_respiratory_category.non_invasive_interface,
        neo_nicu_respiratory_category.o2_flow_rate
    from
        nicu_repiratory_patients
        inner join {{ ref('neo_nicu_respiratory_summary') }} as neo_nicu_respiratory_summary
            on nicu_repiratory_patients.visit_key = neo_nicu_respiratory_summary.visit_key
        inner join {{ ref('neo_nicu_respiratory_history') }} as neo_nicu_respiratory_history
            on neo_nicu_respiratory_history.visit_key = neo_nicu_respiratory_summary.visit_key
        left join {{ ref('neo_nicu_respiratory_category') }} as neo_nicu_respiratory_category
            on neo_nicu_respiratory_category.visit_key = neo_nicu_respiratory_history.visit_key
            and neo_nicu_respiratory_category.recorded_date
            <= neo_nicu_respiratory_history.resp_support_stop_datetime
            and neo_nicu_respiratory_category.recorded_date
            >= neo_nicu_respiratory_history.resp_support_start_datetime
    group by
        nicu_repiratory_patients.visit_key,
        nicu_repiratory_patients.mrn,
        nicu_repiratory_patients.patient_name,
        nicu_repiratory_patients.hospital_admit_date,
        nicu_repiratory_patients.last_high_fio2_datetime,
        nicu_repiratory_patients.invasive_support_days,
        nicu_repiratory_patients.non_invasive_support_days,
        nicu_repiratory_patients.total_support_days,
        neo_nicu_respiratory_history.respiratory_support_category,
        neo_nicu_respiratory_history.resp_support_start_datetime,
        neo_nicu_respiratory_history.resp_support_stop_datetime,
        neo_nicu_respiratory_category.recorded_date,
        neo_nicu_respiratory_category.resp_o2_device,
        neo_nicu_respiratory_category.mode,
        neo_nicu_respiratory_category.invasive_device,
        neo_nicu_respiratory_category.hfjv_pip_set,
        neo_nicu_respiratory_category.hfov_amplitude_actual,
        neo_nicu_respiratory_category.non_invasive_interface,
        neo_nicu_respiratory_category.o2_flow_rate
)
select
    chnd_resp_key,
    visit_key,
    mrn,
    patient_name,
    hospital_admit_date,
    last_high_fio2_datetime,
    invasive_support_days,
    non_invasive_support_days,
    total_support_days,
    category,
    start_time,
    stop_time,
    recording_time,
    resp_o2_device,
    mode,
    invasive_device,
    hfjv_pip_set,
    hfov_amplitude_actual,
    non_invasive_interface,
    o2_flow_rate
from
    chnd_resp

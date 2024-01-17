with event_times as (
    select
        visit_ed_event.visit_key,
        max(case when master_event_type.event_id in (112700038, 112700045) then 1 else null end) as perf_ind,
        max(case when master_event_type.event_id = 112700047 then 1 else null end) as emerg_ind,
        max(case when master_event_type.event_id = 112700048 then 1 else null end) as stby_ind,
        min(
            case when master_event_type.event_id in (112700038, 112700045) then event_dt else null end
        ) as perf_rec_begin_tm,
        max(case when master_event_type.event_id = 1120000001 then event_dt else null end) as case_start_tm,
        max(
            case when master_event_type.event_id in (112700040, 1127000052) then event_dt else null end
        ) as cdi_recal_date,
        max(case when master_event_type.event_id = 1120000002 then event_dt else null end) as anes_stop_tm,
        max(
            case when master_event_type.event_id in (112700039, 112700049) then event_dt else null end
        ) as perf_rec_end_tm,
        max(case when master_event_type.event_id = 1120000046 then event_dt else null end) as handoff_tm
    from
        {{source('cdw', 'visit_ed_event')}} as visit_ed_event
        inner join {{source('cdw', 'master_event_type')}} as master_event_type
            on visit_ed_event.event_type_key = master_event_type.event_type_key
    where
        master_event_type.event_id in (
            112700038,
            112700045,
            112700047,
            112700048,
            112700038,
            112700045,
            1120000001,
            112700001,
            112700040,
            1127000052,
            112700002,
            1120000002,
            112700039,
            112700049,
            1120000046,
            112700007,
            112700008)
        and visit_ed_event.event_stat is null
    group by
        visit_ed_event.visit_key
)
select distinct
    stg_patient.mrn,
    stg_patient.patient_name,
    stg_patient.sex,
    stg_patient.dob,
    stg_patient.pat_key,
    stg_encounter.csn,
    anesthesia_encounter_link.or_log_key as anes_log_key,
    or_log.log_key,
    or_log.log_id,
    anes_key,
    anes_event_visit_key,
    anes_visit_key,
    or_case.or_case_key,
    or_log_visit_key,
    proc_visit_key,
    anesthesia_encounter_link.visit_key,
    visit_stay_info.vsi_key as anes_vsi_key,
    visit_addl_info.vsi_key as hsp_vai_key,
    perf_ind,
    emerg_ind,
    stby_ind,
    perf_rec_begin_tm,
    case_start_tm as anes_start_tm,
    cdi_recal_date,
    perf_rec_end_tm,
    anes_stop_tm,
    handoff_tm,
    cast(anes_stop_tm  + interval '24 hour' as timestamp) as end_unit_use_tm
from
    {{source('cdw', 'or_case')}} as or_case
    inner join {{source('cdw', 'or_log')}} as or_log
        on or_case.log_key = or_log.log_key
    inner join {{source('cdw', 'anesthesia_encounter_link')}} as anesthesia_encounter_link
        on anesthesia_encounter_link.or_case_key = or_case.or_case_key
    inner join {{source('cdw', 'visit')}} as visit
        on visit.visit_key = anesthesia_encounter_link.visit_key
    left join {{source('cdw', 'visit_addl_info')}} as visit_addl_info
        on visit_addl_info.visit_key = anesthesia_encounter_link.visit_key
    left join {{source('cdw', 'visit_stay_info')}} as visit_stay_info
        on visit_stay_info.visit_key = anesthesia_encounter_link.anes_visit_key
    inner join {{ref('stg_patient')}} as stg_patient
        on stg_patient.pat_key = anesthesia_encounter_link.pat_key
    inner join {{ref('stg_encounter')}} as stg_encounter
        on stg_encounter.visit_key = anesthesia_encounter_link.anes_visit_key
    left join event_times
        on event_times.visit_key = anesthesia_encounter_link.anes_visit_key
where
    perf_ind = 1
    and cast(perf_rec_begin_tm as date) >= to_date('4/16/2018', 'MM/DD/YYYY')
    and or_log.log_key > 0

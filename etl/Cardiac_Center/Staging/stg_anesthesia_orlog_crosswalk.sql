with anes_timestamps  as (
select
       aes.anes_id as anesthesia_id,
       stg_encounter.csn as anesthesia_csn,
       or_log.log_id,
       stg_encounter.pat_key,
       min(case when event_type = 1120000001
                then event_time end) as anesthesia_start_date,
       max(case when event_type = 1120000002
                then event_time end) as anesthesia_stop_date --select *
from
     {{source('clarity_ods','ed_iev_event_info')}} as evtinfo
     inner join {{source('clarity_ods','ed_iev_pat_info')}} as patinfo
        on evtinfo.event_id = patinfo.event_id-- where anes_id = 56054168
     inner join {{ref('stg_encounter')}} as stg_encounter
        on stg_encounter.csn = patinfo.pat_enc_csn_id
     inner join {{source('cdw','anesthesia_encounter_link')}} as aes
        on stg_encounter.visit_key = aes.anes_visit_key
     inner join {{source('cdw','or_log')}} as or_log
        on or_log.log_key = aes.or_log_key
group by
       aes.anes_id,
       stg_encounter.csn,
       or_log.log_id,
       stg_encounter.pat_key
),

surgery_timestamps as (
  select
        or_log.pat_key,
        or_log_case_times.log_key,
        min(case when dict_in_room.src_id = 5 then event_in_dt end) as in_room_date --select *
    from
        {{source('cdw','or_log_case_times')}} as or_log_case_times
        inner join {{source('cdw','or_log')}} as or_log
           on or_log_case_times.log_key = or_log.log_key
        inner join {{source('cdw','cdw_dictionary')}} as dict_in_room
            on or_log_case_times.dict_or_pat_event_key = dict_in_room.dict_key
    group by
        or_log.pat_key,
        or_log_case_times.log_key
),

surgery_anes as (
select
      or_log.case_key,
      or_log.log_key,
      anes_timestamps.anesthesia_id,
      anes_timestamps.anesthesia_csn as csn_new,
      anes_timestamps.log_id as log_id_new,
      stg_encounter.csn as anesthesia_csn,
      stg_patient.mrn,
      full_dt as surgery_date,
      or_log.log_id,
      anes_timestamps.anesthesia_start_date,
      surgery_timestamps.in_room_date,
      anes_timestamps.anesthesia_stop_date
from
    {{source('cdw','or_log')}} as or_log
    left join {{source('cdw','anesthesia_encounter_link')}} as aes
      on or_log.log_key = aes.or_log_key
    left join {{ref('stg_encounter')}} as stg_encounter
        on stg_encounter.visit_key = aes.anes_visit_key
    inner join surgery_timestamps
      on or_log.log_key = surgery_timestamps.log_key
    inner join {{ref('stg_patient')}} as stg_patient
      on or_log.pat_key = stg_patient.pat_key
    inner join {{source('cdw','master_date')}} as master_date
      on or_log.surg_dt_key = master_date.dt_key
    left join anes_timestamps
      on anes_timestamps.pat_key = surgery_timestamps.pat_key
        and surgery_timestamps.in_room_date
            between anes_timestamps.anesthesia_start_date
            and anes_timestamps.anesthesia_stop_date
where
     surg_dt_key >= 20150124
),

order_setup as (
select
      surgery_anes.mrn,
      surgery_date,
      log_id,
      log_key,
      case_key,
      procedure_order_clinical.procedure_order_id,
      procedure_order_clinical.proc_ord_key,
      coalesce(csn_new, anesthesia_csn) as anesthesia_csn_link,
      coalesce(log_id_new, log_id) as anesthesia_log_link,
      row_number() over (partition by log_key order by seq_num desc) as order_ind
from
      surgery_anes
      left join {{source('cdw','or_case_order')}} as or_case_order
        on surgery_anes.case_key = or_case_order.or_case_key
      left join {{ref('procedure_order_clinical')}} as procedure_order_clinical
        on procedure_order_clinical.proc_ord_key = or_case_order.ord_key
        and order_status != 'Canceled'
)

select *
from
    order_setup
where
    order_ind = 1

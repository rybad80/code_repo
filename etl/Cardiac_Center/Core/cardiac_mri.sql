with future_order as (
select
      procedure_order.proc_ord_key as future_proc_ord_key,
      procedure_order.proc_ord_id as future_proc_ord_id,
      procedure_order_parent_info.parent_proc_ord_key as parent_proc_ord_key,
      procedure_order.placed_dt as future_order_placed_dt,
      auth_prov_key as order_prov_key,
      proc_ord_rec_type as main_order_type,
      dict_nm as order_status,
      proc_ord_desc,
      pat_key,
      expire_dt
   from
      {{source('cdw', 'procedure_order')}} as procedure_order
      left join {{source('cdw', 'procedure_order_parent_info')}} as procedure_order_parent_info
         on procedure_order.proc_ord_key = procedure_order_parent_info.proc_ord_key
      left join {{source('cdw', 'cdw_dictionary')}} as cdw_dictionary
         on cdw_dictionary.dict_key = procedure_order.dict_ord_stat_key

  where
       (procedure_order.proc_ord_desc like '%MR%HEART%'
          or procedure_order.proc_ord_desc like '%MR%LYMPH%ANGIOGRAM%')
        and proc_ord_rec_type in ('F', 'P')
        and cancel_dt is null
        and dict_canc_rsn_key = -2
),

standing_order as (
select
      procedure_order.proc_ord_key as standing_proc_ord_key,
      procedure_order.proc_ord_id as standing_proc_ord_id,
      procedure_order_parent_info.parent_proc_ord_key as parent_proc_ord_key,
      procedure_order.placed_dt as standing_order_placed_dt
   from
      {{source('cdw', 'procedure_order')}} as procedure_order
      left join {{source('cdw', 'procedure_order_parent_info')}} as procedure_order_parent_info
         on procedure_order.proc_ord_key = procedure_order_parent_info.proc_ord_key
  where
       (procedure_order.proc_ord_desc like '%MR%HEART%'
          or procedure_order.proc_ord_desc like '%MR%LYMPH%ANGIOGRAM%')
        and proc_ord_rec_type is null
        and cancel_dt is null
),

child_order as (
select
      procedure_order.proc_ord_key as child_proc_ord_key,
      procedure_order.proc_ord_id as child_proc_ord_id,
      procedure_order_parent_info.parent_proc_ord_key as parent_proc_ord_key,
      procedure_order.placed_dt as child_order_placed_dt,
      start_dt as mri_start,
      end_dt as mri_end,
      tech_emp_key
   from
      {{source('cdw', 'procedure_order')}} as procedure_order
      left join {{source('cdw', 'procedure_order_parent_info')}} as procedure_order_parent_info
         on procedure_order.proc_ord_key = procedure_order_parent_info.proc_ord_key
  where
       (procedure_order.proc_ord_desc like '%MR%HEART%'
          or procedure_order.proc_ord_desc like '%MR%LYMPH%ANGIOGRAM%')
        and proc_ord_rec_type = 'C'
        and cancel_dt is null
),

mri_times as (
select
      radiology_event.proc_ord_key,
      max(provider.full_nm) as mri_reading_provider,
      max(case when radstat.src_id = 7 then rad_event_dt end) as mri_arrive_dttm,
      max(case when radstat.src_id = 10 then rad_event_dt end) as mri_begin_dttm,
      max(case when radstat.src_id = 30 then rad_event_dt end) as mri_end_dttm
  from
       {{source('cdw', 'radiology_event')}} as radiology_event
       left join {{source('cdw', 'radiology_reading')}} as radiology_reading
          on radiology_event.proc_ord_key = radiology_reading.proc_ord_key
             and radiology_reading.seq_num = 1
       inner join {{source('cdw', 'cdw_dictionary')}} as radevent
          on radiology_event.dict_rad_event_key = radevent.dict_key
       inner join {{source('cdw', 'cdw_dictionary')}} as radstat
          on radiology_event.dict_rad_event_stat_key = radstat.dict_key
       left join {{source('cdw', 'provider')}} as provider
          on radiology_reading.prov_key  = provider.prov_key
group by
       radiology_event.proc_ord_key
),

sched_arr as (
select
      pat_enc_csn_id,
      appt_arrival_dttm
from
      {{source('clarity_ods', 'pat_enc_4')}}
),

order_visit_link as (
select
      case when main_order_type = 'P'
           then ordapptp.pat_enc_csn_id
           else ordapptf.pat_enc_csn_id end as ord_visit_link_csn_id,
      future_order.future_proc_ord_key
 from
     future_order
     left join standing_order
        on standing_order.parent_proc_ord_key = future_order.parent_proc_ord_key
     left join child_order
        on child_order.parent_proc_ord_key = future_order.parent_proc_ord_key
     left join {{source('clarity_ods', 'appt_req_appt_links')}} as ordapptf
        on ordapptf.request_id = future_order.future_proc_ord_id
           and ordapptf.link_status_c = 1
     left join {{source('clarity_ods', 'appt_req_appt_links')}} as ordapptp
        on ordapptp.request_id = child_order.child_proc_ord_id
           and ordapptp.link_status_c = 1
),

mri_orders as (
select
     future_order.future_proc_ord_key,
     future_proc_ord_id as future_proc_ord_id,
     future_order_placed_dt,
     main_order_type,
     csn,
     patient.full_nm as patient_name,
     patient.pat_stat,
     expire_dt,
     order_status,
     proc_ord_desc,
     provider.full_nm as ordering_provider,
     ordappt.ord_visit_link_csn_id as request_appt_link,
     standing_proc_ord_key,
     standing_proc_ord_id as standing_proc_ord_id,
     child_proc_ord_key,
     child_proc_ord_id as child_proc_ord_id,
     dict_appt_stat.dict_nm as appointment_status,
     appt_arrival_dttm as sched_arrival_date,
     sched_mri.appt_dt as appointment_date,
     appt_lgth_min as sched_appt_length,
     checkin_time as checked_in_time,
     coalesce(mri_times1.mri_begin_dttm, mri_times2.mri_begin_dttm) as mri_begin_date,
     coalesce(mri_times1.mri_end_dttm, mri_times2.mri_end_dttm) as mri_end_date,
     mri_start,
     mri_end,
     sched_mri.visit_key,
     stg_encounter.visit_type,
     coalesce(mri_times1.mri_reading_provider, mri_times2.mri_reading_provider) as mri_reading_provider,
     employee.full_nm as technologist,
     pat_mrn_id as mrn,
     patient.dob as date_of_birth,
     stg_encounter.patient_class,
     patient.pat_key

 from
     future_order
     left join standing_order on standing_order.parent_proc_ord_key = future_order.parent_proc_ord_key
     left join child_order on child_order.parent_proc_ord_key = future_order.parent_proc_ord_key
     left join order_visit_link as ordappt on future_order.future_proc_ord_key = ordappt.future_proc_ord_key
     left join {{source('cdw', 'visit')}} as sched_mri on ordappt.ord_visit_link_csn_id = sched_mri.enc_id
     left join {{ref('stg_encounter')}} as stg_encounter on sched_mri.visit_key = stg_encounter.visit_key
     left join sched_arr on sched_arr.pat_enc_csn_id = stg_encounter.csn
     left join {{source('cdw', 'patient')}} as patient on patient.pat_key = future_order.pat_key
     left join {{source('cdw', 'cdw_dictionary')}} as dict_appt_stat
        on dict_appt_stat.dict_key = sched_mri.dict_appt_stat_key
     left join {{source('cdw', 'employee')}} as employee on employee.emp_key = child_order.tech_emp_key
     left join mri_times as mri_times1 on mri_times1.proc_ord_key = child_order.child_proc_ord_key
     left join mri_times as mri_times2 on mri_times2.proc_ord_key = future_order.future_proc_ord_key
     left join {{source('cdw', 'provider')}} as provider on future_order.order_prov_key = provider.prov_key
     left join {{source('clarity_ods', 'pat_enc')}} as pat_enc on pat_enc.pat_enc_csn_id = stg_encounter.csn
),

anesthesia as (
select
        visit_key,
        pat_key,
        date(anes_start_tm) as anes_date,
        anes_visit_key,
        proc_visit_key,
        anes_event_visit_key,
        anes_start_tm,
        anes_end_tm,
        doc_comp_tm,
        full_nm as anesthesiologist
from
    {{source('cdw', 'anesthesia_encounter_link')}} as anesthesia_encounter_link
    left join {{source('cdw', 'provider')}} as provider
      on anesthesia_encounter_link.prov_key = provider.prov_key

),

contrast as (
select
     visit_key,
     max(case when generic_medication_name like ('%FERUMOXYTOL%') then 1 else 0 end) as ferumoxytol,
     max(case when generic_medication_name like ('%GADOBUTROL%') then 1 else 0 end) as gadobutrol
from
    {{ref('medication_order_administration')}}
where
      generic_medication_name like ('%FERUMOXYTOL%') or generic_medication_name like ('%GADOBUTROL%')
group by
      visit_key
),

order_questions as (
select
        order_question.ord_key,
        cast(max(case when quest_id = '600511' then trim(both ' ' from cast(ansr as varchar(100))) end)
             as varchar(255)) as cardiac_sedation_need,
        cast(max(case when quest_id = '600501' then trim(both ' ' from cast(ansr as varchar(100))) end)
             as varchar(255)) as study_request,
        cast(max(case when quest_id = '600500' then trim(both ' ' from cast(ansr as varchar(100))) end)
             as varchar(255)) as sched_timeframe,
        max(case when quest_id = '600500'
                  and trim(both ' ' from cast(ansr as varchar(100))) = 'Less than 3 months'
                then 60
                when quest_id = '600500'
                  and trim(both ' ' from cast(ansr as varchar(100))) = '2-4 weeks'
                then 28
                when quest_id = '600500'
                  and trim(both ' ' from cast(ansr as varchar(100))) = 'Greater than 3 months'
                then 90
                when quest_id = '600500'
                  and trim(both ' ' from cast(ansr as varchar(100))) = 'Next Available'
                then 1
                when quest_id = '600500'
                  and trim(both ' ' from cast(ansr as varchar(100))) = 'Urgent/1 Week'
                then 7
            else null end) as mri_request_timeframe_numeric,
        cast(max(case when quest_id = '6000146' then trim(both ' ' from cast(ansr as varchar(100))) end)
             as varchar(255)) as reason_for_exam,
        cast(max(case when quest_id = '6000052' then trim(both ' ' from cast(ansr as varchar(100))) end)
             as varchar(255)) as additional_history

 from
     {{source('cdw', 'order_question')}} as order_question
     inner join {{source('cdw', 'master_question')}} as master_question
        on master_question.quest_key = order_question.quest_key
     inner join mri_orders on order_question.ord_key = mri_orders.future_proc_ord_key
where
     quest_id in ('600634', '600537', '600511', '600509', '600507', '600506', '600505', '600504',
                  '600502', '600501', '600500', '6000146', '6000052', '60000422', '6000042')
group by
      order_question.ord_key
),

mri_wq as (
select
      workqueue_nm,
      proc_ord_key,
      entry_dt as workqueue_entry_dt,
      deferred_due_dt,
      wq_tab_stat_nm,
      (extract(epoch from date(now()) - entry_dt) / 86400) as days_in_workqueue
from
     {{source('cdw', 'workqueue_schedule_order')}} as workqueue_schedule_order
     inner join {{source('cdw', 'workqueue_info')}} as workqueue_info
        on workqueue_info.wq_key = workqueue_schedule_order.wq_key
     inner join {{source('cdw', 'workqueue_schedule_order_item')}} as workqueue_schedule_order_item
        on workqueue_schedule_order.wq_key = workqueue_schedule_order_item.wq_key
     inner join {{source('cdw', 'dim_workqueue_tab_status')}} as dim_workqueue_tab_status
        on dim_workqueue_tab_status.dim_wq_tab_stat_key  = workqueue_schedule_order_item.dim_wq_tab_stat_key
where
     workqueue_info.wq_id in (3717, 3649)
     and release_dt is null
     and tab_entry_dt is not null
)


select
     mri_orders.visit_key as cardiac_study_id,
     cast(future_proc_ord_id as integer) as mri_order_id,
     csn as mri_csn,
     mrn,
     patient_name,
     date_of_birth,
     checked_in_time - date_of_birth as age_at_mri,
     future_order_placed_dt as mri_order_date,
     proc_ord_desc as procedure_order_description,
     ordering_provider,
     upper(order_status) as order_status,
     case when main_order_type = 'P' then 'INPATIENT'
         else coalesce(appointment_status, 'UNSCHEDULED')
     end as mri_appointment_status,
     case when main_order_type = 'P' then 1
          when appointment_status is null then 1
         else 0 end as unscheduled_mri_ind,
     patient_class,
     visit_type,
     sched_arrival_date,
     checked_in_time as mri_arrive_date,
     appointment_date,
     sched_appt_length,
     anes_start_tm as anesthesia_start_date,
     mri_start as mri_start_date,
     mri_end as mri_end_date,
     anes_end_tm as anesthesia_end_date,
     doc_comp_tm as documentation_complete_date,
     technologist,
     upper(mri_reading_provider) as mri_reading_provider,
     anesthesiologist,
     case when main_order_type = 'P' then '1900-01-01' else workqueue_entry_dt end as mri_workqueue_entry_date,
     deferred_due_dt as mri_deferred_due_date,
     days_in_workqueue as days_in_mri_workqueue,
     wq_tab_stat_nm as workqueue_tab_number,
     cardiac_sedation_need as mri_sedation_need,
     study_request as mri_study_request,
     sched_timeframe as mri_sched_timeframe,
     mri_request_timeframe_numeric,
     future_order_placed_dt + mri_request_timeframe_numeric as estimated_mri_appt_date,
     date(future_order_placed_dt) + mri_request_timeframe_numeric - date(now()) as mri_sched_days_until_timeframe,
     case when date(future_order_placed_dt) + mri_request_timeframe_numeric < date(now()) then 'Immediately'
              when date(future_order_placed_dt) + mri_request_timeframe_numeric - date(now()) > 0
               and date(future_order_placed_dt) + mri_request_timeframe_numeric - date(now()) <= 7
               then 'Within 1 Week'
              when date(future_order_placed_dt) + mri_request_timeframe_numeric - date(now()) > 7
               and date(future_order_placed_dt) + mri_request_timeframe_numeric - date(now()) <= 28
               then 'Within 1 Month'
              when date(future_order_placed_dt) + mri_request_timeframe_numeric - date(now()) > 28
               and date(future_order_placed_dt) + mri_request_timeframe_numeric - date(now()) <= 60
               then 'Within 2 Months'
              when date(future_order_placed_dt) + mri_request_timeframe_numeric - date(now()) > 60
               and date(future_order_placed_dt) + mri_request_timeframe_numeric - date(now()) <= 90
               then 'Within 3 Months'
              else 'Greater than 3 Months'
              end as mri_sched_days_remain,
     reason_for_exam as mri_reason_for_exam,
     additional_history as mri_addtl_history,
     case when mri_appointment_status != 'COMPLETED'
               and mri_sedation_need in ('Inpatient Team to Sedate', 'Sedation/Cardiac Anesthesia',
                                         'Sedation/Cardiac Anesthesia Standby')
               then 'Sedation'
           when mri_appointment_status != 'COMPLETED'
               and mri_sedation_need in ('No sedation', 'No sedation Feed & Swaddle')
               then 'No Sedation'
           when mri_appointment_status = 'COMPLETED'
              and anes_visit_key is not null
              then 'Sedation'
           when mri_appointment_status = 'COMPLETED'
              and anes_visit_key is null
              then 'No Sedation'
           else 'Not Indicated' end as sedation_indicator,
     case when ferumoxytol = 1 and gadobutrol = 0 then 1 else 0 end as ferumoxytol_only_ind,
     case when ferumoxytol = 0 and gadobutrol = 1 then 1 else 0 end as gadobutrol_only_ind,
     case when ferumoxytol = 1 and gadobutrol = 1 then 1 else 0 end as dual_contrast_ind,
     case when (ferumoxytol = 0 and gadobutrol = 0) or (ferumoxytol is null and gadobutrol is null)
          then 1 else 0 end as no_contrast_ind,
     mri_orders.visit_key as mri_visit_key,
     future_proc_ord_key as mri_order_key

  from
       mri_orders
       left join mri_wq on mri_wq.proc_ord_key = mri_orders.future_proc_ord_key
       left join order_questions on order_questions.ord_key = mri_orders.future_proc_ord_key
       left join contrast on contrast.visit_key = mri_orders.visit_key
       left join anesthesia on mri_orders.pat_key = anesthesia.pat_key
                              and date(mri_orders.mri_start) = date(anesthesia.anes_date)
                              and anes_start_tm < mri_end
where
     not(coalesce(mri_appointment_status, 'BLANK') = 'UNSCHEDULED' and workqueue_entry_dt is null)

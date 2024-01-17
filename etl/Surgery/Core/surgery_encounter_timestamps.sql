with log_timestamps as (
    select
        or_log_case_times.log_key,
        /* pre-op timestamps */
        min(case when dict_out_room.src_id = 1 then event_in_dt end) as in_facility_date,
        min(case when dict_out_room.src_id = 58 then event_in_dt end) as triage_date,
        min(case when dict_out_room.src_id = 49 then event_in_dt end) as request_transport_date,
        min(case when dict_out_room.src_id = 61 then event_in_dt end)
            as core_tech_acknowledge_transport_request_date,
        min(case when dict_out_room.src_id = 31 then event_in_dt end)
            as transport_team_acknowledge_transport_request_date,
        min(case when dict_out_room.src_id = 2 then event_in_dt end) as in_preop_room_date,
        max(case when dict_out_room.src_id in (33, 56) then event_in_dt end) as pre_medication_decision_date,
        min(case when dict_out_room.src_id = 34 then event_in_dt end) as acknowledge_pre_medication_request_date,
        min(case when dict_out_room.src_id = 35 then event_in_dt end) as patient_medicated_date,
        min(case when dict_out_room.src_id = 4 then event_in_dt end) as patient_ready_for_or_date,
        
        /* intra-op timestamps */
        min(case when dict_out_room.src_id = 5 then event_in_dt end) as in_room_date,
        min(case when dict_out_room.src_id = 6 then event_in_dt end) as anesthesia_start_date,
        min(case when dict_out_room.src_id in (24, 3087.0030) then event_in_dt end) as patient_prep_start_date,
        min(case when dict_out_room.src_id = 7 then event_in_dt end) as procedure_start_date,
        min(case when dict_out_room.src_id = 8 then event_in_dt end) as procedure_close_date,
        min(case when dict_out_room.src_id = 9 then event_in_dt end) as anesthesia_stop_date,
        min(case when dict_out_room.src_id in (36, 42) then event_in_dt end) as request_recovery_bed_date,
        min(case when dict_out_room.src_id = 110 then event_in_dt end) as ready_for_pacu_date,
        min(case when dict_out_room.src_id = 10 then event_in_dt end) as out_room_date,

        /* post-op timestamps */
        min(case when dict_out_room.src_id = 11 then event_in_dt end) as recovery_phase_1_date,
        min(case when dict_out_room.src_id = 50 then event_in_dt end) as ready_for_visitation_date,
        min(case when dict_out_room.src_id = 14 then event_in_dt end) as recovery_phase_2_date,
        min(case when dict_out_room.src_id = 12 then event_in_dt end) as recovery_complete_date,
        min(case when dict_out_room.src_id = 17 then event_in_dt end) as recovery_exit_date
    from
        {{source('cdw', 'or_log_case_times')}} as or_log_case_times
        inner join {{source('cdw', 'cdw_dictionary')}} as dict_out_room
            on or_log_case_times.dict_or_pat_event_key = dict_out_room.dict_key
    group by
        or_log_case_times.log_key
)

select
    stg_surgery.or_key,
    stg_surgery.mrn,
    stg_surgery.surgery_date,
    log_timestamps.in_facility_date,
    log_timestamps.triage_date,
    log_timestamps.request_transport_date,
    log_timestamps.core_tech_acknowledge_transport_request_date,
    log_timestamps.transport_team_acknowledge_transport_request_date,
    log_timestamps.in_preop_room_date,
    log_timestamps.pre_medication_decision_date,
    log_timestamps.acknowledge_pre_medication_request_date,
    log_timestamps.patient_medicated_date,
    log_timestamps.patient_ready_for_or_date,
    log_timestamps.in_room_date,
    log_timestamps.anesthesia_start_date,
    log_timestamps.patient_prep_start_date,
    log_timestamps.procedure_start_date,
    log_timestamps.procedure_close_date,
    log_timestamps.anesthesia_stop_date,
    log_timestamps.request_recovery_bed_date,
    log_timestamps.ready_for_pacu_date,
    log_timestamps.out_room_date,
    log_timestamps.recovery_phase_1_date,
    log_timestamps.ready_for_visitation_date,
    log_timestamps.recovery_phase_2_date,
    log_timestamps.recovery_complete_date,
    log_timestamps.recovery_exit_date,
    stg_surgery.hospital_discharge_date,
    {{
        dbt_chop_utils.datetime_diff(
            from_date='log_timestamps.in_room_date',
            to_date='log_timestamps.out_room_date',
            unit='hour'
        )
    }} as in_room_to_out_room_hrs,
    {{
        dbt_chop_utils.datetime_diff(
            from_date='log_timestamps.out_room_date',
            to_date='stg_surgery.hospital_discharge_date',
            unit='day'
        )
    }} as post_op_los_days,
    case
        when log_timestamps.patient_medicated_date is not null then 1 else 0
    end as patient_medicated_ind,
    stg_surgery.case_id,
    stg_surgery.log_id,
    stg_surgery.surgeon_prov_key,
    stg_surgery.case_key,
    stg_surgery.log_key,
    stg_surgery.pat_key,
    stg_surgery.hsp_acct_key,
    stg_surgery.visit_key
from
    {{ ref('stg_surgery') }} as stg_surgery
    inner join log_timestamps
        on log_timestamps.log_key = stg_surgery.log_key

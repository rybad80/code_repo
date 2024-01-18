select
    fact_periop.log_id,
    fact_periop.log_key,
    fact_periop.visit_key,

    -- pre-op timestamps
    min(case when d_case_times.src_id = 1 then event_in_dt end) as in_facility,
    -- also referred to as 'Pink team' or 'Triage'. 
    -- this is when patients are brought into PACU pre-op and ht, wt, vitals etc are recorded
    min(case when d_case_times.src_id = 58 then event_in_dt end) as intake_process,
    -- after intake process, if there are no rooms avaialable patients to back to waiting room
    min(case when d_case_times.src_id = 62 then event_in_dt end) as in_waiting_area,
    -- PACU pre-op is requesting transport from an inpatient unit for a patient to be brought to pre-op 
    min(case when d_case_times.src_id = 49 then event_in_dt end) as request_transport,
    min(case when d_case_times.src_id = 61 then event_in_dt end) as core_tech_ack_req,
    -- transport team acknowledging PACU pre-op transfer request
    min(case when d_case_times.src_id = 31 then event_in_dt end) as ack_transport_request,
    -- also referred to as "Green team"
    -- This is the last stop before the OR. patient meets w/ surgeon, anes, gets pre-med if applicable
    min(case when d_case_times.src_id = 2 then event_in_dt end) as in_preop_room,
    -- this is the 'signal' from the OR to PACU pre-op that the OR for a patient 
    -- is ready/will be ready soon so go ahead and pre-med them. Not every patient gets a pre-med
    min(case when d_case_times.src_id = 33 then event_in_dt end) as premed_patient,
    -- not all patients get a pre-medicated. 
    min(case when d_case_times.src_id = 56 then event_in_dt end) as no_premed_needed,
    -- PACU pre-op sees the OR reqesting pre-med and acknowledges it
    min(case when d_case_times.src_id = 34 then event_in_dt end) as ack_premed_req,
    -- patient usually marked as medicated about 15-25 mins after recieveing pre-med
    min(case when d_case_times.src_id = 35 then event_in_dt end) as pat_medicated,
    -- all pre-op processes are complete. this is the signal for the OR nurse to come get the patient 
    min(case when d_case_times.src_id = 4 then event_in_dt end) as pat_ready_for_or,

    -- intra-op timestamps
    min(case when d_case_times.src_id = 5 then event_in_dt end) as in_room, -- wheels into the OR
    min(case when d_case_times.src_id = 6 then event_in_dt end) as anes_sed_start,
    min(case when d_case_times.src_id in (24, 3087.0030) then event_in_dt end) as prep_start,
    min(case when d_case_times.src_id = 23 then event_in_dt end) as anes_ready,
    min(case when d_case_times.src_id = 7 then event_in_dt end) as proc_start,
    min(case when d_case_times.src_id = 38 then event_in_dt end) as out_for_test_1, -- leaving the OR for a test
    min(case when d_case_times.src_id = 100 then event_in_dt end) as out_for_test_2, -- leaving the OR for a test
    min(case when d_case_times.src_id = 102 then event_in_dt end) as out_for_test_3, -- leaving the OR for a test
    min(case when d_case_times.src_id = 39 then event_in_dt end) as return_from_test_1, -- coming back to the OR
    min(case when d_case_times.src_id = 101 then event_in_dt end) as return_from_test_2, -- coming back to the OR
    min(case when d_case_times.src_id = 103 then event_in_dt end) as return_from_test_3, -- coming back to the OR
    min(case when d_case_times.src_id = 8 then event_in_dt end) as proc_close,
    min(case when d_case_times.src_id = 36 then event_in_dt end) as req_pacu_bed,
    min(case when d_case_times.src_id = 42 then event_in_dt end) as req_cpru_bed,
	min(case when d_case_times.src_id = 110.0000 then event_in_dt end) as ready_for_pacu, --new as of 7/2019
    min(case when d_case_times.src_id = 10 then event_in_dt end) as out_room, -- wheels out of OR

    -- post-op timestamps
    min(case when d_case_times.src_id = 11 then event_in_dt end) as in_rec_phase_i, -- Phase I recovery start
    min(case when d_case_times.src_id = 50 then event_in_dt end) as ready_for_vis,
    min(case when d_case_times.src_id = 14 then event_in_dt end) as phase_ii,
    min(case when d_case_times.src_id = 9 then event_in_dt end) as anes_sed_stop,
    min(case when d_case_times.src_id = 12 then event_in_dt end) as recovery_complete,
    -- transferred to post-op dept or discharged to home
    min(case when d_case_times.src_id = 17 then event_in_dt end) as trans_disch

from {{ ref('fact_periop') }} as fact_periop
    left join {{ source('cdw', 'or_log_case_times') }} as or_log_case_times
        on or_log_case_times.log_key = fact_periop.log_key
    inner join
        {{ source('cdw', 'cdw_dictionary') }} as d_case_times on
            d_case_times.dict_key = or_log_case_times.dict_or_pat_event_key

group by fact_periop.log_id, fact_periop.log_key, fact_periop.visit_key

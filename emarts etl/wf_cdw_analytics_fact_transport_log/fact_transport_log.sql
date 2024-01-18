{{
    config(materialized = 'view')
}}

select
    tea.pat_key,
    coalesce(master_date.dt_key, -1) as transport_complete_canceled_dt_key,
    coalesce(tea.admit_visit_key, -1) as admit_visit_key,
    tea.intake_visit_key,
    cast(tea.clinical_skill_1_attendant as varchar(100)) as attendant_1,
    cast(tea.clinical_skill_1 as varchar(200)) as attendant_1_skill,
    cast(tea.clinical_skill_2_attendant as varchar(100)) as attendant_2,
    cast(tea.clinical_skill_2 as varchar(200)) as attendant_2_skill,
    cast(tea.clinical_skill_3_attendant as varchar(100)) as attendant_3,
    cast(tea.clinical_skill_3 as varchar(200)) as attendant_3_skill,
    cast(tea.clinical_skill_4_attendant as varchar(100)) as attendant_4,
    cast(tea.clinical_skill_4 as varchar(200)) as attendant_4_skill,
    cast((case when lower(tea.transport_team_group) like 'chop%' then 'CHOP'
            else 'NON-CHOP'
        end) as varchar(100)) as chop_transport_group,
    cast(tea.final_service_accepted as varchar(100)) as final_service_accepted,
    cast(upper(to_char(date(tea.intake_date), 'day')) as varchar(20)) as day_of_intake,
    cast(upper(to_char(date(tea.transport_complete_canceled_date), 'day')) as varchar(20)) as day_of_transport,
    cast(tea.delay_reason as varchar(100)) as delay_reason,
    cast(tea.diagnosis_text as varchar(100)) as diagnosis_txt,
    cast(tea.entry_user as varchar(100)) as entry_user,
    cast(tea.lights_and_siren_reason as varchar(200)) as lights_and_siren_reason,
    cast(tea.transport_mode as varchar(100)) as transport_mode,
    cast(tea.non_chop_reason as varchar(100)) as non_chop_reason,
    cast(tea.patient_age_at_intake as varchar(30)) as pat_age,
    tea.patient_full_name as pat_full_nm,
    tea.mrn as pat_mrn_id,
    cast(tea.receiving_facility as varchar(100)) as receiving_facility,
    cast(tea.referring_facility as varchar(500)) as referring_facility,
    cast(tea.sending_unit as varchar(100)) as sending_unit,
    cast(tea.shift_completing_transport as varchar(100)) as shift_completing_transport,
    cast(tea.team_availability_at_intake as varchar(100)) as team_availability,
    cast(tea.chop_team_clinical_staff as varchar(100)) as team_composition_ett,
    cast(tea.transport_type as varchar(100)) as transport_type,
    cast(tea.intake_to_assigned_mins as double) as intake_to_assigned,
    cast(tea.intake_to_accepted_mins as double) as intake_to_accepted,
    cast(tea.intake_to_enroute_mins as double) as intake_to_enroute,
    cast(tea.facility_arrive_to_facility_depart_mins as double) as bedside_to_depart,
    cast(tea.intake_to_available_mins as double) as intake_to_available,
    cast(tea.assigned_to_enroute_mins as double) as assigned_to_enroute,
    cast(tea.arrive_to_handover_mins as double) as arrive_to_handover,
    cast(tea.handover_to_available_mins as double) as handover_to_available,
    cast(tea.accepted_to_enroute_mins as double) as accepted_to_enroute,
    cast(tea.arrive_to_available_mins as double) as arrive_to_available,
    initcap(cast(tea.transport_cancel_reason as varchar(100))) as transport_cancel_reason,
    tea.admit_visit_key as admission_account,
    tea.comm_id,
    cast(tea.comp_user as varchar(100)) as comp_user,
    cast(
        case
            when tea.lt30_ind = 1
            then 'No Delay: Dispatched within 30 Minutes'
            when tea.lt30_ind = 0
            then 'Delay: Dispatched after 30 Minutes'
        end
    as varchar(100)) as delay_status,
    cast(initcap(tea.final_status) as varchar(100)) as final_status,
    cast(tea.initial_service as varchar(100)) as initial_service_contact,
    cast(visit.enc_id as bigint) as intake_account,
    cast(tea.run_rotor_num as varchar(100)) as run_rotor_num,
    tea.service_paged_date as service_paged_dt,
    tea.intake_date as intake_dt,
    tea.transport_complete_canceled_date as transport_complete_canceled_dt,
    tea.service_accepted_date as transport_service_accepted_dt,
    tea.enroute_date as enroute_dt,
    tea.arrive_referring_facility_date as bedside_arrival_dt,
    tea.depart_referring_facility_date as depart_referring_facility_dt,
    tea.arrival_date as destination_arrival_dt,
    tea.team_available_date as team_available_dt,
    tea.transport_assigned_date as transport_assigned_dt,
    tea.patient_handover_date as pat_handover_dt,
    tea.hospital_admit_date as hosp_admit_dt,
    tea.hospital_discharge_date as hosp_dischrg_dt,
    tea.enroute_date as intake_process_dt,
    cast(tea.lights_and_siren_use_ind as byteint) as lights_and_siren_use_ind,
    cast(tea.patient_condition_deteriorate_ind as byteint) as pat_condition_deteriorate_ind,
    now() as create_dt,
    cast('ENTERPRISE-MARTS' as varchar(20)) as create_by,
    cast('ENTERPRISE-MARTS' as varchar(20)) as upd_by,
    now() as upd_dt
from
    {{source('chop_analytics','transport_encounter_all')}} as tea
    left join {{source('cdw','visit')}} as visit
        on visit.visit_key = tea.intake_visit_key
    left join {{source('cdw','master_date')}} as master_date
        on master_date.full_dt = date(tea.transport_complete_canceled_date)

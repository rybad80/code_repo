select
--pull in all variables from stg_transport_all_encounters
    stg_transport_all_encounters.transport_key,
    stg_transport_all_encounters.mrn,
    stg_transport_all_encounters.patient_full_name,
    stg_transport_all_encounters.patient_age_at_intake,
    stg_transport_all_encounters.intake_date,
    stg_transport_all_encounters.transport_complete_canceled_date,
    master_date.c_yyyy        as transport_complete_canceled_date_year,
    master_date.f_yyyy        as transport_complete_canceled_date_fiscal_year,
    master_date.cy_yyyy_mm_nm as transport_complete_canceled_date_monthyear,
    stg_transport_all_encounters.hospital_admit_date,
    stg_transport_all_encounters.hospital_discharge_date,
    stg_transport_all_encounters.entry_user,
    stg_transport_all_encounters.comp_user,
    lower(stg_transport_all_encounters.final_status) as final_status,
    lower(stg_transport_all_encounters.transport_cancel_reason) as transport_cancel_reason,
    stg_transport_all_encounters.referring_facility,
    stg_transport_all_encounters.sending_unit,
    stg_transport_all_encounters.receiving_facility,
    stg_transport_all_encounters.intake_acuity_score,
    stg_transport_all_encounters.team_member_a,
    stg_transport_all_encounters.team_member_b,
    stg_transport_all_encounters.team_member_c,
    stg_transport_all_encounters.team_member_d,
    stg_transport_all_encounters.clinical_skill_1,
    stg_transport_all_encounters.clinical_skill_2,
    stg_transport_all_encounters.clinical_skill_3,
    stg_transport_all_encounters.clinical_skill_4,
    stg_transport_all_encounters.clinical_skill_1_attendant,
    stg_transport_all_encounters.clinical_skill_2_attendant,
    stg_transport_all_encounters.clinical_skill_3_attendant,
    stg_transport_all_encounters.clinical_skill_4_attendant,
    stg_transport_all_encounters.flight_vendor,
    stg_transport_all_encounters.service_paged_date,
    stg_transport_all_encounters.transport_service_accepted_date as service_accepted_date,
    stg_transport_all_encounters.transport_assigned_date,
    stg_transport_all_encounters.enroute_date,
    stg_transport_all_encounters.destination_arrival_date as arrival_date,
    stg_transport_all_encounters.team_available_date,
    stg_transport_all_encounters.bedside_arrival_date as arrive_referring_facility_date,
    stg_transport_all_encounters.patient_contact_at_bedside_date,
    stg_transport_all_encounters.depart_bedside_with_patient_date,
    stg_transport_all_encounters.depart_referring_facility_date,
    stg_transport_all_encounters.patient_handover_date,
    stg_transport_all_encounters.emcp_notified_date,
    stg_transport_all_encounters.transport_mode,
    stg_transport_all_encounters.transport_vendor,
    stg_transport_all_encounters.delay_reason,
    stg_transport_all_encounters.diagnosis_text,
    stg_transport_all_encounters.lights_and_siren_reason,
    stg_transport_all_encounters.non_chop_reason,
    stg_transport_all_encounters.shift_completing_transport,
    stg_transport_all_encounters.team_availability as team_availability_at_intake,
    stg_transport_all_encounters.team_composition_ett as chop_team_clinical_staff,
    stg_transport_all_encounters.run_rotor_num,
    stg_transport_all_encounters.lights_and_siren_use_ind,
    stg_transport_all_encounters.patient_condition_deteriorate_ind,
    stg_transport_all_encounters.final_service_accepted,
    stg_transport_all_encounters.initial_service,
    stg_transport_all_encounters.covid19_tested,
    stg_transport_all_encounters.covid19_test_date,
    stg_transport_all_encounters.covid19_results,
    stg_transport_all_encounters.covid19_expect_pended_result,
    stg_transport_all_encounters.covid19_test_type,
    stg_transport_all_encounters.covid19_vaccine_received,
    stg_transport_all_encounters.covid19_vaccine_stage,
    stg_transport_all_encounters.covid_test_result,
    stg_transport_all_encounters.covid_result_expected,
	stg_transport_all_encounters.rsv_result,
	stg_transport_all_encounters.fluab_result,
	stg_transport_all_encounters.fluab_result_expected,
	stg_transport_all_encounters.rsv_result_expected,
    stg_transport_all_encounters.respiratory_test,
    stg_transport_all_encounters.ventilator_ind,
    stg_transport_all_encounters.tracheal_tube_ind,
    stg_transport_all_encounters.cpr_ind,
    stg_transport_all_encounters.intubation_ind,
    stg_transport_all_encounters.iv_insertion_ind,
    stg_transport_all_encounters.io_insertion_ind,
    stg_transport_all_encounters.transport_young_adult_special_program_ind,
    stg_transport_all_encounters.ed_young_adult_special_program_ind,
    case
        when lower(stg_transport_all_encounters.transport_type_raw) = 'intercampus'
            then capacity_intercampus_transfers.transfer_evaluation_service
        else stg_transport_all_encounters.transfer_evaluation_service
    end as transfer_evaluation_service,
    case
        when lower(stg_transport_all_encounters.transport_type_raw) = 'intercampus'
            then capacity_intercampus_transfers.transfer_indication
        else stg_transport_all_encounters.transfer_indication
    end as transfer_indication,
    case
        when lower(stg_transport_all_encounters.transport_type_raw) = 'intercampus'
            and lower(capacity_intercampus_transfers.transfer_indication) = 'medical/surgical'
            then capacity_intercampus_transfers.transfer_reason
        else stg_transport_all_encounters.transfer_medsurg_reason
    end as transfer_medsurg_reason,
    case
        when lower(stg_transport_all_encounters.transport_type_raw) = 'intercampus'
            and lower(capacity_intercampus_transfers.transfer_reason) = 'surgical procedure'
            then (capacity_intercampus_transfers.transfer_reason_text)
        else stg_transport_all_encounters.transfer_surgical_procedure
    end as transfer_surgical_procedure,
    case
        when lower(stg_transport_all_encounters.transport_type_raw) = 'intercampus'
            and lower(capacity_intercampus_transfers.transfer_reason) = 'ir procedure'
            then (capacity_intercampus_transfers.transfer_reason_text)
        else stg_transport_all_encounters.transfer_ir_procedure_type
    end as transfer_ir_procedure_type,
    case
        when lower(stg_transport_all_encounters.transport_type_raw) = 'intercampus'
            and lower(capacity_intercampus_transfers.transfer_reason) = 'other'
            then (capacity_intercampus_transfers.transfer_reason_text)
        else stg_transport_all_encounters.transfer_medsurg_reason_other
    end as transfer_medsurg_reason_other,
    case
        when lower(stg_transport_all_encounters.transport_type_raw) = 'intercampus'
            and lower(capacity_intercampus_transfers.transfer_indication) = 'non-medical reason'
            then capacity_intercampus_transfers.transfer_reason
        else stg_transport_all_encounters.transfer_nonmedical_reason
    end as transfer_nonmedical_reason,
    stg_transport_all_encounters.selective_mode,
    stg_transport_all_encounters.final_disposition,
    stg_transport_all_encounters.nonchop_affiliate_name,
    stg_transport_all_encounters.affiliate_offered,
    stg_transport_all_encounters.affiliate_not_offered_reason,
	stg_transport_all_encounters.affiliate_success,
	stg_transport_all_encounters.affiliate_name,
    stg_transport_all_encounters.affiliate_unsuccess_reason,
    stg_transport_all_encounters.affiliate_admission_service,
    stg_transport_all_encounters.level_of_care,
    stg_transport_all_encounters.vendor_type,
    stg_transport_all_encounters.images_or_studies_completed_ind,
    stg_transport_all_encounters.ecmo_candidate_ind,
    stg_transport_all_encounters.trauma_ind,
    stg_transport_all_encounters.lowest_temp_f,
    stg_transport_all_encounters.lowest_spo2,
    stg_transport_all_encounters.transfer_type,
    stg_transport_all_encounters.intake_to_assigned_mins,
    stg_transport_all_encounters.intake_to_accepted_mins,
    stg_transport_all_encounters.intake_to_enroute_mins,
    stg_transport_all_encounters.facility_arrive_to_facility_depart_mins,
    stg_transport_all_encounters.intake_to_available_mins,
    stg_transport_all_encounters.assigned_to_enroute_mins,
    stg_transport_all_encounters.arrive_to_handover_mins,
    stg_transport_all_encounters.handover_to_available_mins,
    stg_transport_all_encounters.accepted_to_enroute_mins,
    stg_transport_all_encounters.arrive_to_available_mins,
    stg_transport_all_encounters.bedside_arrive_to_bedside_depart_mins,
    stg_transport_all_encounters.intake_to_service_paged_mins,
    stg_transport_all_encounters.service_paged_to_service_accepted_mins,
    stg_transport_all_encounters.service_accepted_to_assigned_mins,
    stg_transport_all_encounters.service_paged_to_assigned_mins,
    stg_transport_all_encounters.intake_to_arrival_mins,
    stg_transport_all_encounters.referring_depart_to_arrival_mins,
    stg_transport_all_encounters.intake_to_referring_depart_mins,
--end

--pull in variables from stg_transport_department_grouper
    stg_transport_department_grouper.accepting_department,
    stg_transport_department_grouper.accepting_service,
    stg_transport_department_grouper.accepting_service_grouped,
    stg_transport_department_grouper.initial_service_grouped,
    stg_transport_department_grouper.transfer_evaluation_service_grouped,
--end

--calculate variables from stg_transport_all_encounters
    extract(epoch from stg_transport_all_encounters.hospital_discharge_date
        - stg_transport_all_encounters.hospital_admit_date) / 3600.0 / 24.0 as total_los_days,
    case
        when lower(stg_transport_all_encounters.transport_type_raw)
            in ('canceled',
                'interfacility',
                'consult',
                'urgent direct admit',
                'ed referral',
                'outbound',
                'intercampus')
            then stg_transport_all_encounters.transport_type_raw
        when regexp_extract(lower(stg_transport_all_encounters.transport_type_raw), 'interfacility') is not null
            then 'Interfacility'
        when regexp_extract(lower(stg_transport_all_encounters.transport_type_raw), 'inbound') is not null
            then 'Inbound'
    end as transport_type,
    case
        when lower(stg_transport_all_encounters.transport_cancel_reason)
            = 'patient condition not suitable for transfer'
            then 'aborted - patient too unstable for transport'
        when lower(stg_transport_all_encounters.transport_cancel_reason)
            in ('non-par accepted', 'patient declined transfer',
                'referring location pulled request', 'took too long to respond')
            then 'other'
        else lower(stg_transport_all_encounters.transport_cancel_reason)
    end as transport_cancel_reason_group,
    case
        when lower(stg_transport_all_encounters.transport_cancel_reason) = 'no cancellation'
            and (lower(stg_transport_all_encounters.final_status) = 'completed'
                or stg_transport_all_encounters.final_status is null) then 1
        else 0
    end as transport_completed_ind,
    case
        when intake_to_enroute_mins between 0 and 60 then 1
        when intake_to_enroute_mins > 60 then 0
        else null
    end as lt60_ind,
    case
        when intake_to_enroute_mins between 0 and 30 then 1
        when intake_to_enroute_mins > 30 then 0
        else null
    end as lt30_ind,
    case
        when intake_to_referring_depart_mins between 0 and 60 then 1
        when intake_to_referring_depart_mins > 60 then 0
        else null
    end as intercampus_lt60_ind,
    case
        when lower(transport_type) in ('consult', 'urgent direct admit', 'ed referral')
            then transport_type
        when lower(stg_transport_all_encounters.transport_mode) like '%chop% %als%'
            then 'CHOP ALS'
        when lower(stg_transport_all_encounters.transport_mode) in ('chop ground')
            then 'CHOP Ground'
        when lower(stg_transport_all_encounters.transport_mode) in ('chop fixed', 'chop fixed wing')
            then 'CHOP Fixed Wing'
        when lower(stg_transport_all_encounters.transport_mode) in ('chop helicopter', 'chop rotor')
            then 'CHOP Helicopter'
        when lower(stg_transport_all_encounters.transport_mode) in ('chop pennstar')
            then 'CHOP PennSTAR'
        when lower(stg_transport_all_encounters.transport_mode) in ('1p team', '3p team',
                                                                    '7a team', '7p team', '9a team')
            then 'CHOP Unspecified'
        when lower(stg_transport_all_encounters.transport_mode) in ('non-chop ground')
            then 'Non-CHOP Ground'
        when lower(stg_transport_all_encounters.transport_mode) in
            ('non-chop fixed', 'non-chop fixed wing', 'non-chop medway', 'non-chop reva')
            then 'Non-CHOP Fixed Wing'
        when lower(stg_transport_all_encounters.transport_mode) in
            ('non-chop helicopter', 'non-chop rotor', 'non-chop pennstar',
            'non-chop rotor - other', 'non-chop rotor - pennstar')
            then 'Non-CHOP Helicopter'
        when stg_transport_all_encounters.transport_mode is null
            or lower(stg_transport_all_encounters.transport_mode) in ('{ tbd }')
            then 'TBD'
        when lower(stg_transport_all_encounters.transport_mode) in
            ('intl non-chop fixed - chop', 'intl non-chop fixed - chop ground')
            then 'INTL Non-CHOP Fixed Wing - CHOP Ground'
        when lower(stg_transport_all_encounters.transport_mode) in
            ('intl non-chop fixed - non-', 'intl non-chop fixed - non-chop ground')
            then 'INTL Non-CHOP Fixed Wing - Non-CHOP Ground'
        when lower(stg_transport_all_encounters.transport_mode) in
            ('non chop fixed  -chop grou', 'non chop fixed-chop ground')
            then 'Non-CHOP Fixed Wing - CHOP Ground'
        when lower(stg_transport_all_encounters.transport_mode) in
            ('nicu/hup team')
            then 'NICU/HUP Team'
        else stg_transport_all_encounters.transport_mode
        end as transport_team_group,

    case
        when (
            --Dealy reason ouside of CHOP and transport control
                lower(stg_transport_all_encounters.delay_reason) like '%admin%'
                or lower(stg_transport_all_encounters.delay_reason) like '%ambulance%'
                or lower(stg_transport_all_encounters.delay_reason) like '%fixed%'
                or lower(stg_transport_all_encounters.delay_reason) like '%helicopter%'
                or lower(stg_transport_all_encounters.delay_reason) like '%referring%'
                or lower(stg_transport_all_encounters.delay_reason) like '%vendor%'
            ) then 'Extrinsic to CHOP'
        when (
            --Dealy reason outside of transport control but within CHOP
                lower(stg_transport_all_encounters.delay_reason) like '%bh decision%'
                or lower(stg_transport_all_encounters.delay_reason) like '%selective mode%'
                or lower(stg_transport_all_encounters.delay_reason) like '%bed%'
                or lower(stg_transport_all_encounters.delay_reason) like '%fellow%'
                or lower(stg_transport_all_encounters.delay_reason) like '%physician%'
                or lower(stg_transport_all_encounters.delay_reason) like '%direct admission%'
            ) then 'CHOP Enterprise Wide'
        when (
            --Delay reason in transport control
                lower(stg_transport_all_encounters.delay_reason) like '%accepting service%'
                or lower(stg_transport_all_encounters.delay_reason) like '%multiple calls%'
                or lower(stg_transport_all_encounters.delay_reason) like '%team%'
                or lower(stg_transport_all_encounters.delay_reason) like '%support staff%'
                or lower(stg_transport_all_encounters.delay_reason) like '%nitric%'
                or lower(stg_transport_all_encounters.delay_reason) like '%phrn%'
            ) then 'CHOP Transport Centric'
        else null
    end as delay_reason_group,

    case when delay_reason_group is null and lt30_ind = 0 then 1 else 0 end as missing_delay_reason_ind,

    case when lower(stg_transport_all_encounters.delay_reason) like '%selective mode%'
            or lower(stg_transport_all_encounters.transport_cancel_reason) like '%selective mode%'
        then 1
        else 0
    end as selective_mode_ind,

    case when (
		(intake_to_enroute_mins between 0 and 1440)
		and lower(stg_transport_all_encounters.transport_cancel_reason) = 'no cancellation'
		and (transport_team_group like 'CHOP%'
            or transport_team_group = 'Vendor BLS/ALS/Air')
		and (delay_reason_group is null
            or lower(delay_reason_group) != 'extrinsic to chop')
		and lower(transport_type) in ('inbound', 'interfacility', 'intercampus')
		and (lower(stg_transport_all_encounters.final_status) = 'completed'
            or stg_transport_all_encounters.final_status is null)
		) then 1 else 0
    end as cetdem_ind,
    case
        when cetdem_ind = 1
            and (delay_reason_group is null
            or lower(delay_reason_group) != 'chop enterprise wide')
        then 1
        else 0
    end as ctcdem_ind,
    case when (
		lower(stg_transport_all_encounters.transport_cancel_reason) = 'no cancellation'
		and transport_team_group like 'CHOP%'
		and lower(transport_type) in ('inbound', 'interfacility', 'intercampus')
		and (lower(stg_transport_all_encounters.final_status) = 'completed'
            or stg_transport_all_encounters.final_status is null)
		) then 1 else 0
    end as median_duration_ind,

    case when lower(stg_transport_all_encounters.diagnosis_text) like '%mental status%'
        then 1
        else 0
    end as altered_mental_status_ind,

    case when lower(stg_transport_all_encounters.diagnosis_text) like '%stroke%'
        then 1
        else 0
    end as stroke_ind,

    case when (stg_transport_all_encounters.lowest_temp_f - 32.00) * (5.00 / 9.00) < 36.5
        then 1
        else 0
    end as lowest_temp_lt_36_5_ind,

    case
        when stg_transport_all_encounters.lowest_spo2 < 90 then 1
        when stg_transport_all_encounters.lowest_spo2 >= 90 then 0
        else null
    end as hypoxic_event_ind, --denominator needs to be of pts who had at least 1 spo2 reading

    case
        when stg_transport_all_encounters.lowest_spo2 < 90 then 0
        when stg_transport_all_encounters.lowest_spo2 >= 90 then 1
        else null
    end as not_hypoxic_event_ind, --denominator needs to be of pts who had at least 1 spo2 reading
--end

--blocks linked by admit key
    case when encounter_inpatient.visit_key is not null then 1 else 0 end as inpatient_ind,
    case when encounter_ed.visit_key        is not null then 1 else 0 end as ed_ind,
    encounter_inpatient.inpatient_los_days,
    encounter_ed.ed_los_hrs,
    encounter_ed.edecu_los_hrs,
    diagnosis_encounter_all.diagnosis_name as final_primary_diagnosis_name,
    cast(diagnosis_encounter_all.icd9_code as varchar(10)) as final_primary_diagnosis_icd9,
    cast(diagnosis_encounter_all.icd10_code as varchar(10)) as final_primary_diagnosis_icd10,

--foreign keys
    stg_transport_all_encounters.comm_id,
    stg_transport_all_encounters.intake_visit_key,
    case
        when lower(stg_transport_all_encounters.transport_type_raw) = 'intercampus'
            then capacity_intercampus_transfers.visit_key
        else stg_transport_all_encounters.admit_visit_key
    end as admit_visit_key,
    stg_transport_all_encounters.pat_key,
--end
--Current PCP
    patient_all.current_pcp_provider
from
    {{ ref('stg_transport_all_encounters') }} as stg_transport_all_encounters
    left join {{ source('cdw', 'master_date') }} as master_date                                  on
            master_date.full_dt = date(stg_transport_all_encounters.transport_complete_canceled_date)
    left join {{ ref('encounter_inpatient') }} as encounter_inpatient                            on
            encounter_inpatient.visit_key = stg_transport_all_encounters.admit_visit_key
    left join {{ ref('encounter_ed') }} as encounter_ed                                          on
            encounter_ed.visit_key = stg_transport_all_encounters.admit_visit_key
    left join {{ ref('diagnosis_encounter_all') }} as diagnosis_encounter_all                    on
            diagnosis_encounter_all.visit_key = stg_transport_all_encounters.admit_visit_key
            and diagnosis_encounter_all.hsp_acct_final_primary_ind = 1
    left join {{ref('capacity_intercampus_transfers')}} as capacity_intercampus_transfers
        on capacity_intercampus_transfers.transport_key = stg_transport_all_encounters.transport_key
    left join {{ ref('patient_all') }} as patient_all on patient_all.pat_key = stg_transport_all_encounters.pat_key
    inner join {{ ref('stg_transport_department_grouper') }} as stg_transport_department_grouper on
            stg_transport_department_grouper.transport_key = stg_transport_all_encounters.transport_key

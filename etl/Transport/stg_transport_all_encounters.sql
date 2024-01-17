{%- set lookup_coalesce = [
    ('service_paged_date'              ),
    ('transport_service_accepted_date' ),
    ('transport_assigned_date'         ),
    ('enroute_date'                    ),
    ('destination_arrival_date'        ),
    ('team_available_date'             ),
    ('bedside_arrival_date'            ),
    ('patient_contact_at_bedside_date' ),
    ('depart_bedside_with_patient_date'),
    ('depart_referring_facility_date'  ),
    ('patient_handover_date'           ),
    ('emcp_notified_date'              )
] %}

{%- set lookup_durations = [
    ('transport_assigned_date'         , 'intake_date'                    , 'intake_to_assigned_mins'                ),
    ('transport_service_accepted_date' , 'intake_date'                    , 'intake_to_accepted_mins'                ),
    ('enroute_date'                    , 'intake_date'                    , 'intake_to_enroute_mins'                 ),
    ('depart_referring_facility_date'  , 'bedside_arrival_date'           , 'facility_arrive_to_facility_depart_mins'),
    ('team_available_date'             , 'intake_date'                    , 'intake_to_available_mins'               ),
    ('enroute_date'                    , 'transport_assigned_date'        , 'assigned_to_enroute_mins'               ),
    ('patient_handover_date'           , 'destination_arrival_date'       , 'arrive_to_handover_mins'                ),
    ('team_available_date'             , 'patient_handover_date'          , 'handover_to_available_mins'             ),
    ('enroute_date'                    , 'transport_service_accepted_date', 'accepted_to_enroute_mins'               ),
    ('team_available_date'             , 'destination_arrival_date'       , 'arrive_to_available_mins'               ),
    ('depart_bedside_with_patient_date', 'patient_contact_at_bedside_date', 'bedside_arrive_to_bedside_depart_mins'  ),
    ('service_paged_date'              , 'intake_date'                    , 'intake_to_service_paged_mins'           ),
    ('transport_service_accepted_date' , 'service_paged_date'             , 'service_paged_to_service_accepted_mins' ),
    ('transport_assigned_date'         , 'transport_service_accepted_date', 'service_accepted_to_assigned_mins'      ),
    ('transport_assigned_date'         , 'service_paged_date'             , 'service_paged_to_assigned_mins'         ),
    ('destination_arrival_date'        , 'intake_date'                    , 'intake_to_arrival_mins'                 ),
    ('destination_arrival_date'        , 'depart_referring_facility_date' , 'referring_depart_to_arrival_mins'       ),
    ('depart_referring_facility_date'  , 'intake_date'                    , 'intake_to_referring_depart_mins'        )
] %}

{%- set lookup_concept = [
    ("'chop#ref#30070002', 'chop#ref#30070009'", "emergency"   , "Emergency Department"),
    ("'chop#ref#30070002', 'chop#ref#30070009'", "ped"         , "General Pediatrics"  ),
    ("'chop#ref#30070002', 'chop#ref#30070009'", "nicu"        , "Neonatal ICU"        ),
    ("'chop#ref#30070002', 'chop#ref#30070009'", "n.i.c.u"     , "Neonatal ICU"        ),
    ("'chop#ref#30070002', 'chop#ref#30070009'", "nursery"     , "Nursery"             ),
    ("'chop#ref#30070002', 'chop#ref#30070009'", "picu"        , "Pediatric ICU"       ),
    ("'chop#ref#30070002', 'chop#ref#30070009'", "p.i.c.u"     , "Pediatric ICU"       ),
    ("'chop#ref#30070002', 'chop#ref#30070009'", "ccu"         , "CCU"                 ),
    ("'chop#ref#30070002', 'chop#ref#30070009'", "6 south"     , "CICU"                ),
    ("'chop#ref#30070002', 'chop#ref#30070009'", "7 north east", "PCU"                 ),
    ("'chop#ref#30070002', 'chop#ref#30070009'", "chop"        , "General Pediatrics"  )
] %}

with sde as ( --referring and receiving facility
    select
        visit.visit_key,
        max(
            case
                when
                    lower(smart_data_element_all.concept_id) in ('chop#ref#30070002', 'chop#ref#30070009')
                    then cast(smart_data_element_all.element_value as nvarchar(500))
            end
        ) as referring_facility,
        max(
            case
            {%- for concept_ids, element_val, var_val in lookup_concept %}
                when
                    lower(smart_data_element_all.concept_id) in ( {{ concept_ids }} )
                    and lower(smart_data_element_all.element_value) like '%{{ element_val }}%'
                    then '{{ var_val }}'
            {%- endfor %}
                else null
            end
        ) as sending_unit,
        max(
            case
                when
                    lower(smart_data_element_all.concept_id) in ('chop#ref#30070006', 'chop#ref#30070011')
                    then cast(smart_data_element_all.element_value as nvarchar(100))
            else null
            end) as receiving_facility
    from
        {{ ref('smart_data_element_all') }} as smart_data_element_all
        left join {{ source('cdw', 'visit') }} as visit on visit.visit_key = smart_data_element_all.visit_key
    where
        lower(smart_data_element_all.concept_id) in (
            'chop#ref#30070002', --CHOP Transport Referring Facility
            'chop#ref#30070006', --CHOP Transport Receiving Facility
            'chop#ref#30070009', --CHOP Transport Outbound Referring CHOP Unit
            'chop#ref#30070011'  --CHOP Transport Inbound Receiving CHOP Unit
        )
    group by
        visit.visit_key
), 

union_set as (
--new calls
    select distinct
    {{
        dbt_utils.surrogate_key([
            'stg_transport_new_calls.comm_id',
            'coalesce(stg_transport_new_calls.admit_visit_key,
                stg_transport_outbound_admits.visit_key)',
            'stg_transport_new_calls.intake_date',
            'stg_transport_new_calls.transport_complete_canceled_date'
        ])
    }} as transport_key,
    --stg_transport_new_calls
        stg_transport_new_calls.comm_id,
        stg_transport_new_calls.intake_visit_key,
        stg_transport_new_calls.intake_date,
        coalesce(stg_transport_new_calls.admit_visit_key,
            stg_transport_outbound_admits.visit_key) as admit_visit_key,
        stg_transport_new_calls.transport_complete_canceled_date,
        coalesce(stg_transport_new_calls.hospital_admit_date,
            stg_transport_outbound_admits.hosp_admit_dt) as hospital_admit_date,
        coalesce(stg_transport_new_calls.hospital_discharge_date,
            stg_transport_outbound_admits.hosp_dischrg_dt) as hospital_discharge_date,
        stg_transport_new_calls.pat_key,
        stg_transport_new_calls.mrn,
        stg_transport_new_calls.patient_full_name,
        stg_transport_new_calls.patient_age_at_intake,
        stg_transport_new_calls.entry_user,
        stg_transport_new_calls.comp_user,
        stg_transport_new_calls.final_status,
        stg_transport_new_calls.transport_cancel_reason,
        sde.referring_facility,
        sde.sending_unit,
        sde.receiving_facility,
        stg_transport_new_calls.transport_type_raw,
        zc_tc_transfer_type.name as transfer_type,
    --end    
    --stg_transport_flowsheets linked by intake_visit_key
        intake_flowsheets.intake_acuity_score,
        stg_transport_new_calls.team_member_a,
        stg_transport_new_calls.team_member_b,
        stg_transport_new_calls.team_member_c,
        stg_transport_new_calls.team_member_d,
        intake_flowsheets.clinical_skill_1,
        intake_flowsheets.clinical_skill_2,
        intake_flowsheets.clinical_skill_3,
        intake_flowsheets.clinical_skill_4,
        intake_flowsheets.clinical_skill_1_attendant,
        intake_flowsheets.clinical_skill_2_attendant,
        intake_flowsheets.clinical_skill_3_attendant,
        intake_flowsheets.clinical_skill_4_attendant,
        intake_flowsheets.flight_vendor,
    {%- for var_name in lookup_coalesce %}
        coalesce(
            intake_flowsheets.{{ var_name }},
            admit_flowsheets.{{ var_name }}
        ) as {{ var_name }},
    {% endfor %}
        intake_flowsheets.transport_mode,
        intake_flowsheets.transport_vendor,
        intake_flowsheets.delay_reason,
        intake_flowsheets.diagnosis_text,
        intake_flowsheets.lights_and_siren_reason,
        intake_flowsheets.non_chop_reason,
        intake_flowsheets.shift_completing_transport,
        intake_flowsheets.team_availability,
        intake_flowsheets.team_composition_ett,
        intake_flowsheets.run_rotor_num,
        intake_flowsheets.lights_and_siren_use_ind,
        intake_flowsheets.patient_condition_deteriorate_ind,
        intake_flowsheets.final_service_accepted,
        intake_flowsheets.initial_service,
        intake_flowsheets.covid19_tested,
        intake_flowsheets.covid19_test_date,
        intake_flowsheets.covid19_results,
        intake_flowsheets.covid19_expect_pended_result,
        intake_flowsheets.covid19_test_type,
        intake_flowsheets.covid19_vaccine_received,
        intake_flowsheets.covid19_vaccine_stage,
        cast(intake_flowsheets.covid_test_result as varchar(10)) as covid_test_result,
        cast(intake_flowsheets.covid_result_expected as varchar(10)) as covid_result_expected,
        cast(intake_flowsheets.rsv_result as varchar(10)) as rsv_result,
        cast(intake_flowsheets.fluab_result as varchar(10)) as fluab_result,
        cast(intake_flowsheets.fluab_result_expected as varchar(10)) as fluab_result_expected,
        cast(intake_flowsheets.rsv_result_expected as varchar(10)) as rsv_result_expected,
        cast(intake_flowsheets.respiratory_test as varchar(36)) as respiratory_test,
        intake_flowsheets.ventilator_ind,
        intake_flowsheets.tracheal_tube_ind,
        intake_flowsheets.cpr_ind,
        intake_flowsheets.intubation_ind,
        intake_flowsheets.iv_insertion_ind,
        intake_flowsheets.io_insertion_ind,
        intake_flowsheets.transport_young_adult_special_program_ind,
        intake_flowsheets.ed_young_adult_special_program_ind,
        zc_pat_service.name as transfer_evaluation_service,
        intake_flowsheets.transfer_indication,
        intake_flowsheets.transfer_medsurg_reason,
        intake_flowsheets.transfer_surgical_procedure,
        intake_flowsheets.transfer_ir_procedure_type,
        intake_flowsheets.transfer_medsurg_reason_other,
        intake_flowsheets.transfer_nonmedical_reason,
        intake_flowsheets.images_or_studies_completed_ind,
        intake_flowsheets.ecmo_candidate_ind,
        intake_flowsheets.selective_mode,
        intake_flowsheets.final_disposition,
        intake_flowsheets.nonchop_affiliate_name,
        intake_flowsheets.affiliate_offered,   
	    intake_flowsheets.affiliate_not_offered_reason,
	    intake_flowsheets.affiliate_success,
	    intake_flowsheets.affiliate_name,
        intake_flowsheets.affiliate_unsuccess_reason,
        intake_flowsheets.affiliate_admission_service,
        cast(intake_flowsheets.level_of_care as varchar(20)) as level_of_care,
        cast(intake_flowsheets.vendor_type as varchar(15)) as vendor_type,
    --stg_transport_flowsheets linked by admit_visit_key
        case when lower(admit_flowsheets.trauma_registry_val) = 'yes' then 1 else 0 end as trauma_ind,
        admit_flowsheets.lowest_temp_f,
        admit_flowsheets.lowest_spo2

    from {{ ref('stg_transport_new_calls') }} as stg_transport_new_calls
        left join {{ ref('stg_transport_flowsheets') }} as intake_flowsheets                  on
                intake_flowsheets.visit_key = stg_transport_new_calls.intake_visit_key
        left join {{ ref('stg_transport_flowsheets') }} as admit_flowsheets                   on
                admit_flowsheets.visit_key = stg_transport_new_calls.admit_visit_key
        left join {{ ref('stg_transport_outbound_admits') }} as stg_transport_outbound_admits on
                stg_transport_outbound_admits.comm_id = stg_transport_new_calls.comm_id
        left join {{ source('clarity_ods', 'zc_pat_service') }} as zc_pat_service             on
                zc_pat_service.hosp_serv_c = intake_flowsheets.transfer_evaluation_service
        left join {{ source('clarity_ods', 'zc_tc_transfer_type') }} as zc_tc_transfer_type   on
                zc_tc_transfer_type.tc_transfer_type_c = stg_transport_new_calls.transfer_type
        left  join sde on 
            sde.visit_key = stg_transport_new_calls.intake_visit_key
    
union all

--old calls
    select
        {{
        dbt_utils.surrogate_key([
            'null',
            'stg_transport_historic_calls.admit_visit_key',
            'stg_transport_historic_calls.intake_date',
            'stg_transport_historic_calls.transport_complete_canceled_date'
        ])
    }} as transport_key,
        null as comm_id,
        null as intake_visit_key,
        stg_transport_historic_calls.intake_date,
        stg_transport_historic_calls.admit_visit_key,
        stg_transport_historic_calls.transport_complete_canceled_date,
        stg_transport_historic_calls.hospital_admit_date,
        stg_transport_historic_calls.hospital_discharge_date,
        stg_transport_historic_calls.pat_key,
        stg_transport_historic_calls.mrn,
        stg_transport_historic_calls.patient_full_name,
        stg_transport_historic_calls.patient_age_at_intake,
        stg_transport_historic_calls.entry_user,
        null as comp_user,
        stg_transport_historic_calls.final_status,
        stg_transport_historic_calls.transport_cancel_reason,
        stg_transport_historic_calls.referring_facility,
        stg_transport_historic_calls.sending_unit,
        stg_transport_historic_calls.receiving_facility,
        stg_transport_historic_calls.transport_type_raw,
        null as transfer_type,
        null as intake_acuity_score,
        stg_transport_historic_calls.attendant_1 as team_member_a,
        stg_transport_historic_calls.attendant_2 as team_member_b,
        stg_transport_historic_calls.attendant_3 as team_member_c,
        stg_transport_historic_calls.attendant_4 as team_member_d,
        stg_transport_historic_calls.attendant_1_skill as clinical_skill_1,
        stg_transport_historic_calls.attendant_2_skill as clinical_skill_2,
        stg_transport_historic_calls.attendant_3_skill as clinical_skill_3,
        stg_transport_historic_calls.attendant_4_skill as clinical_skill_4,
        stg_transport_historic_calls.attendant_1 as clinical_skill_1_attendant,
        stg_transport_historic_calls.attendant_2 as clinical_skill_2_attendant,
        stg_transport_historic_calls.attendant_3 as clinical_skill_3_attendant,
        stg_transport_historic_calls.attendant_4 as clinical_skill_4_attendant,
        stg_transport_historic_calls.flight_vendor,
        null as service_paged_date,
        stg_transport_historic_calls.transport_service_accepted_date,
        stg_transport_historic_calls.transport_assigned_date,
        stg_transport_historic_calls.enroute_date,
        stg_transport_historic_calls.destination_arrival_date,
        stg_transport_historic_calls.team_available_date,
        stg_transport_historic_calls.bedside_arrival_date,
        null as patient_contact_at_bedside_date,
        null as depart_bedside_with_patient_date,
        stg_transport_historic_calls.depart_referring_facility_date,
        stg_transport_historic_calls.patient_handover_date,
        null as emcp_notified_date,
        stg_transport_historic_calls.transport_mode,
        null as transport_vendor,
        stg_transport_historic_calls.delay_reason,
        stg_transport_historic_calls.diagnosis_text,
        stg_transport_historic_calls.lights_and_siren_reason,
        stg_transport_historic_calls.non_chop_reason,
        stg_transport_historic_calls.shift_completing_transport,
        stg_transport_historic_calls.team_availability,
        stg_transport_historic_calls.team_composition_ett,
        null as run_rotor_num,
        stg_transport_historic_calls.lights_and_siren_use_ind,
        stg_transport_historic_calls.patient_condition_deteriorate_ind,
        stg_transport_historic_calls.final_service_accepted,
        stg_transport_historic_calls.initial_service,
        null as covid19_tested,
        null as covid19_test_date,
        null as covid19_results,
        null as covid19_expect_pended_result,
        null as covid19_test_type,
        null as covid19_vaccine_received,
        null as covid19_vaccine_stage,
        null as covid_test_result,
        null as covid_result_expected,
        null as rsv_result,
        null as fluab_result,
        null as fluab_result_expected,
        null as rsv_result_expected,
        null as respiratory_test,
        stg_transport_historic_calls.ventilator_ind,
        stg_transport_historic_calls.tracheal_tube_ind,
        stg_transport_historic_calls.cpr_ind,
        stg_transport_historic_calls.intubation_ind,
        stg_transport_historic_calls.iv_insertion_ind,
        stg_transport_historic_calls.io_insertion_ind,
        null as transport_young_adult_special_program_ind,
        null as ed_young_adult_special_program_ind,
        null as transfer_evaluation_service,
        null as transfer_indication,
        null as transfer_medsurg_reason,
        null as transfer_surgical_procedure,
        null as transfer_ir_procedure_type,
        null as transfer_medsurg_reason_other,
        null as transfer_nonmedical_reason,
        null as images_or_studies_completed_ind,
        null as ecmo_candidate_ind,
        null as selective_mode,
        null as final_disposition,
        null as nonchop_affiliate_name,
        null as affiliate_offered,   
	    null as affiliate_not_offered_reason,
	    null as affiliate_success,
	    null as affiliate_name,
        null as affiliate_unsuccess_reason,
        null as affiliate_admission_service,
        null as level_of_care,
        null as vendor_type,
        stg_transport_historic_calls.trauma_ind,
        null as lowest_temp_f,
        null as lowest_spo2
    from
        {{ ref('stg_transport_historic_calls') }} as stg_transport_historic_calls
--end
)

select
    transport_key,
    comm_id,
    intake_visit_key,
    intake_date,
    admit_visit_key,
    transport_complete_canceled_date,
    hospital_admit_date,
    hospital_discharge_date,
    pat_key,
    mrn,
    patient_full_name,
    patient_age_at_intake,
    entry_user,
    comp_user,
    final_status,
    transport_cancel_reason,
    referring_facility,
    sending_unit,
    receiving_facility,
    transport_type_raw,
    transfer_type,
    intake_acuity_score,
    team_member_a,
    team_member_b,
    team_member_c,
    team_member_d,
    clinical_skill_1,
    clinical_skill_2,
    clinical_skill_3,
    clinical_skill_4,
    clinical_skill_1_attendant,
    clinical_skill_2_attendant,
    clinical_skill_3_attendant,
    clinical_skill_4_attendant,
    flight_vendor,
    service_paged_date,
    transport_service_accepted_date,
    transport_assigned_date,
    enroute_date,
    destination_arrival_date,
    team_available_date,
    bedside_arrival_date,
    patient_contact_at_bedside_date,
    depart_bedside_with_patient_date,
    depart_referring_facility_date,
    patient_handover_date,
    emcp_notified_date,
    transport_mode,
    transport_vendor,
    delay_reason,
    diagnosis_text,
    lights_and_siren_reason,
    non_chop_reason,
    shift_completing_transport,
    team_availability,
    team_composition_ett,
    run_rotor_num,
    lights_and_siren_use_ind,
    patient_condition_deteriorate_ind,
    final_service_accepted,
    initial_service,
    covid19_tested,
    covid19_test_date,
    covid19_results,
    covid19_expect_pended_result,
    covid19_test_type,
    covid19_vaccine_received,
    covid19_vaccine_stage,
    covid_test_result,
    covid_result_expected,
    rsv_result,
    fluab_result,
    fluab_result_expected,
    rsv_result_expected,
    respiratory_test,
    ventilator_ind,
    tracheal_tube_ind,
    cpr_ind,
    intubation_ind,
    iv_insertion_ind,
    io_insertion_ind,
    transport_young_adult_special_program_ind,
    ed_young_adult_special_program_ind,
    transfer_evaluation_service,
    transfer_indication,
    transfer_medsurg_reason,
    transfer_surgical_procedure,
    transfer_ir_procedure_type,
    transfer_medsurg_reason_other,
    transfer_nonmedical_reason,
    images_or_studies_completed_ind,
    ecmo_candidate_ind,
    trauma_ind,
    /*Calculate metrics for the duration of time each part of the transport process takes*/
    {%- for end_dt, start_dt, var_name in lookup_durations %}
        extract(
            epoch
                from
                    {{ end_dt }} - {{ start_dt }} --noqa: L016
        ) / 60.0  as {{ var_name }}, --noqa: L008
    {%- endfor %}
    lowest_temp_f,
    lowest_spo2,
    selective_mode,
    final_disposition,
    nonchop_affiliate_name,
    affiliate_offered,   
	affiliate_not_offered_reason,
	affiliate_success,
	affiliate_name,
    affiliate_unsuccess_reason,
    affiliate_admission_service,
    level_of_care,
    vendor_type
from 
    union_set

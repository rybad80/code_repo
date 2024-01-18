/*lookup for flowsheet_id and variable names*/
{%- set lookup_rnk = [
    ('3008928'                       , '1', 'meas_val'         , 'intake_acuity_score'          ),
    ('15032'                         , '1', 'meas_val'         , 'trauma_registry_val'          ),
    ('30000022'                      , '1', 'meas_val'         , 'clinical_skill_1'             ),
    ('30000022'                      , '2', 'meas_val'         , 'clinical_skill_2'             ),
    ('30000022'                      , '3', 'meas_val'         , 'clinical_skill_3'             ),
    ('30000022'                      , '4', 'meas_val'         , 'clinical_skill_4'             ),
    ('30000022'                      , '1', 'taken_by_employee', 'clinical_skill_1_attendant'   ),
    ('30000022'                      , '2', 'taken_by_employee', 'clinical_skill_2_attendant'   ),
    ('30000022'                      , '3', 'taken_by_employee', 'clinical_skill_3_attendant'   ),
    ('30000022'                      , '4', 'taken_by_employee', 'clinical_skill_4_attendant'   ),
    ('3008805, 30000708'             , '1', 'meas_val'         , 'non_chop_reason'              ),
    ('30000009, 30000703'            , '1', 'meas_val'         , 'delay_reason'                 ),
    ('30000010, 30000705'            , '1', 'meas_val'         , 'team_availability'            ),
    ('30000006'                      , '1', 'meas_val'         , 'flight_vendor'                ),
    ('3001965'                       , '1', 'meas_val'         , 'lights_and_siren_reason'      ),
    ('3008932'                       , '1', 'meas_val'         , 'covid19_tested'               ),
    ('3008933'                       , '1', 'meas_val'         , 'covid19_test_date'            ),
    ('3008934'                       , '1', 'meas_val'         , 'covid19_results'              ),
    ('3008935'                       , '1', 'meas_val'         , 'covid19_expect_pended_result' ),
    ('3008936'                       , '1', 'meas_val'         , 'covid19_test_type'            ),
    ('3003943'                       , '1', 'meas_val'         , 'covid19_vaccine_received'     ),
    ('3003944'                       , '1', 'meas_val'         , 'covid19_vaccine_stage'        ),
    ('23900'                         , '1', 'meas_val'         , 'covid_test_result'            ),
    ('23901'                         , '1', 'meas_val'         , 'covid_result_expected'        ),
    ('23902'                         , '1', 'meas_val'         , 'rsv_result'                   ),
    ('23903'                         , '1', 'meas_val'         , 'fluab_result'                 ),
    ('23904'                         , '1', 'meas_val'         , 'fluab_result_expected'        ),
    ('23905'                         , '1', 'meas_val'         , 'rsv_result_expected'          ),
    ('23906'                         , '1', 'meas_val'         , 'respiratory_test'             ),
    ('30000006, 30000661, 30000704'  , '1', 'meas_val'         , 'transport_mode'               ),
    ('23450'                         , '1', 'meas_val'         , 'transport_vendor'             ),
    ('3008899, 3008900, 30000706'    , '1', 'meas_val'         , 'shift_completing_transport'   ),
    ('3008884, 3008885'              , '1', 'meas_val'         , 'run_rotor_num'                ),
    ('3008809, 3008810, 30000707'    , '1', 'meas_val'         , 'team_composition_ett'         ),
    ('300700750, 300700200, 30000281', '1', 'meas_val'         , 'diagnosis_text'               ),
    ('20708'                         , '1', 'meas_val'         , 'transfer_evaluation_service'  ),
    ('20687'                         , '1', 'meas_val'         , 'transfer_indication'          ),
    ('30000714'                      , '1', 'meas_val'         , 'transfer_medsurg_reason'      ),
    ('20701'                         , '1', 'meas_val'         , 'transfer_surgical_procedure'  ),
    ('20700'                         , '1', 'meas_val'         , 'transfer_ir_procedure_type'   ),
    ('20709'                         , '1', 'meas_val'         , 'transfer_medsurg_reason_other'),
    ('20697'                         , '1', 'meas_val'         , 'transfer_nonmedical_reason'   ),
    ('3004000'                       , '1', 'meas_val'         , 'selective_mode'               ),
    ('300302'                        , '1', 'meas_val'         , 'final_disposition'            ),
    ('300304'                        , '1', 'meas_val'         , 'nonchop_affiliate_name'       ),
    ('300305'                        , '1', 'meas_val'         , 'affiliate_offered'            ),
    ('300312'                        , '1', 'meas_val'         , 'affiliate_not_offered_reason' ),
    ('300309'                        , '1', 'meas_val'         , 'affiliate_success'            ),
    ('300310'                        , '1', 'meas_val'         , 'affiliate_name'               ),
    ('300311'                        , '1', 'meas_val'         , 'affiliate_unsuccess_reason'   ),
    ('23449'                         , '1', 'meas_val'         , 'affiliate_admission_service'  ),
    ('25021'                         , '1', 'meas_val'         , 'level_of_care'                ),
    ('26273'                         , '1', 'meas_val'         , 'vendor_type'                  )
] %}

{%- set lookup_meas_val = [
    ('3001963'           , 'yes'         , 'lights_and_siren_use_ind'                 ),
    ('3001964, 300700747', 'yes'         , 'patient_condition_deteriorate_ind'        ),
    ('3003941'           , 'yes'         , 'transport_young_adult_special_program_ind'),
    ('3003942'           , 'yes'         , 'ed_young_adult_special_program_ind'       ),
    ('30000022'          , 'ventilator'  , 'ventilator_ind'                           ),
    ('30000022'          , 'trach'       , 'tracheal_tube_ind'                        ),
    ('30000022'          , 'cpr'         , 'cpr_ind'                                  ),
    ('30000022'          , 'intubation'  , 'intubation_ind'                           ),
    ('30000022'          , 'io insertion', 'io_insertion_ind'                         ),
    ('30000022'          , 'iv insertion', 'iv_insertion_ind'                         ),
    ('3008750'           , 'yes'         , 'images_or_studies_completed_ind'          ),
    ('3008811'           , 'yes'         , 'ecmo_candidate_ind'                       )
] %}

{%- set lookup_date = [
    ('service accepted'           , 'transport_service_accepted_date' ),
    ('intake process'             , 'intake_process_date'             ),
    ('call assigned'              , 'transport_assigned_date'         ),
    ('enroute'                    , 'enroute_date'                    ),
    ('arrived destination'        , 'destination_arrival_date'        ),
    ('available time'             , 'team_available_date'             ),
    ('arrived%eferring'           , 'bedside_arrival_date'            ),
    ('patient contact at bedside' , 'patient_contact_at_bedside_date' ),
    ('depart bedside with patient', 'depart_bedside_with_patient_date'),
    ('depart%eferring'            , 'depart_referring_facility_date'  ),
    ('patient handover completed' , 'patient_handover_date'           ),
    ('e-mcp'                      , 'emcp_notified_date'              )
] %}

with
    raw_flowsheet as (--pull all flowsheet data
        select
            flowsheet_all.visit_key,
            flowsheet_all.meas_val,
            flowsheet_all.hospital_admit_date,
            flowsheet_all.recorded_date,
            flowsheet_all.flowsheet_id,
            flowsheet_all.taken_by_employee,
            rank() over(partition by flowsheet_all.visit_key, flowsheet_all.flowsheet_id
                order by flowsheet_all.recorded_date desc) as rnk,
            rank() over(partition by flowsheet_all.visit_key, flowsheet_all.flowsheet_id, flowsheet_all.meas_val
                order by flowsheet_all.recorded_date desc) as max_rnk,
            rank() over(partition by flowsheet_all.visit_key, flowsheet_all.flowsheet_id, flowsheet_all.meas_val
                order by flowsheet_all.recorded_date asc ) as min_rnk
        from
            {{ ref('flowsheet_all') }} as flowsheet_all
        where
            flowsheet_all.flowsheet_id in (
                6,         --temp
                10,        --SPO2
                15032,     --Trauma Registry Patient
                20687,     --Transfer Indication
                20697,     --Non-Medical Reason
                20705,     --Surgical Procedure Type
                20706,     --IR Procedure Type
                20707,     --Other Medical/Surgical Reason
                20708,     --Transfer Evaluation Service
                3001963,   --Lights and Sirens Used?
                3001965,   --Reason for Lights/Sirens Use
                3003941,   --Is this adult patient being transferred to CHOP 
                                --due to the Young Adult Special Program?
                3003942,   --Is this adult patient being seen at CHOP due to the Young Adult Special Program?
                3003943,   --Has the patient received a COVID Vaccine?
                3003944,   --What stage in the vaccination process is this patient? 
                3008750,   --Has the patient had any images or studies completed at your facility?
                3008805,   --Reason for Non-CHOP
                3008809,   --Team Composition
                3008810,   --Outbound Team Composition
                3008811,   --Ecmo Candidate?
                3008813,   --Inbound Service
                3008814,   --Outbound Service
                3008882,   --Consult Service
                3008884,   --Ground Transport Run Number
                3008885,   --Rotor Transport Run Number
                3008888,   --Call Status
                3008889,   --Outbound Call Status
                3008890,   --Consult Call Status
                3008899,   --Transport Team (Shift)
                3008900,   --Outbound Transport Team (Shift)
                3008928,   --Transport Acuity Intake Score
                3008932,   --Was this patient tested for COVID-19? 
                3008933,   --When was the test taken? specify date, and if < 24 hours ago, the time (COVID-19 test)
                3008934,   --What was the COVID-19 result? 
                3008935,   --When do you expect the pended result? (specify date and time) 
                3008936,   --What type of COVID-19 test was performed?
                23900,     --What was the result of the Covid-19 test?
                23901,     --Covid-19 results expected in:
                23902,     --What was the result of the RSV test?
                23903,     --What was the result of the Flu A/B test?
                23904,     --Flu A/B results expected in:
                23905,     --RSV results expected in:
                23906,     --Was this patient recently tested for any of the following:
                30000003,  --Call Status
                30000006,  --Transport Mode
                23450,     --Transport Vendor Name
                30000009,  --Reason for Delay
                30000010,  --Team Availability
                30000022,  --Enter Skills Used
                30000281,  --Intercampus Chief Complaint
                30000661,  --Outbound Transport Mode
                30000665,  --Outbound Call Status
                30000701,  --Intercampus Call Status
                30000702,  --Intercampus Service
                30000703,  --Intercampus Reason for Delay
                30000704,  --Intercampus Transport Mode
                30000705,  --Intercampus Team Availability
                30000706,  --Intercampus Transport Team (Shift)
                30000707,  --Intercampus Team Composition
                30000708,  --Intercampus Reason for NonCHOP
                30000714,  --Med/Surg Reason
                300700200, --Chief Complaint
                300700750, --Outbound Chief Complaint
                3004000,   --CHOP is in Transport Selective Mode 
                300302,    --Transport Final Disposition
                300304,    --Non-CHOP Affiliate Name 
                300305,    --Affiliate offered or not     
                300312,    --Affiliate not offered reason
                300309,    --Affiliate successful or not
                300310,    --Affiliate name
                300311,    --Affiliate unsuccess reason      
                23449,     --Affiliate admission service
                25021,     --CQI: level of care
                26273      --VMSC Vendor Type
            )
            and flowsheet_all.meas_val is not null
            /*Epic Transfer Center launched on this date.
                There are no relevant data prior to this date in the flowsheet rows*/
            and flowsheet_all.entry_date >= to_date('12/10/2018', 'mm/dd/yyyy')
)

select
    raw_flowsheet.visit_key,
    /*Reference lookup_rnk table at top of script to define new variables based on
    corresponding flowsheet ids, flowsheet variables, and rank values */
    {%- for fs_id, rnk_value, var_value, var_name in lookup_rnk %}
        max(
            case
                when raw_flowsheet.flowsheet_id in ( {{ fs_id }} )
                    and raw_flowsheet.rnk = {{ rnk_value }}
                then cast(raw_flowsheet.{{ var_value }} as nvarchar(100))
                else null
            end
        ) as {{ var_name }},
    {% endfor %}
    /*Reference lookup_meas_val table at top of script to define new variables based on
    corresponding flowsheet ids and meas_val values */
    {% for fs_id, meas_val_value, var_name in lookup_meas_val %}
        max(
            case
                when raw_flowsheet.flowsheet_id in ( {{ fs_id }} )
                    and lower(raw_flowsheet.meas_val) like '%{{ meas_val_value }}%'
                then 1
                else 0
            end
        ) as {{ var_name }},
    {% endfor %}
    /*Reference lookup_date table at top of script to define new variables based on
    corresponding meas_val values */
    {%- for meas_val_value, var_name in lookup_date %}
        max(
            case
                when raw_flowsheet.flowsheet_id in (30000003, 30000665, 3008888, 3008890, 3008889, 30000701)
                    and lower(raw_flowsheet.meas_val) like '%{{ meas_val_value }}%'
                    and raw_flowsheet.max_rnk = '1'
                then cast(raw_flowsheet.recorded_date as datetime)
                else null
            end
        ) as {{ var_name }},
    {% endfor %}
    max(
        case
            when raw_flowsheet.flowsheet_id in (3008888, 3008889, 3008890, 30000701)
                and lower(raw_flowsheet.meas_val) like '%service paged%'
                and raw_flowsheet.min_rnk = '1'
            then cast(service.meas_val as nvarchar(100))
            else null
        end
    ) as initial_service,
    max(
        case
            when raw_flowsheet.flowsheet_id in (3008888, 3008889, 3008890, 30000701)
                and lower(raw_flowsheet.meas_val) like '%service accepted%'
                and raw_flowsheet.max_rnk = '1'
            then cast(service.meas_val as nvarchar(100))
            else null
        end
    ) as final_service_accepted,
    min(
        case
            when raw_flowsheet.flowsheet_id in (3008888, 3008890, 3008889, 30000701)
                and lower(raw_flowsheet.meas_val) like '%service paged%'
                and raw_flowsheet.min_rnk = '1'
            then cast(raw_flowsheet.recorded_date as datetime)
            else null
        end
    ) as service_paged_date,
    min(
        case
            when raw_flowsheet.flowsheet_id = 6
                and (raw_flowsheet.recorded_date <= raw_flowsheet.hospital_admit_date)
            then cast(raw_flowsheet.meas_val as decimal(7, 2))
            else null
        end
    ) as lowest_temp_f,
    min(
        case
            when raw_flowsheet.flowsheet_id = 10
                and (raw_flowsheet.recorded_date <= raw_flowsheet.hospital_admit_date)
            then cast(raw_flowsheet.meas_val as decimal(7, 2))
            else null
        end
    ) as lowest_spo2
from
    raw_flowsheet
    left join raw_flowsheet as service on raw_flowsheet.visit_key = service.visit_key
                and raw_flowsheet.recorded_date = service.recorded_date
                --Inbound, Outbound, Consult, and Intercampus Service
                and service.flowsheet_id in (3008813, 3008814, 3008882, 30000702)
group by
    raw_flowsheet.visit_key

with
    transport_call_log as (--select all transport_call_log
        select
            cast(transport_call_log.intake_date as datetime) as intake_date,
            visit.visit_key as admit_visit_key,
            cast(transport_call_log.complete as datetime) as transport_complete_canceled_date,
            visit.hosp_admit_dt as hospital_admit_date,
            visit.hosp_dischrg_dt as hospital_discharge_date,
            patient.pat_key,
            repeat('0', 8 - length(transport_call_log.mrn)) || transport_call_log.mrn as mrn,
            transport_call_log.patient_name as patient_full_name,
            extract(
                epoch from transport_call_log.intake_date - patient.dob
                ) / (86400.00 * 365.25) as patient_age_at_intake,
            transport_call_log.intake_rn as entry_user,
            case
                when lower(transport_call_log.canceled_reason) = 'no cancellation' then 'completed'
                when lower(transport_call_log.canceled_reason) != 'no cancellation'
                    and transport_call_log.canceled_reason is not null then 'canceled'
                else null
                end as final_status,
            transport_call_log.canceled_reason as transport_cancel_reason,
            transport_call_log.referring_facility,
            case
                when transport_call_log.sending_unit = 'cardiology (telemetry)' then 'ccu'
                when transport_call_log.sending_unit = 'cardiology icu'         then 'cicu'
                else transport_call_log.sending_unit
                end as sending_unit,
            transport_call_log.receiving_facility,
            transport_call_log.attendant_1,
            transport_call_log.attendant_1_skill,
            transport_call_log.attendant_2,
            transport_call_log.attendant_2_skill,
            transport_call_log.attendant_3,
            transport_call_log.attendant_3_skill,
            transport_call_log.attendant_4,
            transport_call_log.attendant_4_skill,
            transport_call_log.flight_vendor,
            cast(transport_call_log.accepted_canceled       as datetime) as transport_service_accepted_date,
            cast(transport_call_log.call_assigned           as datetime) as transport_assigned_date,
            cast(transport_call_log.enroute_time            as datetime) as enroute_date,
            cast(transport_call_log.arrive_destination_time as datetime) as destination_arrival_date,
            cast(transport_call_log.available_time          as datetime) as team_available_date,
            cast(transport_call_log.bedside_time            as datetime) as bedside_arrival_date,
            cast(transport_call_log.depart_referring_time   as datetime) as depart_referring_facility_date,
            cast(transport_call_log.patient_handover_time   as datetime) as patient_handover_date,
            transport_call_log.mode as transport_mode,
            transport_call_log.delay_30min as delay_reason,
            transport_call_log.diagnosis as diagnosis_text,
            transport_call_log.lights_and_siren_reason,
            transport_call_log.nonchop_reason as non_chop_reason,
            transport_call_log.shift_completing_transport,
            transport_call_log.team_availability,
            transport_call_log.team_composition_ett,
            case when lower(transport_call_log.lights_and_siren_use) = 'yes'
                then 1 else 0 end as lights_and_siren_use_ind,
            case when lower(transport_call_log.deterioration_in_pt_condition) = 'yes'
                then 1 else 0 end as patient_condition_deteriorate_ind,
            transport_call_log.chop_subspecialty as final_service_accepted,
            transport_call_log.initial_sub_speciality_contacted as initial_service,
            case when lower(transport_call_log.attendant_1_skill
                            || transport_call_log.attendant_2_skill
                            || transport_call_log.attendant_3_skill
                            || transport_call_log.attendant_4_skill) like '%ventilator%'
                 then 1 else 0 end as ventilator_ind,
            case when lower(transport_call_log.attendant_1_skill
                            || transport_call_log.attendant_2_skill
                            || transport_call_log.attendant_3_skill
                            || transport_call_log.attendant_4_skill) like '%trach%'
                then 1 else 0 end as tracheal_tube_ind,
            case when lower(transport_call_log.attendant_1_skill
                            || transport_call_log.attendant_2_skill
                            || transport_call_log.attendant_3_skill
                            || transport_call_log.attendant_4_skill) like '%cpr%'
                then 1 else 0 end as cpr_ind,
            case when lower(transport_call_log.attendant_1_skill
                            || transport_call_log.attendant_2_skill
                            || transport_call_log.attendant_3_skill
                            || transport_call_log.attendant_4_skill) like '%intubation%'
                then 1 else 0 end as intubation_ind,
            case when lower(transport_call_log.attendant_1_skill
                            || transport_call_log.attendant_2_skill
                            || transport_call_log.attendant_3_skill
                            || transport_call_log.attendant_4_skill) like '%iv insertion%'
                then 1 else 0 end as iv_insertion_ind,
            case when lower(transport_call_log.attendant_1_skill
                            || transport_call_log.attendant_2_skill
                            || transport_call_log.attendant_3_skill
                            || transport_call_log.attendant_4_skill) like '%io insertion%'
                then 1 else 0 end as io_insertion_ind,
            case when lower(trauma) = 'yes' then 1 else 0 end as trauma_ind,
            transport_call_log.type_of_transport as transport_type_raw,

            /*create row number per transport to remove duplicates later -
            Since no unique identifiers, using pat_key, whether transport encounter was completed,
            transport type (inbound/outbound/etc.), and date to identify unique transports*/
            row_number() over(partition by
                    patient.pat_key,
                    transport_call_log.complete,
                    case
                        when lower(transport_call_log.type_of_transport) like '%inbound%'  then 'inbound'
                        when lower(transport_call_log.type_of_transport) like '%outbound%' then 'outbound'
                        when lower(transport_call_log.type_of_transport) like '%canceled%' then 'canceled'
                        end
                order by abs(extract(epoch from(
                    case
                        when
                            lower(
                                transport_call_log.type_of_transport
                            ) like '%outbound%' then visit.hosp_dischrg_dt
                        else visit.hosp_admit_dt
                        end
                    - transport_call_log.complete)))) as rownumber
        from
            {{ source('manual_ods', 'transport_call_log') }} as transport_call_log
            inner join {{ source('cdw', 'patient') }} as patient               on
                    patient.pat_mrn_id = repeat('0', 8 - length(transport_call_log.mrn)) || transport_call_log.mrn
            inner join {{ source('cdw', 'visit') }} as visit                   on visit.pat_key = patient.pat_key
            inner join {{ source('cdw', 'cdw_dictionary') }} as cdw_dictionary
                on cdw_dictionary.dict_key = visit.dict_enc_type_key
        where
            cdw_dictionary.src_id = 3 /*hospital encounter*/ --noqa: PRS
            and (/*Correct admission linked to the intake encounter*/
                    (/*For inbound transports, the admission occurred w/in 24 hours of transport*/
                        lower(transport_call_log.type_of_transport) like '%inbound%'
                        and (date(visit.hosp_admit_dt)
                            between date(transport_call_log.complete) - 1 and date(transport_call_log.complete))
                    )
                    or (/*For outbound transports, the discharge occurred w/in 24 hours of transport*/
                        lower(transport_call_log.type_of_transport) like '%outbound%'
                        and (date(visit.hosp_dischrg_dt)
                            between date(transport_call_log.complete) and date(transport_call_log.complete) + 1)
                    )
            )
)

select
    transport_call_log.*
from transport_call_log
where transport_call_log.rownumber = 1

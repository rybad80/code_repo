with visit_treatments as (
    select
        stg_ipc_pat_pat_union_all.action_key,
        stg_ipc_pat_pat_union_all.index_patient_visit_key,
        stg_ipc_pat_pat_union_all.match_patient_visit_key,
        provider.full_nm as provider_name,
        row_number() over (partition by
                                stg_ipc_pat_pat_union_all.action_key,
                                stg_ipc_pat_pat_union_all.index_patient_visit_key,
                                stg_ipc_pat_pat_union_all.match_patient_visit_key
                            order by
                                visit_treatment.prov_start_dt desc,
                                provider.prov_id
        ) as visit_treatment_line
    from
        {{ref('stg_ipc_pat_pat_union_all')}} as stg_ipc_pat_pat_union_all
        left join {{source('cdw', 'visit_treatment')}} as visit_treatment
            on visit_treatment.visit_key = stg_ipc_pat_pat_union_all.index_patient_visit_key
            and visit_treatment.prov_start_dt
                between stg_ipc_pat_pat_union_all.matched_patient_start_date and stg_ipc_pat_pat_union_all.matched_patient_end_date --noqa:L016
        left join {{source('cdw', 'provider')}} as provider
            on provider.prov_key = visit_treatment.prov_key
            and lower(provider.prov_type) != 'resource'
)

select
    stg_ipc_pat_pat_union_all.index_patient_visit_key,
    stg_ipc_pat_pat_union_all.action_key,
    stg_ipc_pat_pat_union_all.action_seq_num,
    index_patient.patient_name as index_patient_name,
    index_patient.mrn as index_patient_mrn,
    index_patient.dob as index_patient_dob,
    index_patient.csn as index_patient_csn,
    index_patient.sex as index_patient_sex,
    index_patient.home_phone_number as index_home_phone_number,
    index_patient.mailing_address_line1 as index_mailing_address_line1,
    index_patient.mailing_address_line2 as index_mailing_address_line2,
    index_patient.mailing_city as index_mailing_city,
    index_patient.mailing_state as index_mailing_state,
    index_patient.mailing_zip as index_mailing_zip,
    index_patient.county as index_county,
    stg_ipc_pat_pat_union_all.index_patient_start_date,
    stg_ipc_pat_pat_union_all.index_patient_end_date,
    stg_ipc_pat_pat_union_all.location_index_bed,
    stg_ipc_pat_pat_union_all.match_patient_visit_key,
    match_patient.patient_name as match_patient_name,
    match_patient.mrn as match_patient_mrn,
    match_patient.dob as match_patient_dob,
    match_patient.csn as match_patient_csn,
    match_patient.sex as match_patient_sex,
    match_patient.home_phone_number as match_home_phone_number,
    match_patient.mailing_address_line1 as match_mailing_address_line1,
    match_patient.mailing_address_line2 as match_mailing_address_line2,
    match_patient.mailing_city as match_mailing_city,
    match_patient.mailing_state as match_mailing_state,
    match_patient.mailing_zip as match_mailing_zip,
    match_patient.county as match_county,
    stg_ipc_pat_pat_union_all.matched_patient_start_date,
    stg_ipc_pat_pat_union_all.matched_patient_end_date,
    round(extract( --noqa: PRS
        epoch from matched_patient_end_date - matched_patient_start_date) / 60.0 / 60, 2
    ) as length_of_exposure,
    stg_ipc_pat_pat_union_all.event_date,
    stg_ipc_pat_pat_union_all.event_description,
    stg_ipc_pat_pat_union_all.location_room,
    stg_ipc_pat_pat_union_all.location_department,
    coalesce(
        visit_treatments.provider_name,
        index_patient.primary_care_provider,
        index_patient.provider_name
    ) as provider_name
from
    {{ref('stg_ipc_pat_pat_union_all')}} as stg_ipc_pat_pat_union_all
    inner join {{ref('stg_ipc_pat_pat_final_columns')}} as index_patient
        on index_patient.visit_key = stg_ipc_pat_pat_union_all.index_patient_visit_key
    inner join {{ref('stg_ipc_pat_pat_final_columns')}} as match_patient
        on match_patient.visit_key = stg_ipc_pat_pat_union_all.match_patient_visit_key
    left join visit_treatments
        on visit_treatments.action_key = stg_ipc_pat_pat_union_all.action_key
        and visit_treatments.index_patient_visit_key = stg_ipc_pat_pat_union_all.index_patient_visit_key
        and visit_treatments.match_patient_visit_key = stg_ipc_pat_pat_union_all.match_patient_visit_key
        and visit_treatments.visit_treatment_line = 1

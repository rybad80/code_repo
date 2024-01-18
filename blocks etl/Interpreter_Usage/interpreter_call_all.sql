select
    {{
        dbt_utils.surrogate_key([
            'lsa_data.call_record_number',
            'lsa_data.raw_mrn'
        ])
    }} as primary_key,
    'LSA' as source,
    lsa_data.mrn,
    lsa_data.raw_mrn,
    lsa_data.pat_key,
    lsa_data.requested_language,
    lsa_data.interpreter_id,
    lsa_data.service_start_time,
    lsa_data.service_end_time,
    lsa_data.service_duration_min,
    null as session_type,
    lsa_data.visit_key,
    lsa_data.hospital_admit_date,
    lsa_data.hospital_discharge_date,
    lsa_data.encounter_date
from
    {{ref('interpreter_call_details_lsa')}} as lsa_data

union all

select
    {{
        dbt_utils.surrogate_key([
            'amn_data.session_id',
            'amn_data.start_time',
            'amn_data.interpreter_id',
            'amn_data.raw_mrn'
        ])
    }} as primary_key,
    'AMN_STRATUS' as source,
    amn_data.mrn,
    amn_data.raw_mrn,
    amn_data.pat_key,
    amn_data.requested_language,
    amn_data.interpreter_id,
    amn_data.service_start_time,
    amn_data.service_end_time,
    amn_data.duration_min,
    amn_data.session_type,
    amn_data.visit_key,
    amn_data.hospital_admit_date,
    amn_data.hospital_discharge_date,
    amn_data.encounter_date
from
    {{ref('interpreter_call_details_amn_stratus')}} as amn_data

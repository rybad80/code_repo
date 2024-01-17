{{ config(
    materialized='table', 
    dist='visit_key',
    meta = {
        'critical': true
    } 
) }}

select
    visit.visit_key,
    clarity_ser.prov_name as secondary_provider_name,
    clarity_ser.prov_type as secondary_provider_type
from
    {{source('cdw','visit')}} as visit
    inner join {{source('clarity_ods','pat_enc_appt')}} as pat_enc_appt
        on pat_enc_appt.pat_enc_csn_id = visit.enc_id
    inner join {{source('clarity_ods','clarity_ser')}} as clarity_ser
        on clarity_ser.prov_id = pat_enc_appt.prov_id
where
    pat_enc_appt.line = 2
    and lower(clarity_ser.prov_type) != 'resource'

{{ config(materialized='table', dist='pat_enc_csn_id') }}

--This query pulls the number of check-ins per encounter.
select
    pat_enc_es_aud_act.pat_enc_csn_id,
    count(pat_enc_es_aud_act.es_audit_time) as check_in_count
from {{source('clarity_ods', 'pat_enc_es_aud_act')}} as pat_enc_es_aud_act
where
    pat_enc_es_aud_act.es_audit_time < current_date
    and pat_enc_es_aud_act.es_audit_action_c = 2
group by
    pat_enc_es_aud_act.pat_enc_csn_id

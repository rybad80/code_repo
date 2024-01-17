{{ 
    config(
        materialized='table',
        dist='visit_key',
        meta={
            'critical': true
        }
    )
}}

select
    visit.visit_key,
    date_trunc('day', pat_enc_es_aud_act.es_audit_time) as orig_appt_made_date,
    case when pat_enc_es_aud_act.es_audit_action_c = 8 then 1 else 0 end as rescheduled_ind,
    /* indicator for appointments scheduled through mychop 382 = 'mychart, generic'; 483 = 'mychartbguser' */
    case when pat_enc_es_aud_act.es_audit_user_id in('382', '483') then 1 else 0 end as mychop_scheduled_ind
from
    {{source('clarity_ods','pat_enc_es_aud_act')}} as pat_enc_es_aud_act
    inner join {{source('cdw','visit')}} as visit
        on visit.enc_id = pat_enc_es_aud_act.pat_enc_csn_id
where
    pat_enc_es_aud_act.es_audit_action_c in (1, 8)  --1 = made on; 8 = rescheduled
    and date_trunc('day', pat_enc_es_aud_act.es_audit_time) >= '20160901' --mychop startdate

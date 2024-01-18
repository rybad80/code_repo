{{ config(
    materialized='table',
    dist='visit_key',
    meta = {
        'critical': true
    }
) }}

select distinct
    visit.visit_key,
   pat_enc_4.echkin_status_c as echeckin_status,
   zc_echkin_step_stat.name as echeckin_status_name,
   case when pat_enc_4.echkin_status_c = 5 then 1 else 0 end as echeckin_complete_ind,
   pat_enc_3.begin_checkin_dttm as begin_checkin_date
from
    {{source('cdw', 'visit')}} as visit
     left join {{source('clarity_ods', 'pat_enc_3')}} as pat_enc_3
      on  visit.enc_id = pat_enc_3.pat_enc_csn
    left join {{source('clarity_ods', 'pat_enc_4')}} as pat_enc_4
        on  visit.enc_id = pat_enc_4.pat_enc_csn_id
    left join {{source('clarity_ods', 'zc_echkin_step_stat')}} as zc_echkin_step_stat
    on pat_enc_4.echkin_status_c = zc_echkin_step_stat.echkin_step_stat_c
    where pat_enc_3.begin_checkin_dttm is not null or pat_enc_4.echkin_status_c is not null

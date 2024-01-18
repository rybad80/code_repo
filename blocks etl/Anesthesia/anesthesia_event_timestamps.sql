select
     aes.anes_id as anesthesia_id,
     stg_surgery_log.log_id,
     evttype.record_id,
     evttype.event_name as anesthesia_event_desc,
     event_time as anesthesia_event_date,
     line as seq_num
from
     {{source('clarity_ods', 'ed_iev_event_info')}} as evtinfo
     inner join {{source('clarity_ods', 'ed_iev_pat_info')}} as patinfo
        on evtinfo.event_id = patinfo.event_id
     inner join {{source('clarity_ods', 'ed_event_tmpl_info')}} as evttype
        on evttype.record_id = evtinfo.event_type
     inner join {{source('cdw', 'visit')}} as visit
        on visit.enc_id = patinfo.pat_enc_csn_id
     inner join {{source('cdw', 'anesthesia_encounter_link')}} as aes
        on visit.visit_key = aes.anes_visit_key
     inner join {{ref('stg_surgery')}} as stg_surgery_log
        on stg_surgery_log.log_key = aes.or_log_key

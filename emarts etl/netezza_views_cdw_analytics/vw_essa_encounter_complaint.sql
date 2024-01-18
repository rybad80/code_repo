select
    vr.visit_key as "Encounter Key", 
    case when (vr.seq_num = 1) then 'Yes'::"VARCHAR" else 'No'::"VARCHAR" end as "Chief Complaint Indicator", 
    mr.rsn_nm as "Complaint Name", 
    vr.rsn_cmt as "Complaint Impression" 
from 
    {{ source('cdw', 'visit_reason') }} vr 
    join {{ source('cdw', 'master_reason_for_visit') }} mr on mr.rsn_key = vr.rsn_key
select
    100 as facility_code,
    case
        when (v.enc_id is null) then cast('9999999999' as numeric(10, 0))
        else v.enc_id
    end as account_number,
    vd.seq_num as sequence_number,
    d.icd9_cd as icd9_diagnosis_code,
    'ICD-9-CM' as codeset
from
        {{source('cdw', 'visit_diagnosis')}} as vd
        left join {{ref('visit')}} as v on ((vd.visit_key = v.visit_key))
        left join {{source('cdw', 'diagnosis')}} as d on ((vd.dx_key = d.dx_key))
where
    (upper(vd.create_by) = 'FASTRACK')

select
    vis.visit_key,
    pat.pat_key,
    hav.hsp_acct_key,
    s_diag.line,
    md.dx_key,
    s_diag.admit_diag_text
from
    {{ref('s_hsp_admit_diag')}} as s_diag
    left join {{source('cdw', 'visit')}} as vis on
        case
            when s_diag.pat_enc_csn_id is not null then s_diag.pat_enc_csn_id
            when 0 is not null then '0'::int8 else null::int8
        end = vis.enc_id
    left join {{source('cdw', 'patient')}} as pat on s_diag.pat_id = pat.pat_id
    left join {{source('cdw', 'master_diagnosis')}} as md on
        case
            when s_diag.dx_id is not null then s_diag.dx_id
            when 0 is not null then '0'::int8
            else null::int8
        end = md.dx_id
    left join {{source('cdw', 'hospital_account_visit')}} as hav on
        case
            when vis.visit_key is not null then vis.visit_key
            when 0 is not null then '0'::int8
            else null::int8
        end = case
            when hav.visit_key is not null then hav.visit_key
            when 0 is not null then '0'::int8
            else null::int8
        end

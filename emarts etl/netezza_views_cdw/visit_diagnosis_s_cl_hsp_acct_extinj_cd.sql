with hav as (
    select distinct
        hospital_account_visit.visit_key,
        hospital_account_visit.hsp_acct_key
    from
        {{source('cdw', 'hospital_account_visit')}} as hospital_account_visit
    where
        hospital_account_visit.pri_visit_ind = 1
)
select
    hav.visit_key,
    ha.pat_key,
    ha.hsp_acct_key,
    s_cd.line,
    diag.dx_key,
    s_cd.ecode_dx_poa_c
from
    {{ref('s_cl_hsp_acct_extinj_cd')}} as s_cd
    left join {{source('cdw', 'master_diagnosis')}} as diag on
        (
            case
                when s_cd.ext_injury_dx_id is not null then s_cd.ext_injury_dx_id
                when 0 is not null then cast('0' as int8)
                else cast(null as int8)
            end = diag.dx_id
        )
    left join {{source('cdw', 'hospital_account')}} as ha on s_cd.hsp_account_id = ha.hsp_acct_id
    left join hav on ha.hsp_acct_key = hav.hsp_acct_key

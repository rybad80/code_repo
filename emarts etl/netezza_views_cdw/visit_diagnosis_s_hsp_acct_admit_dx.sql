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
    dx.line,
    diag.dx_key,
    dx.admit_dx_text
from
    {{ref('s_hsp_acct_admit_dx')}} as dx
    left join {{source('cdw', 'master_diagnosis')}} as diag on
        case
            when dx.admit_dx_id is not null then dx.admit_dx_id
            when 0 is not null then '0'::int8
            else null::int8
        end = diag.dx_id
    left join {{source('cdw', 'hospital_account')}} as ha on dx.hsp_account_id = ha.hsp_acct_id
    left join hav on ha.hsp_acct_key = hav.hsp_acct_key

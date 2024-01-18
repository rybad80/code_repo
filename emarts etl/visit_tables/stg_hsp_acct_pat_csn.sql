select
    hsp_acct_pat_csn.hsp_account_id,
    hsp_acct_pat_csn.line,
    hsp_acct_pat_csn.pat_id,
    hsp_acct_pat_csn.pat_enc_csn_id,
    --hsp_acct_pat_csn.pat_enc_date_real,
    hsp_acct_pat_csn.pat_enc_date,
    hsp_acct_pat_csn.cm_phy_owner_id,
    hsp_acct_pat_csn.cm_log_owner_id,
    case when hsp_account.hsp_account_id is not null then 1 else 0 end as pri_visit_ind
from
    {{source('clarity_ods','hsp_acct_pat_csn')}} as hsp_acct_pat_csn
    left join {{source('clarity_ods','hsp_account')}} as hsp_account
        on hsp_account.hsp_account_id = hsp_acct_pat_csn.hsp_account_id
        and hsp_account.prim_enc_csn_id = hsp_acct_pat_csn.pat_enc_csn_id

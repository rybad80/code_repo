select
    hospital_account.hsp_acct_key,
    master_date.full_dt as date_overturned,
    max(case when rfl_disposition.denied_dispostn_c in (9, 23, 50)
            then 1 else 0 end) as peer_to_peer_overturned_ind

from
    {{source('clarity_ods', 'rfl_cvg_den_start')}} as den_start
    inner join {{source('clarity_ods', 'rfl_cvg_den_end')}} as den_end
        on den_end.referral_id = den_start.referral_id
        and den_end.group_line = den_start.group_line
        and den_end.value_line = den_start.value_line

    inner join {{source('cdw', 'master_date')}} as master_date
        on master_date.full_dt >= den_start.denied_start_date
        and master_date.full_dt <= den_end.denied_end_date

    inner join {{source('cdw', 'referral')}} as referral
        on referral.rfl_id = den_start.referral_id
    inner join {{source('cdw', 'hospital_account')}} as hospital_account
        on hospital_account.authcert_rfl_key = referral.rfl_key

    inner join {{source('clarity_ods', 'rfl_cvg_den_dispos')}} as rfl_disposition
        on rfl_disposition.referral_id = den_start.referral_id
        and rfl_disposition.group_line = den_start.group_line
        and rfl_disposition.value_line = den_start.value_line
        and rfl_disposition.denied_dispostn_c
            in (9, 23, 24, 25, 26, 27, 40, 50) -- overturned dispositions

group by
    hospital_account.hsp_acct_key,
    master_date.full_dt

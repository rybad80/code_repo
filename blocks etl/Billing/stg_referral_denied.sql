with denial_metrics as (
    select
        rfl_cvg_den_start.referral_id,
        rfl_cvg_den_start.group_line,
        rfl_cvg_den_start.value_line,
        max(case
            when rfl_cvg_den_dispos.denied_dispostn_c
                in (8, 9, 11, 13, 16, 17, 18, 20, 21, 23, 24, 25, 26,
                    27, 28, 29, 30, 31, 32, 33, 34, 39, 41, 50, 51)
            then 1
            when rfl_cvg_den_appeal.denied_appealed_yn = 'Y' then 1
            else 0 end
        ) as rfl_appealed_ind,
        max(case when rfl_cvg_den_dispos.denied_dispostn_c
                in (9, 11, 23, 28, 33, 50) then 1 else 0 end)
        as peer_to_peer_appealed_ind,
        max(case when flowsheet.fs_id = 320000003095
            then cast(flowsheet_measure.meas_val as int)
            + to_date('12/31/1840', 'MM/DD/YYYY')
            else null end
        ) as rfl_denial_closed_date,
        max(case when flowsheet.fs_id is null then 0 else 1 end)
        as rfl_denial_open_ind
    from
        {{source('clarity_ods', 'rfl_cvg_den_start')}} as rfl_cvg_den_start
        left join {{source('clarity_ods', 'rfl_cvg_den_dispos')}} as rfl_cvg_den_dispos
            on rfl_cvg_den_dispos.referral_id = rfl_cvg_den_start.referral_id
            and rfl_cvg_den_dispos.group_line = rfl_cvg_den_start.group_line
            and rfl_cvg_den_dispos.value_line = rfl_cvg_den_start.value_line
        left join {{source('clarity_ods', 'rfl_cvg_den_appeal')}} as rfl_cvg_den_appeal
            on rfl_cvg_den_appeal.referral_id = rfl_cvg_den_start.referral_id
            and rfl_cvg_den_appeal.group_line = rfl_cvg_den_start.group_line
            and rfl_cvg_den_appeal.value_line = rfl_cvg_den_start.value_line
        inner join {{source('cdw', 'referral')}} as referral
            on referral.rfl_id = rfl_cvg_den_start.referral_id
        inner join {{source('cdw', 'hospital_account')}} as hospital_account
            on hospital_account.authcert_rfl_key = referral.rfl_key
        inner join {{source('cdw', 'hospital_account_visit')}} as hospital_account_visit
            on hospital_account.hsp_acct_key = hospital_account_visit.hsp_acct_key
        inner join {{source('cdw', 'visit')}} as visit
            on hospital_account_visit.visit_key = visit.visit_key
        -- following joins are to get to the flowsheet record 
        -- to find denial closed date for referral denials
        left join {{source('cdw', 'visit_stay_info')}} as visit_stay_info
            on visit_stay_info.visit_key = visit.visit_key
        left join {{source('cdw', 'flowsheet_record')}} as flowsheet_record
            on flowsheet_record.vsi_key = visit_stay_info.vsi_key
        left join {{source('cdw', 'flowsheet_measure')}} as flowsheet_measure
            on flowsheet_measure.fs_rec_key = flowsheet_record.fs_rec_key
        left join {{source('cdw', 'flowsheet')}} as flowsheet
            on flowsheet.fs_key = flowsheet_measure.fs_key
            and flowsheet.fs_id = 320000003095
    group by
        rfl_cvg_den_start.referral_id,
        rfl_cvg_den_start.group_line,
        rfl_cvg_den_start.value_line
),

denial_info as (
    select
        visit.visit_key,
        visit.hosp_admit_dt as hospital_admit_date,
        -- if null return today's date for easy filtering 
        coalesce(visit.hosp_dischrg_dt, cast(now() as date)) as hospital_discharge_date,
        hospital_account.hsp_acct_key,
        hospital_account.hsp_acct_id as har,
        visit.enc_id as csn,
        referral.rfl_key,
        rfl_cvg_den_start.denied_start_date,
        rfl_cvg_den_end.denied_end_date,
        zc_denied_reason.title as rfl_denial_reason,
        payor.payor_nm as rfl_denial_payor,
        denial_metrics.rfl_appealed_ind,
        denial_metrics.peer_to_peer_appealed_ind,
        denial_metrics.rfl_denial_closed_date,
        denial_metrics.rfl_denial_open_ind
    from
        {{source('clarity_ods', 'rfl_cvg_den_start')}} as rfl_cvg_den_start
        inner join {{source('clarity_ods', 'rfl_cvg_den_end')}} as rfl_cvg_den_end
            on rfl_cvg_den_end.referral_id = rfl_cvg_den_start.referral_id
            and rfl_cvg_den_end.group_line = rfl_cvg_den_start.group_line
            and rfl_cvg_den_end.value_line = rfl_cvg_den_start.value_line
        inner join denial_metrics
            on denial_metrics.referral_id = rfl_cvg_den_start.referral_id
            and denial_metrics.group_line = rfl_cvg_den_start.group_line
            and denial_metrics.value_line = rfl_cvg_den_start.value_line
        inner join {{source('cdw', 'referral')}} as referral
            on referral.rfl_id = rfl_cvg_den_start.referral_id
        left join {{source('clarity_ods', 'rfl_cvg_den_dispos')}} as rfl_cvg_den_dispos
            on rfl_cvg_den_dispos.referral_id = rfl_cvg_den_start.referral_id
            and rfl_cvg_den_dispos.group_line = rfl_cvg_den_start.group_line
            and rfl_cvg_den_dispos.value_line = rfl_cvg_den_start.value_line
        left join {{source('clarity_ods', 'rfl_cvg_den_rsn_c')}} as rfl_cvg_den_rsn_c
            on rfl_cvg_den_rsn_c.referral_id = rfl_cvg_den_start.referral_id
            and rfl_cvg_den_rsn_c.group_line = rfl_cvg_den_start.group_line
            and rfl_cvg_den_rsn_c.value_line = rfl_cvg_den_start.value_line
        left join {{source('clarity_ods', 'zc_denied_reason')}} as zc_denied_reason
            on zc_denied_reason.denied_reason_c = rfl_cvg_den_rsn_c.denied_reason_c
        -- following joins to get the denial payor
        left join {{source('clarity_ods', 'referral_cvg')}} as referral_cvg
            on referral_cvg.referral_id = rfl_cvg_den_start.referral_id
            and referral_cvg.line = rfl_cvg_den_start.group_line
        left join {{source('cdw', 'coverage')}} as coverage
            on coverage.cvg_id = referral_cvg.cvg_id
        left join {{source('cdw', 'payor')}} as payor
            on payor.payor_key = coverage.payor_key
        inner join {{source('cdw', 'hospital_account')}} as hospital_account
            on hospital_account.authcert_rfl_key = referral.rfl_key
        inner join {{source('cdw', 'hospital_account_visit')}} as hospital_account_visit
            on hospital_account.hsp_acct_key = hospital_account_visit.hsp_acct_key
        inner join {{source('cdw', 'visit')}} as visit
            on hospital_account_visit.visit_key = visit.visit_key
        -- want visits found in encounter_inpatient ONLY
        inner join {{ref('stg_encounter_inpatient')}} as stg_encounter_inpatient
            on visit.visit_key = stg_encounter_inpatient.visit_key
)

select
    denial_info.visit_key,
    denial_info.hsp_acct_key,
    denial_info.har,
    denial_info.csn,
    denial_info.rfl_key,
    master_date.full_dt as date_denied,
    denial_info.rfl_denial_reason,
    denial_info.rfl_denial_payor,
    denial_info.rfl_appealed_ind,
    denial_info.peer_to_peer_appealed_ind,
    denial_info.rfl_denial_closed_date,
    denial_info.rfl_denial_open_ind
from
    denial_info
    inner join {{source('cdw', 'master_date')}} as master_date
        on master_date.full_dt between denial_info.denied_start_date and denial_info.denied_end_date
        and master_date.full_dt >= cast(denial_info.hospital_admit_date as date)
        and master_date.full_dt < cast(denial_info.hospital_discharge_date as date)

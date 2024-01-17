with denial_info as (
    select
        stg_encounter_inpatient.visit_key,
        hospital_account.hsp_acct_key,
        hospital_account.hsp_acct_id as har,
        visit.enc_id as csn,
        visit.hosp_admit_dt,
        case when visit.hosp_dischrg_dt is not null
            then cast(visit.hosp_dischrg_dt as date)
            else cast(now() as date) end as discharge_date,
        bdc_info.bdc_id,
        bdc_info.bdc_create_date,
        payor.payor_nm as bdc_denial_payor,
        case when bdc_info.record_status_c = 90 then 0 else 1 end as bdc_open_ind,
        case when bdc_open_ind = 0 then bdc_info.last_update_dttm
            end as bdc_closed_date,
        case when bdc_appeal.level_1_appeal_date is not null
            or bdc_appeal.level_2_appeal_date is not null
            or bdc_appeal.level_3_appeal_date is not null
            or bdc_appeal.level_4_appeal_date is not null
            or bdc_appeal.level_1_tracking_id is not null
            or bdc_appeal.level_2_tracking_id is not null
            or bdc_appeal.level_3_tracking_id is not null
            or bdc_appeal.level_4_tracking_id is not null
            or bdc_appeal.level_1_decision_c is not null
            or bdc_appeal.level_2_decision_c is not null
            or bdc_appeal.level_3_decision_c is not null
            or bdc_appeal.level_4_decision_c is not null
        then 1
        else 0 end as bdc_appealed_ind,
        -- the following indicators are used to rank the "top" bdc id to pull in data such as denial reason
        -- BDC Records often have multiple records with denial info on it
        case when bdc_info.remit_code_id = 9999 then 1 else 0 end as remit_code_ind,
        -- owning area case management
        case when bdc_info.owning_area_c = 2113 then 1 else 0 end as owning_area_ind,
        denial_reason.title as bdc_denial_reason
    from
        {{source('clarity_ods', 'bdc_info')}} as bdc_info
        inner join {{source('cdw', 'liability_bucket')}} as liability_bucket
            on bdc_info.bucket_id = liability_bucket.bkt_id
        inner join {{source('cdw', 'liability_bucket_account_xref')}}
            as liability_bucket_account_xref
            on liability_bucket.liab_bkt_key
            = liability_bucket_account_xref.liab_bkt_key
        inner join {{source('cdw', 'hospital_account')}} as hospital_account
            on liability_bucket_account_xref.hsp_acct_key = hospital_account.hsp_acct_key

        inner join {{source('cdw', 'hospital_account_visit')}} as hospital_account_visit
            on hospital_account.hsp_acct_key = hospital_account_visit.hsp_acct_key
        inner join {{source('cdw', 'visit')}} as visit
            on hospital_account_visit.visit_key = visit.visit_key

        -- inner join to stg_adt_all to get ONLY encounters found in encounter_inpatient
        inner join {{ref('stg_encounter_inpatient')}} as stg_encounter_inpatient
            on visit.visit_key = stg_encounter_inpatient.visit_key
        left join {{source('clarity_ods', 'bdc_rac_appeal_info')}} as bdc_appeal
            on bdc_info.bdc_id = bdc_appeal.bdc_id
        -- Denial Reason information
        left join {{source('clarity_ods', 'zc_rac_rslt_rsn')}} as denial_reason
            on bdc_appeal.result_reason_c = denial_reason.rac_rslt_rsn_c
        -- Denial Payor
        left join {{source('clarity_ods', 'hsp_bdc_payor')}} as hsp_bdc_payor
            on bdc_info.bdc_id = hsp_bdc_payor.bdc_id

        left join {{source('cdw', 'payor')}} as payor
            on hsp_bdc_payor.payor_id = payor.payor_id

    where
        bdc_info.record_type_c = 1 -- denial records ONLY
        and bdc_info.record_status_c != 99 -- no voided denial records
),

denial_dates as (

        select
            denied_dates.bdc_id,
            master_date.full_dt as date_denied
        from
            {{source('clarity_ods', 'hsp_bdc_appeal_dates')}} as denied_dates
            inner join {{source('cdw', 'master_date')}} as master_date
                on master_date.full_dt between denied_dates.appeal_start_date
                and denied_dates.appeal_end_date
        group by
            denied_dates.bdc_id,
            master_date.full_dt
        -- want to return all records from both tables
        -- if located in both hsp_bdc_appeal_dates and hsp_bdc_denied_dates the denial has been overturned
    union all
        select
            denied_dates.bdc_id,
            master_date.full_dt as date_denied
        from
            {{source('clarity_ods', 'hsp_bdc_denied_dates')}} as denied_dates
            inner join {{source('cdw', 'master_date')}} as master_date
                on master_date.full_dt between denied_dates.denied_start_date
                and denied_dates.denied_end_date
        group by
            denied_dates.bdc_id,
            master_date.full_dt

),

denied_dates_grouped as (
-- USED TO DETERMINE IF DENIED DATE WAS OVERTURNED
    select
        bdc_id,
        date_denied,
        count(*) as denied_grouped_count,
        -- if denied_grouped_count ==== 2 then date is overturned (located in both denied and overturn dates)
        case when denied_grouped_count = 2 then 1 else 0 end as overturned_ind
    from
        denial_dates
    group by
        bdc_id,
        date_denied
)

select
    denial_info.visit_key,
    denial_info.hsp_acct_key,
    denial_info.har,
    denial_info.csn,
    denied_dates_grouped.date_denied,
    denied_dates_grouped.overturned_ind,
    denial_info.bdc_id,
    denial_info.bdc_denial_payor,
    denial_info.bdc_open_ind,
    denial_info.bdc_closed_date,
    denial_info.bdc_appealed_ind,
    denial_info.bdc_denial_reason,
    row_number() over(partition by denial_info.hsp_acct_key, denied_dates_grouped.date_denied
            order by
                denial_info.remit_code_ind desc,
                denial_info.owning_area_ind desc,
                denial_info.bdc_appealed_ind desc,
                denial_info.bdc_open_ind desc,
                denied_dates_grouped.overturned_ind desc,
                denial_info.bdc_create_date desc)
            as bdc_ranking
from
    denial_info
    inner join denied_dates_grouped
        on denial_info.bdc_id = denied_dates_grouped.bdc_id
        -- denied dates must be during patients hospital admission (catch user entry error)
        and cast(denial_info.hosp_admit_dt as date) <= denied_dates_grouped.date_denied
        and denial_info.discharge_date > denied_dates_grouped.date_denied

{{ config(meta = {
    'critical': true
}) }}

with buckets as (
-- gets the payors and liablity buckets for the hospital accounts
    select
        hospital_account.hsp_acct_id,
        bdc_info.bucket_id,
        payor.payor_id,
        case
            when to_char(bdc_info.bdc_receive_date, 'yyyymmdd') between 20201001 and 20210228
                and payor.payor_id = '1074' and bdc_info.remit_code_id = '29' then 0
            else 1
        end as kf_29_ind
    from
        {{source('clarity_ods', 'bdc_info')}} as bdc_info
    inner join
        {{source('clarity_ods', 'hsp_bdc_payor')}} as hsp_bdc_payor
            on hsp_bdc_payor.bdc_id = bdc_info.bdc_id
    inner join
        {{source('cdw', 'payor')}} as payor
            on hsp_bdc_payor.payor_id = payor.payor_id
    inner join
        {{source('cdw', 'liability_bucket')}} as liability_bucket
            on liability_bucket.bkt_id = bdc_info.bucket_id
    inner join
        {{source('cdw', 'liability_bucket_account_xref')}} as liability_bucket_account_xref
            on liability_bucket_account_xref.liab_bkt_key = liability_bucket.liab_bkt_key
    inner join
        {{source('cdw', 'hospital_account')}} as hospital_account
            on hospital_account.hsp_acct_key = liability_bucket_account_xref.hsp_acct_key
    inner join
        {{source('cdw', 'cdw_dictionary')}} as denial_status
            on denial_status.dict_key = liability_bucket.dict_bkt_stat_key
    inner join
        {{source('cdw', 'master_remittance')}} as master_remittance
            on master_remittance.remit_key = bdc_info.remit_code_id
    inner join
        {{source('cdw', 'cdw_dictionary')}} as denial_type
            on master_remittance.dict_remit_cd_grp_two_key = denial_type.dict_key
    where
        bdc_info.record_type_c = 1 -- denial records only
        and bdc_info.record_status_c != 99
    and denial_type.src_id not in (6, 10, 11) --excludes contractual, informational, and self pay denial types
    and bdc_info.remit_code_id not in ('101', '23') -- 101 considered informational and 23 is sub-contractual
    and denial_status.src_id in (5, 7) --only liability buckets that have a status of outstanding or closed
    and bdc_info.remit_code_id != 59
    and to_char(bdc_info.bdc_receive_date, 'yyyymmdd') >= 20200701
    --bdc_info.bdc_receive_date >= add_months(date_trunc('year',current_date),-6)
),

dates as (
-- gets a month end date for every combo of bucket and payor for each account
    select distinct
        buckets.*,
        last_day(master_date.full_dt) as full_dt
    from
        buckets
    cross join
        {{source('cdw', 'master_date')}} as master_date
    where
        buckets.kf_29_ind = 1
        and to_char(master_date.full_dt, 'yyyymmdd') >= 20200630
),



balances as (
    select
        row_number() over (partition by buckets.hsp_acct_id, buckets.payor_id, buckets.bucket_id,
            last_day(hsp_bkt_snapshot.snap_start_date)
            order by buckets.hsp_acct_id, buckets.payor_id, buckets.bucket_id,
                (hsp_bkt_snapshot.snap_end_date) desc) as row_num,
        buckets.hsp_acct_id,
        buckets.payor_id,
        buckets.bucket_id,
        snap_start_date,
        snap_end_date,
        last_day(snap_start_date) as end_month,
        bucket_bal
    from
        buckets
    inner join
        {{source('clarity_ods', 'hsp_bkt_snapshot')}} as hsp_bkt_snapshot
            on hsp_bkt_snapshot.bucket_id = buckets.bucket_id
            and buckets.kf_29_ind = 1
),

latest_account_month as (
    select
        hsp_acct_id,
        payor_id,
        bucket_id,
        max(end_month) as end_month
    from
        balances
    where
        row_num = 1
        and to_char(end_month, 'yyyymmdd') < 20200701
    group by
        1, 2, 3
),

month_balances as (
    select
        latest_account_month.hsp_acct_id,
        latest_account_month.payor_id,
        latest_account_month.bucket_id,
        '2020-06-30 00:00:00' as end_month,
        balances.bucket_bal
    from
        latest_account_month
    left join
        balances
            on balances.bucket_id = latest_account_month.bucket_id
            and balances.end_month = latest_account_month.end_month
    where
        balances.row_num = 1
    union
    select
        balances.hsp_acct_id,
        balances.payor_id,
        balances.bucket_id,
        balances.end_month,
        balances.bucket_bal
    from
        balances
    where
        to_char(balances.end_month, 'yyyymmdd') >= 20200701
        and balances.row_num = 1
),

final_balance_list as (
    select
        dates.hsp_acct_id,
        dates.payor_id,
        dates.bucket_id,
        dates.full_dt as end_month,
        month_balances.bucket_bal
    from
        dates
    left join
        month_balances
            on month_balances.bucket_id = dates.bucket_id
            and month_balances.end_month = dates.full_dt
)

select
    hsp_acct_id as hospital_account_id,
    payor_id,
    bucket_id,
    end_month as balance_month,
    last_value(bucket_bal ignore nulls) over ( partition by hsp_acct_id, payor_id, bucket_id --noqa: PRS
        order by hsp_acct_id, payor_id, bucket_id, end_month rows
        between unbounded preceding and current row) as month_bal
from
    final_balance_list
where
    to_char(end_month, 'yyyymmdd') between 20200701
        and to_char(add_months(last_day(current_date), -1), 'yyyymmdd')

select
    liability_bucket.bkt_id as bucket_id,
    master_date.full_dt as ar_date,
    fact_ar_history_hb.amt as amount,
    fact_ar_history_hb.hsp_amt as hospital_amount,
    cdw_dict_lb_bkt_sts.dict_nm as bucket_status,
    cdw_dict_lb_bkt_typ.dict_nm as bucket_type,
    liability_bucket.bkt_nm as bucket_name,
    cdw_pyr_arh_pyr.payor_id as arhist_payor_id,
    case
        when (lower(payor.payor_nm) = 'invalid' or lower(payor.payor_nm) = 'default') then null
        else payor.payor_nm
    end as payor_name,
    dict1.dict_nm as hospital_base_class,
    dict2.dict_nm as hospital_account_class,
    hospital_account.dnb_dt as discharged_not_billed_date,
    liability_bucket.first_clm_dt as first_claim_date,
    dict6.dict_nm as hospital_billing_status,
    hospital_account.acct_bill_dt as hospital_account_billed_date,
    case
        when ((hospital_account.bad_debt_flag_ind = '1')
        and ((hospital_account.extern_ar_flag_ind = '0')
        or (hospital_account.extern_ar_flag_ind = '-2'))) then 'bad debt'
        when ((hospital_account.extern_ar_flag_ind = '1')
        and ((hospital_account.bad_debt_flag_ind = '0')
        or (hospital_account.bad_debt_flag_ind = '-2'))) then 'external ar'
        else 'none'
    end as ar_collection_status,
    last_day(add_months(date_trunc('month', now()), -1)) - hospital_account.disch_dt as discharge_days
from
   {{source('cdw', 'liability_bucket')}} as liability_bucket
left outer join
    {{source('cdw', 'liability_bucket_account_xref')}} as liability_bucket_account_xref
        on liability_bucket_account_xref.liab_bkt_key = liability_bucket.liab_bkt_key
left outer join
    {{source('cdw', 'hospital_account')}} as hospital_account
        on liability_bucket_account_xref.hsp_acct_key = hospital_account.hsp_acct_key
left join
    {{source('cdw', 'cdw_dictionary')}} as dict1
        on hospital_account.dict_acct_basecls_key = dict1.dict_key
left join
    {{source('cdw', 'cdw_dictionary')}} as dict2
        on hospital_account.dict_acct_class_key = dict2.dict_key
left join
    {{source('cdw', 'cdw_dictionary')}} as dict6
        on hospital_account.dict_bill_stat_key = dict6.dict_key
left join
    {{source('cdw', 'payor')}} as payor
        on payor.payor_key = hospital_account.pri_payor_key
left outer join
    {{source('cdw', 'cdw_dictionary')}} as cdw_dict_lb_bkt_sts
        on liability_bucket.dict_bkt_stat_key = cdw_dict_lb_bkt_sts.dict_key
left outer join
    {{source('cdw', 'cdw_dictionary')}} as cdw_dict_lb_bkt_typ
        on cdw_dict_lb_bkt_typ.dict_key = liability_bucket.dict_bkt_type_key
left outer join
    {{source('cdw', 'payor')}} as cdw_payor_lb
        on liability_bucket.payor_key = cdw_payor_lb.payor_key
left outer join
    {{source('cdw', 'fact_ar_history_hb')}} as fact_ar_history_hb
        on fact_ar_history_hb.liab_bkt_key = liability_bucket.liab_bkt_key
left outer join
    {{source('cdw', 'master_date')}} as master_date
        on master_date.dt_key = fact_ar_history_hb.age_dt_key
left outer join
    {{source('cdw', 'payor')}} as cdw_pyr_arh_pyr
        on cdw_pyr_arh_pyr.payor_key = fact_ar_history_hb.pri_payor_key
where
   master_date.full_dt between add_months(date_trunc('month', current_date), -25)
       and add_months(last_day(current_date), -1)
    and (
            cdw_dict_lb_bkt_typ.dict_nm is null
        or (
            case
            when ((hospital_account.bad_debt_flag_ind = '1')
            and ((hospital_account.extern_ar_flag_ind = '0')
            or (hospital_account.extern_ar_flag_ind = '-2'))) then 'bad debt'
            when ((hospital_account.extern_ar_flag_ind = '1')
            and ((hospital_account.bad_debt_flag_ind = '0')
            or (hospital_account.bad_debt_flag_ind = '-2'))) then 'external ar'
            else 'none'
        end  !=  'bad debt')
)

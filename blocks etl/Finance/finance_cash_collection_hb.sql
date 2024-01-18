{{ config(meta = {
    'critical': true
}) }}

with report_groups as (
    select
        payor.rpt_grp_6 as report_group,
        sum(fact_transaction_hb.pmt_amt) as cash,
        master_date.full_dt as cash_month,
        cdw_dict_ha_acct_base_cls.dict_nm as patient_account_class
    from
        {{source('cdw', 'payor')}} as payor
        right join
            {{source('cdw', 'hospital_account')}} as hospital_account
                on payor.payor_key = hospital_account.pri_payor_key
        left join
            {{source('cdw', 'cdw_dictionary')}} as cdw_dict_ha_acct_base_cls
                on cdw_dict_ha_acct_base_cls.dict_key = hospital_account.dict_acct_basecls_key
        left join
            {{source('cdw', 'fact_transaction_hb')}} as fact_transaction_hb
                on fact_transaction_hb.hsp_acct_key = hospital_account.hsp_acct_key
        left join
            {{source('cdw', 'master_date')}} as master_date
                on master_date.dt_key = fact_transaction_hb.post_dt_key
        left join
            {{source('cdw', 'cdw_dictionary')}} as cdw_d_htr_tx_typ
                on cdw_d_htr_tx_typ.dict_key = fact_transaction_hb.trans_type_key
    where
        master_date.full_dt between
            add_months(date_trunc('month', current_date), -25)
            and add_months(last_day(current_date), -1)
        and lower(cdw_d_htr_tx_typ.dict_nm) = 'payment'
    group by
        payor.rpt_grp_6,
        master_date.full_dt,
        cdw_dict_ha_acct_base_cls.dict_nm
)

select
    {{
        dbt_utils.surrogate_key([
            'report_groups.report_group',
            'report_groups.cash_month',
            'report_groups.patient_account_class'
            ])
    }} as cash_collection_hb_key,
    report_groups.report_group,
    report_groups.cash,
    report_groups.cash_month,
    report_groups.patient_account_class
from
    report_groups

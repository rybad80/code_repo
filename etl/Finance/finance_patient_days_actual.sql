{{ config(meta = {
    'critical': true
}) }}

with patient_days as (
    select
        fact_financial_statistic.fs_acct_key as hsp_acct_key,
        fact_financial_statistic.post_dt_key,
        cost_center.gl_comp as cost_center_ledger_id,
        fact_financial_statistic.stats_cd,
        sum(fact_financial_statistic.stat_measure) as patient_days_actual
    from
        {{source('cdw_analytics', 'fact_financial_statistic')}} as fact_financial_statistic
        inner join {{source('cdw','cost_center')}} as cost_center
            on cost_center.cost_cntr_key = fact_financial_statistic.cost_cntr_key
    where
        fact_financial_statistic.post_dt_key >= 20180701
        and (fact_financial_statistic.stats_cd in ('32')
            or (fact_financial_statistic.stats_cd = '14'
                and fact_financial_statistic.patient_type = 'OP')
        )
    group by
        fact_financial_statistic.fs_acct_key,
        fact_financial_statistic.post_dt_key,
        cost_center.gl_comp,
        fact_financial_statistic.stats_cd
)

select
    patient_days.hsp_acct_key,
    hospital_account.pat_key,
    hospital_account.pri_visit_key as visit_key,
    to_date(patient_days.post_dt_key, 'yyyymmdd') as post_date,
    date_trunc('month', post_date) as post_date_month,
    stg_cost_center.cost_center_ledger_id,
    stg_cost_center.cost_center_name,
    stg_cost_center.cost_center_site_id,
    stg_cost_center.cost_center_site_name,
    case
        when patient_days.stats_cd = 32 then 'IP Patient Days'
        when patient_days.stats_cd = 14 then 'Observation Patient Days'
    end as patient_day_type,
    patient_days.patient_days_actual
from
    patient_days
    inner join {{ref('stg_cost_center')}} as stg_cost_center
            on stg_cost_center.cost_center_ledger_id = patient_days.cost_center_ledger_id
    left join {{source('cdw','hospital_account')}} as hospital_account
        on patient_days.hsp_acct_key = hospital_account.hsp_acct_key

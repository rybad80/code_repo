{{ config(meta = {
    'critical': true
}) }}

select
    {{
        dbt_utils.surrogate_key([
            'location.loc_id',
            'location.rpt_grp_6',
            'department.gl_prefix',
            'department.rpt_grp_3',
            'payor.gl_prefix',
            'financial_class.fc_nm',
            'master_date.full_dt'
            ])
    }} as cash_collection_pb_key,
    location.loc_id as location_id,
    location.rpt_grp_6 as revenue_location_group,
    department.gl_prefix as cost_center_code,
    department.rpt_grp_3 as cost_center_site_id,
    payor.gl_prefix as current_payor_gl_prefix,
    case
        when payor.gl_prefix = '1900' then '1900-SELF-PAY'
        else replace(payor.rpt_grp_6, 'PA_', '')
    end as payor_chop_gl,
    financial_class.fc_nm as financial_class,
    master_date.full_dt as full_date,
    last_day(master_date.full_dt) as pb_month,
    sum(fact_transaction.pmt_pract) * -1 as collections
from
    {{source('cdw', 'fact_transaction')}} as fact_transaction
inner join {{source('cdw', 'master_date')}} as master_date
    on fact_transaction.post_dt_key = master_date.dt_key
inner join {{source('cdw', 'payor')}} as payor
    on fact_transaction.action_payor_key = payor.payor_key
inner join {{source('cdw', 'financial_class')}} as financial_class
    on fact_transaction.action_fc_key = financial_class.fc_key
inner join {{source('cdw', 'department')}} as department
    on fact_transaction.dept_key = department.dept_key
inner join {{source('cdw', 'location')}} as location
    on fact_transaction.loc_key = location.loc_key
where
    master_date.full_dt between add_months(date_trunc('month', current_date), -25)
        and add_months(last_day(current_date), -1)
    and lower(location.rpt_grp_6) in ('chca', 'caa', 'csa', 'rach')
    and fact_transaction.pmt_pract is not null
group by
    location.loc_id,
    location.rpt_grp_6,
    department.gl_prefix,
    payor.gl_prefix,
    department.rpt_grp_3,
    master_date.full_dt,
    case
        when payor.gl_prefix = '1900' then '1900-SELF-PAY'
        else replace(payor.rpt_grp_6, 'PA_', '')
    end,
    financial_class.fc_nm

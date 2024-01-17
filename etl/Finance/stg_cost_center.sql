{{ config(meta = {
    'critical': true
}) }}

with cost_center_raw as (
    select
        cost_center.cost_cntr_key,
        cost_center.gl_comp as cost_center_ledger_id,
        cost_center.cost_cntr_nm as cost_center_name,
        cost_center.rpt_grp_1 as cost_center_site_id,
        workday_cost_center_site.cost_cntr_site_nm as cost_center_site_name,
        row_number() over(partition by cost_center.gl_comp order by cost_center.create_dt desc) as cost_center_line
    from
        {{source('cdw', 'cost_center')}} as cost_center
        inner join {{source('workday','workday_cost_center_site')}} as workday_cost_center_site
            on workday_cost_center_site.cost_cntr_site_id = cost_center.rpt_grp_1
)

select
    cost_center_raw.cost_cntr_key,
    cost_center_raw.cost_center_ledger_id,
    cost_center_raw.cost_center_name,
    cost_center_raw.cost_center_site_name,
    cost_center_raw.cost_center_site_id
from
 cost_center_raw
where
    cost_center_raw.cost_center_line = 1

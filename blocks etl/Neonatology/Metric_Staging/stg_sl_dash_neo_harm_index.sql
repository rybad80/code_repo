/*
Name: stg_sl_dash_neo_harm_index
Description:
    Harm index metric for the Neonatology SL Dashboard.
    Used to obtain overall harm index and drill down for harm types.
Author: Jess Yarnall
*/

with harm_type_id as (
    select
        harm_type,
        row_number() over (order by harm_type) as harm_type_id
    from
        {{ source('cdw', 'fact_ip_harm_monthly_dept_grp') }}
    group by
        harm_type
)


select
    fact_ip_harm_monthly_dept_grp.harm_event_dt_month,
    case
        /* changing Harm Index row to be labeled "overall" to be consistent with formatting in other tables */
        when lower(fact_ip_harm_monthly_dept_grp.harm_type) = 'harm index' then 'overall'
        else fact_ip_harm_monthly_dept_grp.harm_type
    end as harm_type,
    harm_type_id.harm_type_id,
    replace(date(harm_event_dt_month), '-', '') || harm_type_id as harm_index_id,
    sum(fact_ip_harm_monthly_dept_grp.num_of_harm_events) as num_of_harm_events,
    sum(fact_ip_harm_monthly_dept_grp.num_of_population_days) as num_of_population_days
from
    {{ source('cdw', 'fact_ip_harm_monthly_dept_grp') }} as fact_ip_harm_monthly_dept_grp
    inner join harm_type_id
        on harm_type_id.harm_type = fact_ip_harm_monthly_dept_grp.harm_type
where
    fact_ip_harm_monthly_dept_grp.dept_grp_abbr = 'NICU'
    and fact_ip_harm_monthly_dept_grp.num_of_population_days != 0
group by
    fact_ip_harm_monthly_dept_grp.harm_event_dt_month,
    fact_ip_harm_monthly_dept_grp.harm_type,
    harm_type_id.harm_type_id

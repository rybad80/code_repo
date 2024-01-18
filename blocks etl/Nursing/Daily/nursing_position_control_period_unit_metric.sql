{{ config(meta = {
    'critical': true
}) }}

with last_pay_period as (
    select max(pp_end_dt) as last_pp_dt
    from {{ref('nursing_position_control_pay_period')}}
)

select 'PC Functional' as tag,
nursing_position_control_pay_period.pp_dt_key,
nursing_position_control_pay_period.pp_end_dt,
nursing_position_control_pay_period.cost_center_cd as company_cost_center_and_site,
strleft(nursing_position_control_pay_period.cost_center_cd, 3) as company_id,
substring(nursing_position_control_pay_period.cost_center_cd, 4, 5) as cost_center_id,
strright(nursing_position_control_pay_period.cost_center_cd, 3) as cost_center_site_num,
'CS_' || strright(nursing_position_control_pay_period.cost_center_cd, 3) as cost_center_site_id,
nursing_position_control_pay_period.job_code,
nursing_position_control_pay_period.worker_id,
trim(nursing_position_control_pay_period.full_name) as full_name,
sum(nursing_position_control_pay_period.fte::numeric) as aggregated_value
from {{ref('nursing_position_control_pay_period')}} as nursing_position_control_pay_period
left join {{source('workday', 'workday_cost_center')}} as cost_center
        on substring(nursing_position_control_pay_period.cost_center_cd, 4, 5) = cost_center.cost_cntr_cd
inner join last_pay_period
on nursing_position_control_pay_period.pp_end_dt between
nursing_position_control_pay_period.start_on_unit_job
and coalesce(nursing_position_control_pay_period.end_date_on_unit::date, last_pp_dt::date)
group by    tag,
            nursing_position_control_pay_period.pp_dt_key,
            nursing_position_control_pay_period.pp_end_dt,
            nursing_position_control_pay_period.cost_center_cd,
            strleft(nursing_position_control_pay_period.cost_center_cd, 3),
            substring(nursing_position_control_pay_period.cost_center_cd, 4, 5),
            strright(nursing_position_control_pay_period.cost_center_cd, 3),
            'CS_' || strright(nursing_position_control_pay_period.cost_center_cd, 3),
            nursing_position_control_pay_period.job_code,
            nursing_position_control_pay_period.worker_id,
            nursing_position_control_pay_period.full_name

union all

select 'PC Hired FTEs' as tag,
nursing_position_control_pay_period.pp_dt_key,
nursing_position_control_pay_period.pp_end_dt,
nursing_position_control_pay_period.cost_center_cd as company_cost_center_and_site,
strleft(nursing_position_control_pay_period.cost_center_cd, 3) as company_id,
substring(nursing_position_control_pay_period.cost_center_cd, 4, 5) as cost_center_id,
strright(nursing_position_control_pay_period.cost_center_cd, 3) as cost_center_site_num,
'CS_' || strright(nursing_position_control_pay_period.cost_center_cd, 3) as cost_center_site_id,
nursing_position_control_pay_period.job_code,
nursing_position_control_pay_period.worker_id,
trim(nursing_position_control_pay_period.full_name) as full_name,
sum(nursing_position_control_pay_period.hired_fte::numeric) as aggregated_value
from {{ref('nursing_position_control_pay_period')}} as nursing_position_control_pay_period
left join {{source('workday', 'workday_cost_center')}} as cost_center
        on substring(nursing_position_control_pay_period.cost_center_cd, 4, 5) = cost_center.cost_cntr_cd
inner join last_pay_period
on nursing_position_control_pay_period.pp_end_dt between
nursing_position_control_pay_period.start_on_unit_job
and coalesce(nursing_position_control_pay_period.end_date_on_unit::date, last_pp_dt::date)
group by    tag,
            nursing_position_control_pay_period.pp_dt_key,
            nursing_position_control_pay_period.pp_end_dt,
            nursing_position_control_pay_period.cost_center_cd,
            strleft(nursing_position_control_pay_period.cost_center_cd, 3),
            substring(nursing_position_control_pay_period.cost_center_cd, 4, 5),
            strright(nursing_position_control_pay_period.cost_center_cd, 3),
            'CS_' || strright(nursing_position_control_pay_period.cost_center_cd, 3),
            nursing_position_control_pay_period.job_code,
            nursing_position_control_pay_period.worker_id,
            nursing_position_control_pay_period.full_name

union all

select 'PC Hired HeadCount' as tag,
nursing_position_control_pay_period.pp_dt_key,
nursing_position_control_pay_period.pp_end_dt,
nursing_position_control_pay_period.cost_center_cd as company_cost_center_and_site,
strleft(nursing_position_control_pay_period.cost_center_cd, 3) as company_id,
substring(nursing_position_control_pay_period.cost_center_cd, 4, 5) as cost_center_id,
strright(nursing_position_control_pay_period.cost_center_cd, 3) as cost_center_site_num,
'CS_' || strright(nursing_position_control_pay_period.cost_center_cd, 3) as cost_center_site_id,
nursing_position_control_pay_period.job_code,
nursing_position_control_pay_period.worker_id,
trim(nursing_position_control_pay_period.full_name) as full_name,
sum(nursing_position_control_pay_period.headcount_ind::numeric) as aggregated_value
from {{ref('nursing_position_control_pay_period')}} as nursing_position_control_pay_period
left join {{source('workday', 'workday_cost_center')}} as cost_center
        on substring(nursing_position_control_pay_period.cost_center_cd, 4, 5) = cost_center.cost_cntr_cd
inner join last_pay_period
on nursing_position_control_pay_period.pp_end_dt between
nursing_position_control_pay_period.start_on_unit_job
and coalesce(nursing_position_control_pay_period.end_date_on_unit::date, last_pp_dt::date)
group by    tag,
            nursing_position_control_pay_period.pp_dt_key,
            nursing_position_control_pay_period.pp_end_dt,
            nursing_position_control_pay_period.cost_center_cd,
            strleft(nursing_position_control_pay_period.cost_center_cd, 3),
            substring(nursing_position_control_pay_period.cost_center_cd, 4, 5),
            strright(nursing_position_control_pay_period.cost_center_cd, 3),
            'CS_' || strright(nursing_position_control_pay_period.cost_center_cd, 3),
            nursing_position_control_pay_period.job_code,
            nursing_position_control_pay_period.worker_id,
            nursing_position_control_pay_period.full_name

union all

select 'PC Orientation FTEs' as tag,
nursing_position_control_pay_period.pp_dt_key,
nursing_position_control_pay_period.pp_end_dt,
nursing_position_control_pay_period.cost_center_cd as company_cost_center_and_site,
strleft(nursing_position_control_pay_period.cost_center_cd, 3) as company_id,
substring(nursing_position_control_pay_period.cost_center_cd, 4, 5) as cost_center_id,
strright(nursing_position_control_pay_period.cost_center_cd, 3) as cost_center_site_num,
'CS_' || strright(nursing_position_control_pay_period.cost_center_cd, 3) as cost_center_site_id,
nursing_position_control_pay_period.job_code,
nursing_position_control_pay_period.worker_id,
trim(nursing_position_control_pay_period.full_name) as full_name,
case when nursing_position_control_pay_period.term_date <= nursing_position_control_pay_period.pp_end_dt then 0
    else
        case when (nursing_position_control_pay_period.orientation_start_date
              <= nursing_position_control_pay_period.pp_end_dt
               and nursing_position_control_pay_period.orientation_end_date
               >= nursing_position_control_pay_period.pp_end_dt)
                   then nursing_position_control_pay_period.current_fte::numeric else 0 end
                       end as aggregated_value
from {{ref('nursing_position_control_pay_period')}} as nursing_position_control_pay_period
left join {{source('workday', 'workday_cost_center')}} as cost_center
        on substring(nursing_position_control_pay_period.cost_center_cd, 4, 5) = cost_center.cost_cntr_cd
inner join last_pay_period
on nursing_position_control_pay_period.pp_end_dt between
nursing_position_control_pay_period.orientation_start_date
and coalesce(nursing_position_control_pay_period.orientation_end_date::date, last_pp_dt::date)
group by    tag,
            nursing_position_control_pay_period.pp_dt_key,
            nursing_position_control_pay_period.pp_end_dt,
            nursing_position_control_pay_period.cost_center_cd,
            strleft(nursing_position_control_pay_period.cost_center_cd, 3),
            substring(nursing_position_control_pay_period.cost_center_cd, 4, 5),
            strright(nursing_position_control_pay_period.cost_center_cd, 3),
            'CS_' || strright(nursing_position_control_pay_period.cost_center_cd, 3),
            nursing_position_control_pay_period.job_code,
            nursing_position_control_pay_period.worker_id,
            nursing_position_control_pay_period.full_name,
            nursing_position_control_pay_period.term_date,
            nursing_position_control_pay_period.orientation_start_date,
            nursing_position_control_pay_period.orientation_end_date,
            nursing_position_control_pay_period.current_fte
union all

select 'PC UNIT LOAs' as tag,
nursing_position_control_pay_period.pp_dt_key,
nursing_position_control_pay_period.pp_end_dt,
nursing_position_control_pay_period.cost_center_cd as company_cost_center_and_site,
strleft(nursing_position_control_pay_period.cost_center_cd, 3) as company_id,
substring(nursing_position_control_pay_period.cost_center_cd, 4, 5) as cost_center_id,
strright(nursing_position_control_pay_period.cost_center_cd, 3) as cost_center_site_num,
'CS_' || strright(nursing_position_control_pay_period.cost_center_cd, 3) as cost_center_site_id,
nursing_position_control_pay_period.job_code,
nursing_position_control_pay_period.worker_id,
trim(nursing_position_control_pay_period.full_name) as full_name,
case when nursing_position_control_pay_period.term_date <= nursing_position_control_pay_period.pp_end_dt then 0
    else
        case when (nursing_position_control_pay_period.loa_start_date
            <= nursing_position_control_pay_period.pp_end_dt
            and nursing_position_control_pay_period.loa_end_date
                >= nursing_position_control_pay_period.pp_end_dt)
                    then nursing_position_control_pay_period.current_fte::numeric else 0 end
                        end as aggregated_value
from {{ref('nursing_position_control_pay_period')}} as nursing_position_control_pay_period
left join {{source('workday', 'workday_cost_center')}} as cost_center
        on substring(nursing_position_control_pay_period.cost_center_cd, 4, 5) = cost_center.cost_cntr_cd
inner join last_pay_period
on nursing_position_control_pay_period.pp_end_dt between
nursing_position_control_pay_period.loa_start_date
and coalesce(nursing_position_control_pay_period.loa_end_date::date, last_pp_dt::date)
group by    tag,
            nursing_position_control_pay_period.pp_dt_key,
            nursing_position_control_pay_period.pp_end_dt,
            nursing_position_control_pay_period.cost_center_cd,
            strleft(nursing_position_control_pay_period.cost_center_cd, 3),
            substring(nursing_position_control_pay_period.cost_center_cd, 4, 5),
            strright(nursing_position_control_pay_period.cost_center_cd, 3),
            'CS_' || strright(nursing_position_control_pay_period.cost_center_cd, 3),
            nursing_position_control_pay_period.job_code,
            nursing_position_control_pay_period.worker_id,
            nursing_position_control_pay_period.full_name,
            nursing_position_control_pay_period.term_date,
            nursing_position_control_pay_period.loa_start_date,
            nursing_position_control_pay_period.loa_end_date,
            nursing_position_control_pay_period.current_fte

union all

select 'PC Termed FTEs' as tag,
nursing_position_control_pay_period.pp_dt_key,
nursing_position_control_pay_period.pp_end_dt,
nursing_position_control_pay_period.cost_center_cd as company_cost_center_and_site,
strleft(nursing_position_control_pay_period.cost_center_cd, 3) as company_id,
substring(nursing_position_control_pay_period.cost_center_cd, 4, 5) as cost_center_id,
strright(nursing_position_control_pay_period.cost_center_cd, 3) as cost_center_site_num,
'CS_' || strright(nursing_position_control_pay_period.cost_center_cd, 3) as cost_center_site_id,
nursing_position_control_pay_period.job_code,
nursing_position_control_pay_period.worker_id,
trim(nursing_position_control_pay_period.full_name) as full_name,
case when nursing_position_control_pay_period.term_date between nursing_position_control_pay_period.pp_start_dt
    and nursing_position_control_pay_period.pp_end_dt
        then nursing_position_control_pay_period.current_fte::numeric else 0 end as aggregated_value
from {{ref('nursing_position_control_pay_period')}} as nursing_position_control_pay_period
left join {{source('workday', 'workday_cost_center')}} as cost_center
        on substring(nursing_position_control_pay_period.cost_center_cd, 4, 5) = cost_center.cost_cntr_cd
inner join last_pay_period
on nursing_position_control_pay_period.term_date between
nursing_position_control_pay_period.pp_end_dt - 13
and coalesce(nursing_position_control_pay_period.pp_end_dt::date, last_pp_dt::date)
group by    tag,
            nursing_position_control_pay_period.pp_dt_key,
            nursing_position_control_pay_period.pp_end_dt,
            nursing_position_control_pay_period.cost_center_cd,
            strleft(nursing_position_control_pay_period.cost_center_cd, 3),
            substring(nursing_position_control_pay_period.cost_center_cd, 4, 5),
            strright(nursing_position_control_pay_period.cost_center_cd, 3),
            'CS_' || strright(nursing_position_control_pay_period.cost_center_cd, 3),
            nursing_position_control_pay_period.job_code,
            nursing_position_control_pay_period.worker_id,
            nursing_position_control_pay_period.full_name,
            nursing_position_control_pay_period.term_date,
            nursing_position_control_pay_period.pp_start_dt,
            nursing_position_control_pay_period.pp_end_dt,
            nursing_position_control_pay_period.current_fte

with derm_oppe_measures_provider_lvl as (
select
service_provider_key,
service_prov,
cast(service_dt as date) as service_dt,
1 as count_rows,
case
    when ord_narr like 'guardian:%yes%'
    then 1
    else 0 end as confirmed_site_before_surgery,
case
    when ord_narr like 'complications:%'
        and ord_narr not like 'complications:%none%'
    then 1
    else 0 end as complication_rate,
sel_procedure
from
    {{ ref('stg_derm_oppe_sel_measures') }}
where
    service_prov is not null
    and service_dt is not null
)
select
service_provider_key,
service_prov,
service_dt,
sum(confirmed_site_before_surgery) as confirmed_site_before_surgery_num,
sum(complication_rate) as complication_rate_num,
sum(case
    when sel_procedure = 1 then complication_rate
    else 0 end) as sel_proc_complication_rate_num,
sum(count_rows) as count_rows_den,
sum(case
    when sel_procedure = 1 then count_rows
    else 0 end) as sel_proc_count_rows_den,
year(service_dt) as yr_service_dt,
month(service_dt) as mnth_service_dt,
date_part('week', service_dt) as week_number
from
    derm_oppe_measures_provider_lvl
group by
    service_provider_key,
    service_prov,
    service_dt,
    yr_service_dt,
    mnth_service_dt,
    week_number

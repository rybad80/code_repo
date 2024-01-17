{{ config(meta = {
    'critical': false
}) }}

select
    'clinical' as domain, -- noqa: L029
    'Surgical Site Infection (SSI) Rate' as metric_name,
    replace(date(fact_ip_harm_monthly.harm_event_dt_month), '-', '') as primary_key,
    fact_ip_harm_monthly.harm_event_dt_month as metric_date,
    fact_ip_harm_monthly.num_of_harm_events * 100 as num,
    fact_ip_harm_monthly.num_of_population_days as denom
from
    {{source('cdw', 'fact_ip_harm_monthly')}} as fact_ip_harm_monthly
where
    lower(fact_ip_harm_monthly.harm_type) = 'ssi'
    and date_trunc('month', fact_ip_harm_monthly.harm_event_dt_month) < date_trunc('month', current_date)

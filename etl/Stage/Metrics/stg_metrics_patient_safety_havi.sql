{{ config(meta = {
    'critical': false
}) }}

select
    'clinical' as domain, -- noqa: L029
    'Healthcare Associated Viral Infection (HAVI) Rate' as metric_name,
    replace(date(fact_ip_harm_monthly.harm_event_dt_month), '-', '') as primary_key,
    fact_ip_harm_monthly.harm_event_dt_month as metric_date,
    fact_ip_harm_monthly.num_of_harm_events * 1000 as num,
    fact_ip_harm_monthly.num_of_population_days as denom
from
    {{source('cdw', 'fact_ip_harm_monthly')}} as fact_ip_harm_monthly
where
    lower(fact_ip_harm_monthly.harm_type) = 'havi'
    and date_trunc('month', fact_ip_harm_monthly.harm_event_dt_month) < date_trunc('month', current_date)

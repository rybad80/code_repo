{{ config(meta = {
    'critical': false
}) }}

select distinct
    *,
    date(death_date) as metric_date
from
    {{ ref('cancer_center_visit')}}
where
    death_date is not null

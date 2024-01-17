{{ config(meta = {
    'critical': false
}) }}

select
    *
from
    {{ ref('stg_cancer_center_outpatient_visits')}}
where
    outpatient_visits is not null

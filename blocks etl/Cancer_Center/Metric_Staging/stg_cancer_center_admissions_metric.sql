{{ config(meta = {
    'critical': false
}) }}

select
    *
from
    {{ ref('stg_cancer_center_admissions')}}
where
    admissions is not null

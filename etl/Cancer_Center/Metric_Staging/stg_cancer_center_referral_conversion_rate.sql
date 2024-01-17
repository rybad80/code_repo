{{ config(meta = {
    'critical': false
}) }}

select
    *
from
    {{ref('stg_cancer_center_referrals')}}
where
    lower(drill_down) != 'bone marrow transplant'

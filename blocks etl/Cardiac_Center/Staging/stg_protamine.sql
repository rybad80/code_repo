select
    perfusion_meds.log_key,
    min(administration_date) as first_protamine,
    max(administration_date) as last_protamine
from
    {{ref('stg_perfusion_meds')}} as perfusion_meds
    inner join
        {{ref('cardiac_perfusion_surgery')}} as surgery on
            perfusion_meds.log_key = surgery.log_key
where
    medication_id = 7931
group by
    perfusion_meds.log_key

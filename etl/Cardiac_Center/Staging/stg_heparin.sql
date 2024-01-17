select
    perfusion_meds.log_key,
    min(administration_date) as first_heparin,
    max(administration_date) as last_heparin
from
    {{ref('stg_perfusion_meds')}} as perfusion_meds
    inner join
        {{ref('cardiac_perfusion_surgery')}} as surgery on
            perfusion_meds.log_key = surgery.log_key
where
    medication_id = 11976
group by
   perfusion_meds.log_key

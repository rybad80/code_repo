select
    perfusion_meds.log_key,
    sum(cast(perfusion_meds.admin_dose as integer)) as cplegiavol
from
    {{ref('stg_perfusion_meds')}} as perfusion_meds
where
    perfusion_meds.medication_id = 200202683
group by
    perfusion_meds.log_key

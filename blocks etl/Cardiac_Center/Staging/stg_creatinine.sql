select
    surgery.log_key,
    resultdate,
    resultvaluenumeric as creatvalue,
    row_number() over (partition by surgery.log_key order by resultdate) as creat_postop_order
from
    {{ref('stg_perfusion_labs')}} as perfusion_labs
    inner join {{ref('cardiac_perfusion_surgery')}} as surgery on
        surgery.log_key = perfusion_labs.log_key
    inner join {{ref('surgery_encounter_timestamps')}} as timestamps on
        surgery.log_key = timestamps.or_key
where
    result_component_name = 'CREATININE'
    and resultdate between out_room_date and out_room_date + interval '48 hour'

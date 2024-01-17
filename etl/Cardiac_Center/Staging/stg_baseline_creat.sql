select
    surgery.log_key,
    resultdate,
    resultvaluenumeric,
    row_number() over (partition by surgery.log_key order by
    timestamps.in_room_date - resultdate) as baseline_creat_order
from
    {{ref('stg_perfusion_labs')}} as perfusion_labs
    inner join
        {{ref('cardiac_perfusion_surgery')}} as surgery
                on surgery.pat_key = perfusion_labs.pat_key
                and perfusion_labs.resultdate <= surgery.perfusion_date
    inner join
         {{ref('surgery_encounter_timestamps')}} as timestamps
                on surgery.log_key = timestamps.log_key
where
    result_component_name = 'CREATININE'

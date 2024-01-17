select
      surgery.log_key,
      max(resultvaluenumeric) as lactatemax24
from
    {{ref('stg_perfusion_labs')}} as perfusion_labs
    inner join
        {{ref('cardiac_perfusion_surgery')}} as surgery on perfusion_labs.pat_key = surgery.pat_key
    inner join
        {{ref('surgery_encounter_timestamps')}} as timestamps on timestamps.or_key = surgery.log_key
where
    result_component_name like '%LACTATE%W%B%'
    and resultdate between out_room_date and out_room_date + ('23:59:59')
group by
    surgery.log_key

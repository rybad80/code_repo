select
    creatinine.log_key,
    max(creatinine.creatvalue) as resultvaluenumeric
from
    {{ref('stg_creatinine')}} as creatinine
group by
    creatinine.log_key

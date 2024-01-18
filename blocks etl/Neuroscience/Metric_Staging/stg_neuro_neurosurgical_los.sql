select
    *
from
    {{ ref ('stg_neuro_surgical_cases')}}
where
    surgery_day_row = 1
    and ip_ind = 1
    and hospital_discharge_date is not null

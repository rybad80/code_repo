select
    pat_key,
    r_mort_dt
from
    {{source('cdw', 'registry_patient_mortality')}}
where
    cur_rec_ind = 1
group by
    pat_key,
    r_mort_dt
